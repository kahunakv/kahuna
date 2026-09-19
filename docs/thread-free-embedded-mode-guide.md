# Thread-free embedded mode guide

The thread-free embedded mode runs a single-node embedded Kahuna (`EmbeddedKahunaNode`) on a host
that cannot create threads. The main target is the default single-threaded .NET WebAssembly runtime
(`browser-wasm` with `WasmEnableThreads` off), in a browser tab or under Node.js.

The mode is a separate build of `Kahuna.Core`. The compilation symbol `KAHUNA_THREAD_FREE` turns it
on. A normal build does not define the symbol, so normal behavior does not change.

The mode depends on the thread-free build of Kommander (`KOMMANDER_THREAD_FREE`, Kommander 1.7.0 or
later). Read the Kommander thread-free embedded mode developer guide for the Raft side: the host
pump, the scheduler changes, and the Kommander configuration rules.

## Why a separate build

The single-threaded WebAssembly runtime has two hard limits:

1. `new Thread(...).Start()` throws `PlatformNotSupportedException`.
2. A blocking wait (`.Wait()`, `.Result`, `GetAwaiter().GetResult()`, `SemaphoreSlim.Wait()`) has
   no other thread to release it. It never returns, or it spins until its timeout.

The normal `Kahuna.Core` package also references ASP.NET Core for the gRPC and REST server surface.
That framework has no `browser-wasm` runtime pack, so a browser app that references the normal
package fails to build with `NETSDK1082`.

## How to get the thread-free build

`Kahuna.Core` and `Kahuna.Shared` contain two target frameworks, `net10.0` and `net10.0-browser`.
Only `net10.0-browser` defines `KAHUNA_THREAD_FREE`. A project gets it when its own target framework
is `net10.0-browser`:

```xml
<Project Sdk="Microsoft.NET.Sdk.WebAssembly">
  <PropertyGroup>
    <TargetFramework>net10.0-browser</TargetFramework>
    <RuntimeIdentifier>browser-wasm</RuntimeIdentifier>
    <WasmEnableThreads>false</WasmEnableThreads>
    <JsonSerializerIsReflectionEnabledByDefault>true</JsonSerializerIsReflectionEnabledByDefault>
  </PropertyGroup>
</Project>
```

Two settings in this example are necessary:

- **`net10.0-browser`, not `net10.0`.** A WebAssembly app that targets plain `net10.0` gets the
  normal assemblies of Kahuna and Kommander. The build then fails with `NETSDK1082`.
- **`JsonSerializerIsReflectionEnabledByDefault`.** A WebAssembly app turns off reflection-based
  `System.Text.Json` by default. Kommander serializes the partition map and other system state with
  reflection. Without this property, `StartAsync` never finishes.

## What the build changes

| Area | Normal build | Thread-free build |
| --- | --- | --- |
| Raft scheduling | Kommander worker threads | Kommander host pump (`EnableHostPumpedScheduling`, default true) |
| Backend read and write schedulers | Worker threads | Each operation runs inline on the caller |
| Persistence backend (`Storage`) | `memory`, `sqlite`, `rocksdb` | `memory` only |
| Raft WAL (`WalStorage`) | `memory`, `sqlite`, `rocksdb` | `memory` only |
| RocksDB shared memory bundle and WAL shard tuning | Included | Not included |
| `EmbeddedKahunaNode` constructors | Single-node and cluster | Single-node only |
| gRPC and REST server surface (`Communication/External/Grpc`, `ClusterLeave`, `NodeTransportGate`) | Included | Not included |
| gRPC inter-node client (`GrpcInterNodeCommunication`) | Included | Not included. It needs `SocketsHttpHandler`, which the browser does not have. |
| Script parse cache | Filled when the caller sends a script hash | Never filled. Blake3 has no `browser-wasm` native library. Every script is parsed. |
| HTTPS certificate in `KahunaConfiguration` | Loaded | Refused. The browser has no X509 support. |
| Transaction coordinator dispose | Waits up to 5 s for deferred resolutions | Cancels them without a wait |
| ASP.NET Core reference | Yes | No. `Microsoft.Extensions.ObjectPool` is a normal package reference. |

## Configuration rules

`EmbeddedKahunaNode` refuses these options at construction in the thread-free build. Each refusal is
an `ArgumentException` that names the option:

- `Storage` other than `memory`.
- `WalStorage` other than `memory`.
- `EnableSharedExecutorPool = false`. A partition executor on its own thread cannot run there.

Set `EnableHostPumpedScheduling = false` only when something else drives the node. With the pump
off and no driver, nothing runs, and `StartAsync` never finishes.

Do not use `Microsoft.Extensions.Logging.Console` as the logger. Its processor writes from a
dedicated thread. Use a logger that writes synchronously.

## Example

```csharp
await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
{
    Storage = "memory",
    WalStorage = "memory",
    InitialPartitions = 1,
}, loggerFactory);

await node.StartAsync(cancellationToken);
```

Use only awaits on this path. A blocking call in the host code has the same effect as a blocking
call in the library.

## Testing the thread-free build

`scripts/run-wasm-smoke.sh` builds `Kahuna.WasmSmoke` for `browser-wasm` with `WasmEnableThreads`
off and runs it under Node.js. The app does these steps:

1. It checks that the constructor refuses `rocksdb` storage.
2. It starts a node and waits for the election.
3. It runs an interactive transaction (a write and a commit) and reads the value back.
4. It runs a transaction script and reads the value back.
5. It disposes the node.

A thread start, a blocking wait, a stall, or a wrong result makes the check fail. The check needs
the .NET 10 SDK and Node.js, but not the `wasm-tools` workload. The CI workflow runs it.

```sh
scripts/run-wasm-smoke.sh
```

A test on normal .NET cannot replace this check. Library code resumes with
`ConfigureAwait(false)`, so on normal .NET a blocked continuation moves to another thread and the
test still passes. Also, the `net10.0` assembly of Kommander does not contain the host pump.

`Kahuna.WasmSmoke` is not in `Kahuna.sln`, so the normal solution build does not change.

## Rules for changes to Kahuna.Core

- Put thread-free code under `#if KAHUNA_THREAD_FREE`. Keep each region small, and write a comment
  that tells why the region is gated.
- Do not change the normal path to share code with the thread-free path. When a thread-free form
  would change the normal path, put it under `#if KAHUNA_THREAD_FREE` and keep the current code in
  `#else`.
- A file that the browser cannot use (for example, a native backend or an ASP.NET Core type) goes in
  the `Compile Remove` list of the `net10.0-browser` item group in `Kahuna.Core.csproj`.
- A new `Thread` start, a new blocking wait on the embedded path, or a new scheduler that starts
  worker threads breaks the smoke check. The browser target builds with no warnings, so a new
  `CA1416` warning there is a new call to an API that the browser does not support.

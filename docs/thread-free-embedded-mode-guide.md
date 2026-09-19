# Thread-free embedded mode guide

The thread-free embedded mode runs embedded Kahuna on a host that cannot create threads: a single
node (`EmbeddedKahunaNode`), or a cluster of several nodes in one process (`EmbeddedKahunaCluster`).
The main target is the default single-threaded .NET WebAssembly runtime (`browser-wasm` with
`WasmEnableThreads` off), in a browser tab or under Node.js.

The mode is a separate build of `Kahuna.Core`. The compilation symbol `KAHUNA_THREAD_FREE` turns it
on. A normal build does not define the symbol, so normal behavior does not change.

The mode depends on the thread-free build of Kommander (`KOMMANDER_THREAD_FREE`, Kommander 1.7.0 or
later; 1.7.2 or later to run with reflection-based JSON off). Read the Kommander thread-free embedded mode developer guide for the Raft side: the host
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
  </PropertyGroup>
</Project>
```

The target framework is necessary: **`net10.0-browser`, not `net10.0`.** A WebAssembly app that
targets plain `net10.0` gets the normal assemblies of Kahuna and Kommander. The build then fails with
`NETSDK1082`.

A WebAssembly app turns off reflection-based `System.Text.Json` by default. Kahuna and Kommander 1.7.2
or later serialize only with source-generated metadata, so the app can keep that default. With
Kommander 1.7.0 or 1.7.1, set `JsonSerializerIsReflectionEnabledByDefault` to `true`: those versions
serialize the partition map with reflection, and without the property `StartAsync` never finishes.

## What the build changes

| Area | Normal build | Thread-free build |
| --- | --- | --- |
| Raft scheduling | Kommander worker threads | Kommander host pump (`EnableHostPumpedScheduling`, default true) |
| Backend read and write schedulers | Worker threads | Each operation runs inline on the caller |
| Persistence backend (`Storage`) | `memory`, `sqlite`, `rocksdb` | `memory` only |
| Raft WAL (`WalStorage`) | `memory`, `sqlite`, `rocksdb` | `memory` only |
| RocksDB shared memory bundle and WAL shard tuning | Included | Not included |
| `EmbeddedKahunaNode` constructors | Single-node and cluster | Single-node, and cluster with the in-memory transports only |
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
- In the cluster constructor, an inter-node transport other than `MemoryInterNodeCommmunication`, or a
  Raft transport other than Kommander's `InMemoryCommunication`. The browser has no sockets.

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

## In-memory cluster

`EmbeddedKahunaCluster` runs several full nodes in one process. Each node has its own storage, Raft
log and actors. The nodes talk through one shared `MemoryInterNodeCommmunication` and one shared
`InMemoryCommunication`, so nothing crosses a network. The cluster replicates, elects leaders and
fails over as a networked cluster does. It is available in both builds.

```csharp
await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(
    3,
    new EmbeddedKahunaOptions { NodeName = "demo", Storage = "memory", WalStorage = "memory", InitialPartitions = 2 },
    loggerFactory,
    cancellationToken);

EmbeddedKahunaNode node = cluster.GetNode(1);          // any running node takes requests

int leader = await cluster.GetLeaderIndexAsync(0, cancellationToken);
await cluster.StopNodeAsync(leader);                    // another node takes over
await cluster.RestartNodeAsync(leader, cancellationToken); // restarts empty and catches up
```

`CreateInMemoryAsync` gives each node a copy of the base options with a distinct node id (1 to N), node
name (`{NodeName}-{id}`) and port (`Port + index`, or 7000 + index when `Port` is 0). It starts all nodes
together, and it returns when every partition has a leader. Storage and WAL must be `memory`.

**Stop and restart.** `StopNodeAsync` stops a node as if its host crashed. Its Raft traffic is cut in
both directions, the Kahuna transport stops routing to it, and the node is disposed. The other nodes
elect new leaders for the partitions that it led. `RestartNodeAsync` builds the node again under the
same endpoint, with empty storage and log. It rejoins through the static roster and catches up from
the other nodes.

**Retries during a failover.** A Kahuna call that another node forwards to a stopped node fails with
`KahunaServerException` until a new leader is elected. A write that loses its leader can also return
`MustRetry`. Retry both. The retry routes to the new leader.

**Leader lookup.** `GetLeaderIndexAsync` returns a running node that believes that it leads the
partition. Right after a failover, an old leader can still believe that it leads until it hears the new
term. Use the result as a routing hint.

**Network partitions.** `BlockLink(from, to)` drops the traffic between two members on both transports,
and `IsolateNode(index)` cuts one member off from every other. `UnblockLink`, `UnblockLinkBothWays` and
`UnblockAllLinks` restore the traffic, and `IsLinkBlocked` reports it. It needs Kommander 1.7.3 or
later.

An isolated member keeps running, unlike a stopped one: it keeps its timers and keeps campaigning. It
cannot commit, because it reaches no quorum. The other members elect a leader of their own and serve
writes. When the links are restored, the isolated member learns the new term, steps down and catches
up.

A Raft message that needs no reply is dropped only in the blocked direction. Every call that waits for
a reply fails in both directions, because a call needs its reply.

`GetLeaderIndexAsync` can still name an isolated member: a member cut from the majority keeps believing
that it leads until it hears the new term. Look for the leader among the members of the majority side
instead.

**Memory.** Each node keeps its own copy of the data and of the Raft log, so 3 nodes use about 3 times
the memory of one node.

### Timings on one event loop

In the browser, every node, partition and timer runs on the same thread. A heartbeat that runs late
looks like a dead leader, and it starts a needless election. The election timeout must stay well above
the longest gap between two heartbeats.

These values were measured with 3 nodes and 2 data partitions under Node.js, for 30 s idle and then
30 s at 4 transactions per second. The largest gap is the longest time between two heartbeats of one
leader.

| Timings | Election timeout | Largest heartbeat gap | Elections in 60 s |
| --- | --- | --- | --- |
| Embedded defaults: `HeartbeatInterval` 100 ms, `CheckLeaderInterval` 250 ms | 500 to 1500 ms | 255 to 275 ms | 0 |
| Fast single-node smoke values: `HeartbeatInterval` 50 ms, `CheckLeaderInterval` 25 ms | 100 to 250 ms | 81 to 103 ms | 0 |

Use the embedded defaults for a cluster. The largest gap follows the configured heartbeat and
leader-check intervals, not load on the event loop. With the defaults, the shortest election timeout
is about twice the largest gap. The fast values also passed, but their shortest election timeout is
about equal to the largest gap, so one slow pass of the event loop can start an election.

To measure elections in a WebAssembly app, set `MetricsSupport` to `true`. A WebAssembly app turns off
`System.Diagnostics.Metrics` by default, and a `MeterListener` then gets no measurements. Kommander
counts each election start in `raft.elections_started_total` and each heartbeat gap in
`raft.heartbeat_delay_ms`.

## Testing the thread-free build

`scripts/run-wasm-smoke.sh` builds `Kahuna.WasmSmoke` for `browser-wasm` with `WasmEnableThreads`
off and runs it under Node.js. The app does these steps:

1. It checks that the constructor refuses `rocksdb` storage.
2. It starts a node and waits for the election.
3. It runs an interactive transaction (a write and a commit) and reads the value back.
4. It runs a transaction script and reads the value back.
5. It disposes the node.
6. It checks that the cluster constructor refuses a Raft transport that is not in memory.
7. It starts a 3-node in-memory cluster, commits a transaction through each node, and reads each value
   through every node.
8. It keeps the cluster idle for 30 s and then under a light write load for 30 s. Any election in
   this time makes the check fail.
9. It stops the leader of the meta partition, waits for another node to take over, and commits a
   write.
10. It restarts the stopped node and reads that write through it.
11. It isolates the leader of one data partition, checks that this node cannot commit, and checks that
    the majority elects a leader and commits. It then restores the links and reads that write through
    the healed node.

A thread start, a blocking wait, a stall, or a wrong result makes the check fail. The check needs
the .NET 10 SDK and Node.js, but not the `wasm-tools` workload. The CI workflow runs it.

```sh
scripts/run-wasm-smoke.sh
```

A test on normal .NET cannot replace this check. Library code resumes with
`ConfigureAwait(false)`, so on normal .NET a blocked continuation moves to another thread and the
test still passes. Also, the `net10.0` assembly of Kommander does not contain the host pump. The
cluster tests in `Kahuna.Server.Tests` (`TestEmbeddedKahunaCluster`) run the same steps on normal
.NET on a one-thread `SynchronizationContext`, but they use the normal build.

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

# User-defined functions guide

Kahuna script ships with about forty built-in functions. A deployment can add its own, written in C#,
and call them from any script exactly like a built-in:

```sql
LET total = acme_price_with_tax(@amount, 'ES')
SET users/{@id}/checksum acme_crc32(@payload)

IF acme_is_business_day(current_time()) THEN
    SET orders/{@id}/status 'open'
END
```

The point is to run domain logic — encoding, hashing, formatting, a business predicate — inside the
transaction, next to the data, instead of paying a client round trip for every value.

This guide covers what a function may do, how to install one, and what happens when one fails.

---

## 1. Writing a function

A function is a delegate. It takes a context and the evaluated arguments, and it returns one value.

```csharp
using Kahuna.Extensibility;

static KahunaValue Crc32(in KahunaFunctionContext context, ReadOnlySpan<KahunaValue> args)
{
    ReadOnlyMemory<byte> payload = args[0].AsBytes();

    return KahunaValue.From((long)Crc32Algorithm.Compute(payload.Span));
}
```

### The value model

`KahunaValue` is the value a function sees and returns. It has seven kinds, the same seven the script
language has:

| `KahunaValueKind` | Built with | Read with |
|---|---|---|
| `Null` | `KahunaValue.Null` | `IsNull` |
| `Bool` | `From(bool)` | `AsBool()` / `TryGetBool(out …)` |
| `Long` | `From(long)` | `AsLong()` / `TryGetLong(out …)` |
| `Double` | `From(double)` | `AsDouble()` / `TryGetDouble(out …)` |
| `String` | `From(string?)` | `AsString()` / `TryGetString(out …)` |
| `Bytes` | `From(ReadOnlyMemory<byte>)` or `From(byte[]?)` | `AsBytes()` / `TryGetBytes(out …)` |
| `Array` | `FromArray(IReadOnlyList<KahunaValue>)` | `AsArray()` / `TryGetArray(out …)` |

An `As*` accessor throws `KahunaFunctionException` when the kind does not match. A `TryGet*` accessor
returns `false` instead. Check `Kind` first, or use `TryGet*`, wherever the script could pass more
than one kind.

Two details are worth knowing:

- A value read from a key arrives as the kind the script sees. Text stored in a key arrives as
  `String`, not as `Bytes`. `Bytes` is the kind of a buffer a function itself produced.
- A revision and an expiry are not carried. A value a function returns is a plain value, exactly like
  the result of `concat` or `upper`. To read a key's revision or expiry, apply the built-in `rev()`
  or `expires()` in the script and pass the result in as an argument.

### The context

`KahunaFunctionContext` carries identity and diagnostics only:

| Member | What it is |
|---|---|
| `FunctionName` | The name the script called. |
| `Line` | The 1-based script line of the call site. |
| `TransactionId` | The HLC that identifies the transaction. `Zero` for a single command outside one. |
| `ReadTimestamp` | The snapshot the transaction reads at, or `Zero` for "latest". |
| `NodeName` | The node evaluating the script. |
| `Logger` | A logger. Use it sparingly: this is the request path. |
| `Fail(message)` | Fails the call with a message. |

There is deliberately no way to read or write a key from the context. See rule 2 below.

---

## 2. The rules a function must obey

Kahuna cannot detect a violation of any of these. Each one is a bug in the calling application.

1. **Be synchronous and fast.** No network, no disk, no lock acquisition, no `Task.Wait()`, no
   `.Result`, no `Thread.Sleep`. The call blocks a request path while the transaction holds its locks
   and write intents, so a slow function shows up as latency on keys it never touches. Aim for
   microseconds. A call slower than `--function-slow-warn-ms` is logged.

2. **Do not call back into Kahuna.** Do not use `IKahuna`, a `KahunaClient`, or any other Kahuna API
   from inside a function. The call happens mid-transaction with locks held, and re-entering can
   deadlock against the actor mailbox and against the calling transaction's own write intents. Read
   the data in the script and pass the value in as an argument instead.

3. **Be thread-safe.** One registration serves the whole node, and many transactions can call it at
   once. Prefer a function that holds no state. Captured state must be immutable — synchronising it
   under contention breaks rule 1.

4. **Be idempotent.** A script transaction can be re-executed, and the executor can re-run statements
   inside its own retry loop. Kahuna promises nothing about how many times a function is invoked for
   one logical transaction, so a function must never send mail, publish to a queue, or move any other
   externally visible counter.

5. **Prefer determinism.** A non-deterministic function is safe to replicate — Raft carries the
   result, not the call — but a retried transaction can then store a different value than its first
   attempt produced. Read `context.ReadTimestamp` rather than the wall clock; it is the same on every
   attempt of one transaction and it orders consistently across nodes.

6. **Bound the output.** A returned string or byte buffer can become a key's value and travels
   through Raft. Keep it inside the size limits any write obeys.

A function may reuse its own return buffer between calls. The engine copies a returned byte buffer on
the way out, so the stored value does not move when the buffer is overwritten.

---

## 3. Installing a function

### An embedded node

Register on the options before the node is constructed:

```csharp
EmbeddedKahunaOptions options = new() { /* … */ };

options.Functions
    .Register("acme_double", static (in ctx, args) => KahunaValue.From(args[0].AsLong() * 2), 1, 1)
    .Register("acme_crc32", Crc32, 1, 1);

await using EmbeddedKahunaNode node = new(options, loggerFactory);
await node.StartAsync(cancellationToken);
```

### A host that embeds `KahunaManager`

Register on `KahunaConfiguration.Functions` before the manager is constructed. It is the single field
the engine reads.

### The shipped server binary

An operator running `kahuna-server` cannot edit the source, so the functions come from an assembly:

```csharp
using Kahuna.Extensibility;

public sealed class AcmeFunctions : IKahunaFunctionProvider
{
    public void Register(KahunaFunctionRegistry registry)
    {
        registry.Register("acme_crc32", Crc32, 1, 1);
    }
}
```

Build it against `Kahuna.Core` and point the server at it:

```sh
kahuna-server --extension-assembly /opt/acme/Acme.KahunaFunctions.dll
```

The flag is repeatable. Without it nothing is loaded: there is no plugin directory, no probing and no
discovery, so a node that was not asked to load an extension runs none of that code.

Each loaded file is logged with its path and the SHA-256 of its bytes, so an operator can confirm
every node loaded the same artifact.

### Registration rules

`Register(name, function, minArgs, maxArgs)` validates when the node starts, never at script time:

- The name must match `[a-zA-Z_][a-zA-Z0-9_]*`. No other shape can be reached from a script.
- The name must not be a built-in or one of its aliases. Built-ins are reserved so a script's
  built-ins mean one fixed thing everywhere.
- The name must not already be registered. A second registration is rejected, not silently ignored.
- `minArgs` must not be negative. `maxArgs` must be `-1` for a variadic function, or at least
  `minArgs`.

Names are matched ordinally and case-sensitively.

**Prefix your names with an application tag**, such as `acme_crc32`. It is a convention, not a rule,
and it is what keeps a built-in added in a later Kahuna release from colliding with the scripts a
deployment already runs.

Registration must finish before the node is built. The engine freezes the registry at construction,
and a later `Register` throws `InvalidOperationException`.

---

## 4. Every node must register the same set

**The registry is per process. Kahuna does not replicate it.** Every node of a cluster must register
an identical set, the same way every node must run the same binary.

What a node without a function can still do is the important half:

| Path | Does the function run? |
|---|---|
| Script execution on the coordinating node | **Yes** — the only place. |
| Raft proposal and replication | No. It carries the value the function produced. |
| Apply on a follower | No. |
| WAL replay, cold restart, snapshot install | No. |
| Background writer to the persistence backend | No. |
| Range split and merge, state transfer, backup restore | No. |
| Lock planning | No. A function cannot appear in key position. |

So a node that lacks a function is still a correct follower. It applies the log, it restores from a
cold start, and it serves reads of values another node's function produced. It only refuses to
*coordinate* a script that calls what it does not have. This is also why a function can be retired: a
value it wrote stays readable after the function is gone.

To make a mismatch visible rather than mysterious, every node reports a **fingerprint** — a short
hash over its registered names and their argument counts:

- It is logged when the node starts, along with the function count.
- It is on the node's metrics as a tag of `kahuna.script_functions.registered`.
- It is in the error a script gets when it calls a function the node does not have:

  ```
  Undefined function 'acme_crc32' on node kahuna-2 (functions 3f2a91c0d4e17b55)
  ```

Two nodes that report different fingerprints loaded different extension builds. Kahuna does not
gossip fingerprints or refuse to form a cluster on a mismatch, because that would block a rolling
deployment that adds a function.

---

## 5. What happens when a function fails

Every failure produces `Errored`, and the transaction rolls back. Nothing is written and no lock is
left held.

| Cause | Message |
|---|---|
| Wrong argument count | `Invalid number of arguments for 'acme_x' function` |
| `context.Fail(reason)` or a thrown `KahunaFunctionException` | `Function 'acme_x' failed: reason` |
| Any other exception | `Function 'acme_x' threw InvalidOperationException: …` (also logged) |
| The node does not have the function | `Undefined function 'acme_x' on node … (functions …)` |

`Errored` is the correct classification and it is deliberate. It is never `MustRetry`, which would
spin a client on a failure that repeats on every attempt, and never `Aborted`, which tells a client
its transaction lost a genuine conflict.

`OperationCanceledException` is not special-cased. A function has no cancellation contract, so a raw
cancellation out of one is a failure like any other and is not read as the transaction timing out.

A slow function is not aborted. .NET cannot preempt a running call, so the transaction timeout is the
backstop that reclaims the session.

---

## 6. Observability

| Instrument | What it reports |
|---|---|
| `kahuna.script_functions.registered` | How many functions the node has, tagged with the node and the fingerprint. |
| `kahuna.script_functions.calls` | Invocations of each function since the node started. |
| `kahuna.script_functions.elapsed_ms` | Total time spent inside each function. |

`--function-slow-warn-ms` (default `50`) logs a warning for a call slower than that threshold. Setting
it to `0` turns the warning off **and** stops the node reading the clock on the call path, which
removes about 13 ns per call. The call count stays exact either way; `elapsed_ms` then stops growing.

---

## 7. Security

**Registering a function grants arbitrary in-process code execution with the node's full
privileges.** That is inherent. For the embedded and library surfaces it is also unremarkable: the
consumer already owns the process.

For the shipped binary:

- Extension loading is off unless `--extension-assembly` is passed.
- Every loaded assembly is logged with its path and its SHA-256.
- **Nothing is sandboxed.** A hostile or buggy extension can corrupt state, block the node, or crash
  the process. A `StackOverflowException` or an `OutOfMemoryException` from user code cannot be
  caught and will take the process down. Treat an extension assembly with exactly the same trust as
  the server binary.
- A script or a client cannot register, replace, enumerate, or remove a function over the wire. The
  wire surface is unchanged: a client can only *call* what the operator installed.

Startup fails, rather than continuing, on any loading problem — a missing file, an assembly with no
provider, a provider that throws, or a rejected name. A node that came up missing one function would
answer scripts that call it with `Errored` while its peers answered normally, which is a far harder
failure to trace than a node that refused to start.

---

## 8. Cost

Measured on an Apple M3, against the same dispatcher a built-in goes through:

| Call | Time | Allocated |
|---|---|---|
| Built-in, two arguments (`min`) | 37 ns | 328 B |
| User-defined, two arguments, slow-call timer off | 53 ns | 328 B |
| User-defined, two arguments, slow-call timer on | 65 ns | 328 B |

**Allocation is identical.** A user-defined call allocates nothing beyond the argument list the
expression walk already builds, for any call of eight arguments or fewer. A longer call rents its
argument buffer from a pool and returns it cleared.

The 16 ns of extra time is the value-model boundary: a built-in receives the evaluator's own value
type, while a user-defined function receives and returns `KahunaValue`. That separation is what lets
the script engine change without breaking a compiled extension, so the cost is deliberate. The
further 12 ns with the timer on is two clock reads.

All of this is small next to the microseconds a function body is expected to take. It matters only
for a function that does almost nothing.

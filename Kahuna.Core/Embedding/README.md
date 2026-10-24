# Embedding

Embedding provides the in-process API for hosting Kahuna without running the external server executable.

`EmbeddedKahunaNode` wires together:

- an actor system,
- a local Raft instance,
- `KahunaManager`,
- in-memory inter-node communication for embedded clusters.

Use this component for tests, local tools, and applications that need a Kahuna node inside their own process. External HTTP/gRPC hosting is intentionally not handled here.

The standalone constructor `EmbeddedKahunaNode(options)` uses in-process phantom witnesses. They
acknowledge Raft traffic but persist no replicas; check-quorum and backfill are disabled for this
constructor. The cluster constructor accepts caller-supplied transports and discovery and retains
normal cluster quorum checks. `EmbeddedKahunaCluster.CreateInMemoryAsync` builds real in-process members
with in-memory transports and either memory or persistent storage.

## Startup and cancellation

`StartAsync` waits for a leader for every partition the node hosts. An election round that ends without
a leader is logged and retried; the default `EmbeddedKahunaOptions.StartupLeaderWait` is
`Timeout.InfiniteTimeSpan`. A restart during a temporary loss of quorum can therefore remain pending
until peers return. Pass a cancellation token to bound startup for your application:

```csharp
using CancellationTokenSource startup =
    CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);
startup.CancelAfter(TimeSpan.FromMinutes(2));
await node.StartAsync(startup.Token);
```

Disposing the node also cancels a pending start. Restore failures remain fatal; retries apply to
undecided elections and a node that is not ready yet, rather than to storage errors.

Set `StartupLeaderWait` to a positive `TimeSpan` when a supervisor should take over after a bounded
leader wait. The budget applies separately to each partition and is checked after an unsuccessful
election round, so it is not a strict wall-clock deadline for the whole start. Expiry throws a
`RaftException` naming the partition. Use the cancellation token when an overall startup deadline is
required. `JoinTimeout` separately bounds joining an existing cluster.

## Persistent in-process clusters

Despite its name, `CreateInMemoryAsync` also accepts persistent backend and WAL providers. Supply
`StoragePath` for an on-disk backend and `WalPath` for an on-disk WAL:

```csharp
await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(
    3,
    new EmbeddedKahunaOptions
    {
        NodeName = "local",
        Storage = "sqlite",
        StoragePath = "/var/lib/kahuna-test/data",
        WalStorage = "sqlite",
        WalPath = "/var/lib/kahuna-test/wal"
    },
    cancellationToken: cancellationToken);
```

Each member uses its own subdirectory (`local-1`, `local-2`, and `local-3` in this example) under each
base path. For persistent engines, a blank `StorageRevision` or `WalRevision` becomes `member`, so
`RestartNodeAsync` reopens the same files. Explicit revisions are preserved. Memory-backed state
starts empty on restart and catches up from peers; persistent state replays its own files first.
Backend and WAL persistence are chosen independently; memory state does not itself survive process loss.

`RestartNodeAsync` waits for leaders under the same startup policy. Other members can be stopped or
restarted while that wait is pending, allowing quorum to be restored. The helper uses a static member
roster and does not accept `JoinExistingSeeds`.

Engine selection and custom providers are described in the
[storage engines guide](../../docs/storage-engines-guide.md). Embedded reads can use the
[batched probe APIs](../../docs/batched-probes-guide.md), and change consumers can use
[key-value watch streams](../../docs/keyvalue-watch-streams-guide.md).

## Received snapshots

Embedded nodes with a SQLite/RocksDB backend and `StoragePath` default received-snapshot staging
to a private directory under that path.
Memory-only nodes stage in memory unless a directory is supplied. See the
[snapshot and Raft recovery guide](../../docs/snapshot-and-raft-recovery-guide.md) for the receive caps,
deadlines and per-member directory rules. Settlement options are described in the
[durable settlement guide](../../docs/durable-settlement-guide.md); they are independent of storage choice.

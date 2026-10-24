# Batched point and range probes

Embedded callers can read many point keys or bounded ranges through `IKahuna` with fewer actor and
inter-node round trips. Use `LocateAndTryProbeMany` for point keys and `LocateAndTryProbeManyRanges`
for bounded seeks. The locator routes each group to its owning partition leader, including when a
key space has been split. These APIs are exposed by Kahuna.Core; they are not methods on
`KahunaClient` or `KahunaTransactionSession`.

The examples use `IKahuna kahuna` from a started embedded node and a `CancellationToken ct`.
Probe types are in `Kahuna.Server.KeyValues`; timestamps are in `Kommander.Time`, and durability and
response enums are in `Kahuna.Shared.KeyValue`.

## Point reads

```csharp
string[] keys = ["orders/a", "orders/b", "orders/absent"];
KeyValueProbeResult[] results = await kahuna.LocateAndTryProbeMany(
    HLCTimestamp.Zero, HLCTimestamp.Zero, keys,
    KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct);

for (int i = 0; i < results.Length; i++)
{
    KeyValueProbeResult result = results[i];
    if (result.Type == KeyValueResponseType.Get && result.Entry is { } entry)
        Console.WriteLine($"{keys[i]} revision {entry.Revision}");
    else
        Console.WriteLine($"{keys[i]}: {result.Type}");
}
```

All keys must belong to one key space: `orders/a` and `orders/b` do, while `orders/a` and `users/b`
do not. Each result occupies the same position as its input key. `Type` and `Entry` have the same
meaning as a single get; a missing key is a per-item `DoesNotExist`, not a failed batch. The entry
carries the value, revision, expiry, and modification timestamp.

The first timestamp is the transaction id; zero means a read outside a transaction. The second is
the read timestamp; zero selects latest reads. Pass a fixed HLC timestamp when the reads must use a
historical snapshot, with the same snapshot-retention requirements as single reads. Batching alone
does not turn independent latest point reads across partitions into one atomic snapshot.

## Bounded range reads

```csharp
KeyValueProbeRange[] seeks =
[
    new("orders/a", true, "orders/b", false, 32),
    new("orders/c", true, "orders/d", false, 32)
];

KeyValueProbeRangeResult[] results = await kahuna.LocateAndTryProbeManyRanges(
    HLCTimestamp.Zero, readTimestamp, "orders", seeks,
    KeyValueProbeLock.None, 0, KeyValueDurability.Persistent, ct);
```

Here `readTimestamp` is the HLC snapshot the caller wants to read. Each seek specifies start and end
keys, inclusivity, and a positive page limit under the request's one key-space prefix. A null bound
leaves that side open. Results preserve seek order; each contains `Type`, key-ordered `Items`,
`HasMore`, and `ResumeKey`, as well as the lock fields described below.

After consuming a successful page, continue a seek with `HasMore` through `LocateAndScanRange`,
using the same transaction, prefix, end bound, durability, and snapshot timestamp. Start after
`ResumeKey` (exclusive). If `ResumeKey` is null, restart at the seek's original start bound and
inclusivity instead. Give a continuation scan its own operation id when using a registered session.
Use an explicit fixed read timestamp when the continuation must see the same snapshot as the probe;
zero asks for a latest snapshot and does not provide a reusable timestamp to the caller.

A seek that crosses partition boundaries is stitched through the range scan path. Batching reduces
dispatch work; it does not imply that every seek or partition is read in one actor turn.

## Shared locks and transaction registration

`KeyValueProbeLock.None` reads without acquiring a probe lock. `KeyValueProbeLock.Shared` requires a
nonzero transaction id and takes a Shared lock before each read: `[key, key]` for a point probe, or
the seek's bounds for a range probe. Pass `expiresMs` for the lock lease, as for an ordinary range
lock. Within one partition, lock acquisition and the initial read step run in the same actor turn.

For an interactive session, pass the handle's `TransactionId` and `CoordinatorKey`, plus a nonempty
`TransactionOperationId.NewRandom()` for each new logical probe. Keep that operation id and the
request unchanged when retrying the same operation. Registration folds the confirmed read
observations and locks into the coordinator's working set; commit or rollback releases session-owned
locks under the normal transaction rules. See the
[transaction coordinator guide](reusable-transaction-coordinator-guide.md).

Lock-taking batches can partially complete. Within each partition group, the first lock refusal
stops subsequent items in that group:

| Result | Caller action |
| --- | --- |
| Successful read or `DoesNotExist` | Consume the answer; inspect `Locked` separately. A missing key can still be locked. |
| `AlreadyLocked` | Inspect `HolderTransactionId` and apply the transaction's conflict policy. |
| `WaitingForReplication` or `MustRetry` | Retry within a bounded budget with backoff; keep the operation id for the same registered request. |
| `InvalidInput` | Correct the request before retrying. |

Items after a refused lock answer `MustRetry` without a lock or value. Earlier grants remain held;
there is no automatic rollback of the batch. Other partition groups can make progress independently,
so inspect every result. `Locked` reports the lock held after that item even when its read asks for a
retry. Replaying a registered lock-taking probe can renew an existing grant but does not reacquire a
lost lock; it can therefore answer `MustRetry`. Finalize or roll back the session when abandoning the
operation rather than assuming a refused batch released everything.

## Limits and cost

The limits are `KahunaConfiguration` settings:

| Setting | Default | Bound |
| --- | --- | --- |
| `MaxProbeBatchSize` | 512 | Keys in a point probe or seeks in a range probe. |
| `MaxProbeRangeEntries` | 65,536 | Sum of the requested limits of all seeks in a range probe. |

Empty or oversized requests and mixed point-key spaces are refused with `InvalidInput`. Split requests
to fit both bounds; every range limit must be at least one. Shared mode also requires a transaction
and a non-negative lock lease. These core
settings are not exposed as CLI flags or properties on `EmbeddedKahunaOptions`.

Resident reads can finish inline. Backend misses and range pages are grouped to reduce scheduler
hand-offs and resume messages; a range probe still performs the individual backend seeks. See the
[backend I/O scheduler guide](backend-io-scheduler-guide.md) for execution and inspection budgets.

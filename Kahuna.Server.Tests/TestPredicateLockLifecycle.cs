
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Covers the predicate/range-lock lifecycle hardening:
///   • prefix-lock acquisition is atomic — a mid-loop replication conflict rolls back every write
///     intent it stamped, so no key in the bucket is left holding a stray intent and the prefix
///     record is never installed;
///   • expired range locks are pruned on acquire and before export, so an abandoned lock neither
///     blocks a fresh acquire nor is carried into a split/merge transfer set as if it were live.
/// </summary>
public sealed class TestPredicateLockLifecycle
{
    [Fact]
    public async Task PrefixLockAcquire_FailsMidLoop_StrandsNoIntents()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext(CreateConfiguration());
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp live = now + 60_000;

        // Three keys in one bucket, walked in ordinal order. The last one is mid-replication, so the
        // acquire must abort after already stamping the first two — the exact strand scenario.
        InsertEntry(context, "orders/a", now);
        InsertEntry(context, "orders/b", now);
        KeyValueEntry blocked = InsertEntry(context, "orders/c", now);
        blocked.ReplicationIntent = new KeyValueReplicationIntent { ProposalId = 1, Expires = live };

        HLCTimestamp prefixTx = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        TryAcquireExclusivePrefixLockHandler acquire = new(context);

        KeyValueResponse response = acquire.Execute(BuildRequest(
            KeyValueRequestType.TryAcquireExclusivePrefixLock, prefixTx, key: "orders", expiresMs: 10_000));

        Assert.Equal(KeyValueResponseType.WaitingForReplication, response.Type);

        // Rollback: neither earlier key retains an intent, and the prefix record was never installed.
        Assert.Null(context.Store.Get("orders/a")!.WriteIntent);
        Assert.Null(context.Store.Get("orders/b")!.WriteIntent);
        Assert.False(context.LocksByPrefix.ContainsKey("orders"));

        // A subsequent unrelated lock on a rolled-back key is not blocked by a stranded intent.
        HLCTimestamp otherTx = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        TryAcquireExclusiveLockHandler singleKey = new(context);
        KeyValueResponse follow = await singleKey.Execute(BuildRequest(
            KeyValueRequestType.TryAcquireExclusiveLock, otherTx, key: "orders/a", expiresMs: 10_000,
            durability: KeyValueDurability.Ephemeral));

        Assert.Equal(KeyValueResponseType.Locked, follow.Type);
    }

    [Fact]
    public void ExpiredRangeLocks_ArePruned_OnAccessAndExport()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext(CreateConfiguration());
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp expired = new(now.N, TimeSpan.FromSeconds(1).Ticks, now.C);
        HLCTimestamp live = now + 60_000;

        // Lock identities are pure identity, unrelated to the Expires field below. Place them an
        // hour past `now` so the real-clock freshTx (a few ms after `now`) can never collide with
        // one of them — a 1-2 ms offset here was flaky when the wall clock ticked between calls.
        HLCTimestamp abandonedTx = now + 3_600_000;
        HLCTimestamp holderTx = now + 3_600_001;

        // ── Pruned on acquire ──────────────────────────────────────────────────────────────
        // "acc" holds an expired lock over [a,m) and a live lock over [p,z). A fresh acquire whose
        // range overlaps only the expired lock must succeed (the stale lock is pruned, not merely
        // skipped) and the expired entry must be gone from the live list afterwards.
        context.LocksByRange["acc"] =
        [
            RangeLock(abandonedTx, expired, "acc/a", "acc/m"),
            RangeLock(holderTx, live, "acc/p", "acc/z"),
        ];

        HLCTimestamp freshTx = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        TryAcquireExclusiveRangeLockHandler acquire = new(context);
        KeyValueResponse acquired = acquire.Execute(BuildRangeRequest(
            freshTx, key: "acc", startKey: "acc/b", endKey: "acc/f", expiresMs: 10_000));

        Assert.Equal(KeyValueResponseType.Locked, acquired.Type);
        List<KeyValueRangeLock> accLocks = context.LocksByRange["acc"];
        Assert.DoesNotContain(accLocks, l => l.TransactionId == abandonedTx);
        Assert.Contains(accLocks, l => l.TransactionId == holderTx);
        Assert.Contains(accLocks, l => l.TransactionId == freshTx);

        // ── Pruned on export ───────────────────────────────────────────────────────────────
        // The transfer snapshot (GetRangeLocks) must never carry an expired lock as if it were live,
        // and the live list must be pruned as a side effect of the export.
        context.LocksByRange["exp"] =
        [
            RangeLock(abandonedTx, expired, "exp/a", "exp/m"),
            RangeLock(holderTx, live, "exp/p", "exp/z"),
        ];

        GetRangeLocksHandler export = new(context);
        KeyValueResponse snapshot = export.Execute(BuildRequest(
            KeyValueRequestType.GetRangeLocks, HLCTimestamp.Zero, key: "exp", expiresMs: 0));

        Assert.NotNull(snapshot.RangeLockList);
        Assert.DoesNotContain(snapshot.RangeLockList!, l => l.TransactionId == abandonedTx);
        Assert.Contains(snapshot.RangeLockList!, l => l.TransactionId == holderTx);
        Assert.DoesNotContain(context.LocksByRange["exp"], l => l.TransactionId == abandonedTx);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static KeyValueEntry InsertEntry(KeyValueContext context, string key, HLCTimestamp now)
    {
        int slash = key.LastIndexOf('/');
        KeyValueEntry entry = new()
        {
            Value = [1],
            State = KeyValueState.Set,
            Bucket = slash == -1 ? null : key[..slash],
            Revision = 0,
            FlushedRevision = 0,
            LastUsed = now,
            LastModified = now,
            Expires = HLCTimestamp.Zero
        };
        context.InsertStoreEntry(key, entry);
        return entry;
    }

    private static KeyValueRangeLock RangeLock(HLCTimestamp tx, HLCTimestamp expires, string startKey, string endKey) => new()
    {
        TransactionId = tx,
        Expires = expires,
        StartKey = startKey,
        StartInclusive = true,
        EndKey = endKey,
        EndInclusive = false,
        Mode = RangeLockMode.Exclusive
    };

    private static KeyValueRequest BuildRequest(
        KeyValueRequestType type, HLCTimestamp transactionId, string key, int expiresMs,
        KeyValueDurability durability = KeyValueDurability.Ephemeral)
        => new(type, transactionId, HLCTimestamp.Zero, key, null, null, -1, KeyValueFlags.None,
            expiresMs, HLCTimestamp.Zero, durability, 0, 0, null);

    private static KeyValueRequest BuildRangeRequest(
        HLCTimestamp transactionId, string key, string startKey, string endKey, int expiresMs)
    {
        KeyValueRequest request = BuildRequest(
            KeyValueRequestType.TryAcquireExclusiveRangeLock, transactionId, key, expiresMs);
        request.StartKey = startKey;
        request.StartInclusive = true;
        request.EndKey = endKey;
        request.EndInclusive = false;
        request.RangeLockMode = RangeLockMode.Exclusive;
        return request;
    }

    private static (KeyValueContext, RaftManager) CreateContext(KahunaConfiguration config)
    {
        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "predicate-lock-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        KeyValueContext context = new(
            null!,
            store,
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,
            null!,
            null!,
            raft,
            new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger),
            config,
            logger
        );

        return (context, raft);
    }

    private static KahunaConfiguration CreateConfiguration()
    {
        return ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 50_000,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = 1000,
            RevisionRetention = 16
        });
    }
}

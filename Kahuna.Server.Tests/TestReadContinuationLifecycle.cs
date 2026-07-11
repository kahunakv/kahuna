
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
/// Covers the resumable-read continuation lifecycle hardening:
///   • a range scan whose captured entry was evicted between pages must not record MVCC/byte
///     accounting on the orphaned object (no drift, no lost OCC), while a still-resident key does;
///   • the periodic collect sweep expires a past-deadline continuation — resolving its waiters with
///     MustRetry — and a completion that arrives afterwards is dropped;
///   • a continuation admits at most its waiter cap.
/// </summary>
public sealed class TestReadContinuationLifecycle
{
    [Fact]
    public void RangeScanContinuation_KeyEvictedBetweenPages_NoAccountingDriftNoLostOcc()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext(CreateConfiguration());
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp txId = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // "s/a" was resident when the scan started (captured into memItems) but a Collect evicted it
        // before this page resumes — so it is NOT in the live store. "s/b" is still resident.
        byte[] orphanValue = [1, 2, 3];
        KeyValueEntry orphan = new()
        {
            Bucket = "s", Value = orphanValue, State = KeyValueState.Set,
            Revision = 1, FlushedRevision = 1, LastUsed = now, LastModified = now, Expires = HLCTimestamp.Zero
        };

        byte[] residentValue = [4, 5];
        KeyValueEntry resident = new()
        {
            Bucket = "s", Value = residentValue, State = KeyValueState.Set,
            Revision = 1, FlushedRevision = 1, LastUsed = now, LastModified = now, Expires = HLCTimestamp.Zero
        };
        context.InsertStoreEntry("s/b", resident);

        long baselineBytes = context.ApproximateStoreBytes;

        RangeScanContinuation cont = new(
            prefix: "s", limit: 100, durability: KeyValueDurability.Ephemeral, partitionId: 0,
            startInclusive: true, startKey: null, endKey: null, endInclusive: false,
            transactionId: txId, snapshotTs: HLCTimestamp.Zero, currentTime: now, isSnapshotRead: false,
            memItems: [("s/a", orphan), ("s/b", resident)],
            memEnd: null, memEndInclusive: false, memBatch: 100, memMaybeMore: false, diskCursor: "s",
            promise: new TaskCompletionSource<KeyValueResponse?>());

        cont.Execute(context);

        // The orphan captured from a prior page must not have OCC state written onto it — later ops
        // re-load a different object, so an MVCC entry here would be silently lost.
        Assert.True(orphan.MvccEntries is null || !orphan.MvccEntries.ContainsKey(txId),
            "evicted (non-resident) captured entry must not receive an MVCC snapshot");

        // The still-resident key does get OCC tracking (OCC still fires).
        Assert.NotNull(resident.MvccEntries);
        Assert.True(resident.MvccEntries!.ContainsKey(txId));

        // No accounting drift: the only byte increase is the resident key's MVCC snapshot; the orphan
        // contributes nothing (under the old isResident guard it would have leaked its snapshot bytes).
        long expectedDelta = KeyValueStoreAccounting.MvccEntryAddedBytes(dictionaryJustCreated: true, residentValue);
        Assert.Equal(baselineBytes + expectedDelta, context.ApproximateStoreBytes);
    }

    [Fact]
    public void ReadContinuation_DeadlineExpires_ResolvesWaitersMustRetry_IgnoresLateCompletion()
    {
        KahunaConfiguration config = CreateConfiguration();
        (TryCollectHandler collect, KeyValueContext context, RaftManager raft) = CreateCollectHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        const string key = "reads/pending";
        TaskCompletionSource<KeyValueResponse?> primary = new();
        TaskCompletionSource<KeyValueResponse?> waiter = new();

        PointReadContinuation cont = new(key, KeyValueResponseType.Get, primary);
        Assert.True(cont.AddWaiter(waiter));
        // Deadline already in the past → the next collect sweep must expire it.
        cont.Deadline = new HLCTimestamp(now.N, TimeSpan.FromSeconds(1).Ticks, now.C);
        context.PendingReads[(key, -1L, false)] = cont;

        collect.Execute();

        Assert.True(cont.Cancelled);
        Assert.False(context.PendingReads.ContainsKey((key, -1L, false)));
        Assert.True(primary.Task.IsCompletedSuccessfully);
        Assert.True(waiter.Task.IsCompletedSuccessfully);
        Assert.Equal(KeyValueResponseType.MustRetry, primary.Task.Result!.Type);
        Assert.Equal(KeyValueResponseType.MustRetry, waiter.Task.Result!.Type);

        // Late completion: the backend read finally finishes and dispatches ResumeRead. It must be
        // dropped — running Execute now would populate the cache for a read the callers already retried.
        cont.DiskResult = new KeyValueEntry
        {
            Value = [9], State = KeyValueState.Set, Revision = 5, FlushedRevision = 5, Expires = HLCTimestamp.Zero
        };
        ResumeReadHandler resume = new(context);
        resume.Execute(new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = cont });

        Assert.False(context.Store.ContainsKey(key), "a late completion for an expired read must not touch the store");
    }

    [Fact]
    public void ReadContinuation_WaiterCap_IsEnforced()
    {
        TaskCompletionSource<KeyValueResponse?> primary = new();
        PointReadContinuation cont = new(key: "reads/hot", KeyValueResponseType.Get, primary) { MaxWaiters = 3 };

        // Primary already occupies one slot; two more coalesced waiters fit, the fourth is refused.
        Assert.True(cont.AddWaiter(new TaskCompletionSource<KeyValueResponse?>()));
        Assert.True(cont.AddWaiter(new TaskCompletionSource<KeyValueResponse?>()));
        Assert.False(cont.AddWaiter(new TaskCompletionSource<KeyValueResponse?>()));
        Assert.Equal(3, cont.WaiterCount);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static (TryCollectHandler, KeyValueContext, RaftManager) CreateCollectHandler(KahunaConfiguration config)
    {
        (KeyValueContext context, RaftManager raft) = CreateContext(config);
        return (new TryCollectHandler(context), context, raft);
    }

    private static (KeyValueContext, RaftManager) CreateContext(KahunaConfiguration config)
    {
        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "read-continuation-test",
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

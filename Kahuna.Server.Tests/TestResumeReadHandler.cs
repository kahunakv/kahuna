
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
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
/// Unit tests for the resumable-read infrastructure: the ResumeReadHandler and the
/// ReadContinuation abstraction it delegates to.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestResumeReadHandler
{
    // ── (a) Disk result, no concurrent write — disk value served and cached ───────────────

    [Fact]
    public async Task PointRead_NoConcurrentWrite_DiskValueCachedAndServed()
    {
        (ResumeReadHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Stage 2 produced a disk entry (revision 5).
        KeyValueEntry diskEntry = new()
        {
            Value = Encoding.UTF8.GetBytes("from-disk"),
            Revision = 5,
            FlushedRevision = 5,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set
        };

        var promise = new TaskCompletionSource<KeyValueResponse?>();
        var cont = new PointReadContinuation("mykey", KeyValueResponseType.Get, promise) { DiskResult = diskEntry };
        KeyValueRequest msg = MakeResumeMsg(cont);

        handler.Execute(msg);

        Assert.True(promise.Task.IsCompleted);
        KeyValueResponse? resp = await promise.Task;
        Assert.NotNull(resp);
        Assert.Equal(KeyValueResponseType.Get, resp!.Type);
        Assert.NotNull(resp.Entry);
        Assert.Equal(5L, resp.Entry!.Revision);
        Assert.Equal("from-disk", Encoding.UTF8.GetString(resp.Entry.Value!));

        // Entry must be inserted into the in-memory store.
        Assert.True(context.Store.ContainsKey("mykey"));
        Assert.Equal(5L, context.Store.Get("mykey")!.Revision);
    }

    // ── (b) Concurrent write landed — resident higher-revision entry wins ─────────────────

    [Fact]
    public async Task PointRead_ConcurrentWriteLanded_ResidentHigherRevisionWins()
    {
        (ResumeReadHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // A concurrent write committed revision 10 while disk read was in flight.
        KeyValueEntry resident = new()
        {
            Value = Encoding.UTF8.GetBytes("concurrent-write"),
            Revision = 10,
            FlushedRevision = 10,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set
        };
        context.InsertStoreEntry("mykey", resident);

        // Disk returned revision 5 (older).
        KeyValueEntry diskEntry = new()
        {
            Value = Encoding.UTF8.GetBytes("stale-disk"),
            Revision = 5,
            FlushedRevision = 5,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set
        };

        var promise = new TaskCompletionSource<KeyValueResponse?>();
        var cont = new PointReadContinuation("mykey", KeyValueResponseType.Get, promise) { DiskResult = diskEntry };
        handler.Execute(MakeResumeMsg(cont));

        Assert.True(promise.Task.IsCompleted);
        KeyValueResponse? resp = await promise.Task;
        Assert.NotNull(resp);
        Assert.Equal(KeyValueResponseType.Get, resp!.Type);
        Assert.Equal(10L, resp.Entry!.Revision);
        Assert.Equal("concurrent-write", Encoding.UTF8.GetString(resp.Entry.Value!));

        // Cache must not be overwritten with the stale disk value.
        Assert.Equal(10L, context.Store.Get("mykey")!.Revision);
        Assert.Equal("concurrent-write", Encoding.UTF8.GetString(context.Store.Get("mykey")!.Value!));
    }

    // ── (b-variant) Disk result is null — DoesNotExist when nothing is resident either ───

    [Fact]
    public async Task PointRead_DiskMiss_NoResident_DoesNotExistReturned()
    {
        (ResumeReadHandler handler, KeyValueContext context, _) = CreateHandler();

        var promise = new TaskCompletionSource<KeyValueResponse?>();
        // DiskResult stays null (key not on disk).
        var cont = new PointReadContinuation("missing", KeyValueResponseType.Get, promise);
        handler.Execute(MakeResumeMsg(cont));

        Assert.True(promise.Task.IsCompleted);
        KeyValueResponse? resp = await promise.Task;
        Assert.NotNull(resp);
        Assert.Equal(KeyValueResponseType.DoesNotExist, resp!.Type);

        // Key must not be inserted into the store.
        Assert.False(context.Store.ContainsKey("missing"));
    }

    // ── LRU / TouchEntry on resident-wins branch ──────────────────────────────────────────

    [Fact]
    public void PointRead_ResidentWins_TouchesEntryToLruTail()
    {
        (ResumeReadHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Insert two entries so the LRU list has a head and a tail.
        KeyValueEntry cold = new() { Value = Encoding.UTF8.GetBytes("cold"), Revision = 1, LastUsed = now, LastModified = now, State = KeyValueState.Set };
        KeyValueEntry hot = new() { Value = Encoding.UTF8.GetBytes("hot"), Revision = 2, LastUsed = now, LastModified = now, State = KeyValueState.Set };
        context.InsertStoreEntry("cold", cold);
        context.InsertStoreEntry("hot", hot);

        // "cold" is at the head (coldest); "hot" is at the tail.
        Assert.Same(cold, context.LruHead);

        // A point-read resume for "cold" — resident wins (no disk result).
        var promise = new TaskCompletionSource<KeyValueResponse?>();
        var cont = new PointReadContinuation("cold", KeyValueResponseType.Get, promise) { DiskResult = null };
        handler.Execute(MakeResumeMsg(cont));

        // After TouchEntry, "cold" must now be the tail (hottest), not the head.
        Assert.NotSame(cold, context.LruHead);
        KeyValueEntry? tail = context.LruHead;
        while (tail!.LruNext is not null) tail = tail.LruNext;
        Assert.Same(cold, tail);
    }

    // ── tombstone / expiry guards ─────────────────────────────────────────────────────────

    [Fact]
    public async Task PointRead_DiskTombstone_ReturnsDoesNotExist()
    {
        (ResumeReadHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Disk returned a deleted tombstone (State = Deleted, revision 3).
        KeyValueEntry tombstone = new()
        {
            Value = null,
            Revision = 3,
            FlushedRevision = 3,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Deleted
        };

        var promise = new TaskCompletionSource<KeyValueResponse?>();
        var cont = new PointReadContinuation("k", KeyValueResponseType.Get, promise) { DiskResult = tombstone };
        handler.Execute(MakeResumeMsg(cont));

        Assert.True(promise.Task.IsCompleted);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await promise.Task)!.Type);

        // Tombstone is inserted into the cache for housekeeping (eviction/expiry paths need it),
        // but must not be served as a live value.
        Assert.True(context.Store.ContainsKey("k"));
        Assert.Equal(KeyValueState.Deleted, context.Store.Get("k")!.State);
    }

    [Fact]
    public async Task PointRead_ResidentTombstone_ReturnsDoesNotExist()
    {
        (ResumeReadHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // A concurrent delete landed while the disk read was in flight — resident is a tombstone
        // with revision 5, disk returned revision 3 (older live value).
        KeyValueEntry residentTombstone = new()
        {
            Value = null,
            Revision = 5,
            FlushedRevision = 5,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Deleted
        };
        context.InsertStoreEntry("k", residentTombstone);

        KeyValueEntry diskEntry = new()
        {
            Value = Encoding.UTF8.GetBytes("stale-value"),
            Revision = 3,
            FlushedRevision = 3,
            LastModified = now,
            LastUsed = now,
            State = KeyValueState.Set
        };

        var promise = new TaskCompletionSource<KeyValueResponse?>();
        var cont = new PointReadContinuation("k", KeyValueResponseType.Get, promise) { DiskResult = diskEntry };
        handler.Execute(MakeResumeMsg(cont));

        Assert.True(promise.Task.IsCompleted);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await promise.Task)!.Type);

        // Cache must still hold the resident tombstone (not overwritten with the stale live value).
        Assert.Equal(KeyValueState.Deleted, context.Store.Get("k")!.State);
        Assert.Equal(5L, context.Store.Get("k")!.Revision);
    }

    [Fact]
    public async Task PointRead_DiskExpiredEntry_ReturnsDoesNotExist()
    {
        (ResumeReadHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Disk returned an entry whose TTL has already elapsed.
        // HLCTimestamp(0, 1, 0) is 1 tick — far in the past relative to any real now.
        HLCTimestamp pastExpiry = new(0, 1, 0);
        KeyValueEntry expired = new()
        {
            Value = Encoding.UTF8.GetBytes("expired-value"),
            Revision = 2,
            FlushedRevision = 2,
            LastModified = now,
            LastUsed = now,
            Expires = pastExpiry,
            State = KeyValueState.Set
        };

        var promise = new TaskCompletionSource<KeyValueResponse?>();
        var cont = new PointReadContinuation("k", KeyValueResponseType.Get, promise) { DiskResult = expired };
        handler.Execute(MakeResumeMsg(cont));

        Assert.True(promise.Task.IsCompleted);
        Assert.Equal(KeyValueResponseType.DoesNotExist, (await promise.Task)!.Type);
    }

    // ── (c) Multi-page continuation — first call defers, second call resolves ─────────────

    [Fact]
    public async Task MultiPage_FirstCallDefers_SecondCallResolvesWithAggregatedResult()
    {
        (ResumeReadHandler handler, KeyValueContext context, _) = CreateHandler();

        var promise = new TaskCompletionSource<KeyValueResponse?>();
        var cont = new TwoPageTestContinuation(promise);

        // First page — continuation records the page but does not resolve yet.
        cont.DiskResult = null;
        handler.Execute(MakeResumeMsg(cont));
        Assert.Equal(1, cont.ExecuteCount);
        Assert.False(promise.Task.IsCompleted, "promise must not be resolved after first page");

        // Second page — continuation resolves with the aggregated result.
        cont.DiskResult = null;
        handler.Execute(MakeResumeMsg(cont));
        Assert.Equal(2, cont.ExecuteCount);
        Assert.True(promise.Task.IsCompleted, "promise must be resolved after final page");

        KeyValueResponse? resp = await promise.Task;
        Assert.NotNull(resp);
        Assert.Equal(KeyValueResponseType.Get, resp!.Type);
    }

    // ── (c-variant) Multi-page terminates at page limit ──────────────────────────────────

    [Fact]
    public void MultiPage_TerminatesAtPageLimit()
    {
        (ResumeReadHandler handler, _, _) = CreateHandler();

        var promise = new TaskCompletionSource<KeyValueResponse?>();
        var cont = new TwoPageTestContinuation(promise);

        // Two calls → promise resolved on the second.
        handler.Execute(MakeResumeMsg(cont));
        Assert.False(promise.Task.IsCompleted);
        handler.Execute(MakeResumeMsg(cont));
        Assert.True(promise.Task.IsCompleted);
        // No more calls needed.
        Assert.Equal(2, cont.ExecuteCount);
    }

    // ── null continuation guard ───────────────────────────────────────────────────────────

    [Fact]
    public void NullContinuation_ResolvesErrorAndReturnsErrored()
    {
        (ResumeReadHandler handler, _, _) = CreateHandler();

        // Message has no Continuation set — the type-only constructor leaves it null.
        KeyValueRequest msg = new(KeyValueRequestType.ResumeRead);

        KeyValueResponse? result = handler.Execute(msg);

        Assert.NotNull(result);
        Assert.Equal(KeyValueResponseType.Errored, result!.Type);
    }

    // ── stage-3 exception must fail every waiter and clear the in-flight registration ─────

    [Fact]
    public async Task ThrowingContinuation_ResolvesAllWaitersMustRetry_AndClearsPendingRegistration()
    {
        (ResumeReadHandler handler, KeyValueContext context, _) = CreateHandler();

        (string, long, bool) key = ("hot", -1L, false);

        var primary = new TaskCompletionSource<KeyValueResponse?>();
        var cont = new ThrowingTestContinuation(key, primary);

        // Two more callers coalesced onto the same in-flight read (as stage 1 would attach them).
        var waiter2 = new TaskCompletionSource<KeyValueResponse?>();
        var waiter3 = new TaskCompletionSource<KeyValueResponse?>();
        cont.AddWaiter(waiter2);
        cont.AddWaiter(waiter3);

        // The in-flight read is registered so later arrivals would coalesce onto it.
        context.PendingReads[key] = cont;

        // Execute throws inside stage 3. The handler must swallow it (ResumeRead is fire-and-forget)
        // and route through Fail() so no caller is left hanging and the registration is not leaked.
        KeyValueResponse? ret = handler.Execute(MakeResumeMsg(cont));
        Assert.Null(ret);

        foreach (TaskCompletionSource<KeyValueResponse?> w in new[] { primary, waiter2, waiter3 })
        {
            Assert.True(w.Task.IsCompleted, "every coalesced waiter must be resolved, never stranded");
            Assert.Equal(KeyValueResponseType.MustRetry, (await w.Task)!.Type);
        }

        // The dead continuation must be gone from the map, so the next miss starts a fresh read
        // instead of attaching to a continuation that will never resolve.
        Assert.False(context.PendingReads.ContainsKey(key));
    }

    // ── Range snapshot: in-memory entries are projected to their as-of revision ────────────

    [Fact]
    public async Task RangeSnapshotScan_ProjectsInMemoryEntriesToRevisionAtOrBeforeSnapshot()
    {
        (ResumeReadHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp snapshotTs = new(0, 100, 0);

        // r/a — current revision committed at-or-before the snapshot: served as its current value.
        KeyValueEntry a = new()
        {
            Value = Encoding.UTF8.GetBytes("a-latest"),
            Revision = 1,
            LastUsed = now,
            LastModified = new HLCTimestamp(0, 50, 0),
            State = KeyValueState.Set
        };

        // r/b — current revision (5) is newer than the snapshot, but revision 2 was committed
        // at-or-before it: the snapshot scan must serve revision 2, not 5.
        KeyValueEntry b = new()
        {
            Value = Encoding.UTF8.GetBytes("b-v5"),
            Revision = 5,
            LastUsed = now,
            LastModified = new HLCTimestamp(0, 150, 0),
            State = KeyValueState.Set,
            Revisions = new()
            {
                [2] = new KeyValueRevisionEntry(
                    Encoding.UTF8.GetBytes("b-v2"), new HLCTimestamp(0, 60, 0),
                    HLCTimestamp.Zero, KeyValueState.Set)
            }
        };

        // r/c — current revision newer than the snapshot and no retained revision at-or-before it:
        // the key did not exist as of the snapshot and must be excluded.
        KeyValueEntry c = new()
        {
            Value = Encoding.UTF8.GetBytes("c-v3"),
            Revision = 3,
            LastUsed = now,
            LastModified = new HLCTimestamp(0, 150, 0),
            State = KeyValueState.Set
        };

        // memItems mirror the in-memory store snapshot taken at stage 1 (sorted, resident entries).
        context.InsertStoreEntry("r/a", a);
        context.InsertStoreEntry("r/b", b);
        context.InsertStoreEntry("r/c", c);
        List<(string, KeyValueEntry)> memItems = [("r/a", a), ("r/b", b), ("r/c", c)];

        var promise = new TaskCompletionSource<KeyValueResponse?>();
        var cont = new RangeScanContinuation(
            prefix: "r/", limit: 10, durability: KeyValueDurability.Persistent, partitionId: 0,
            startInclusive: true, startKey: null, endKey: null, endInclusive: false,
            transactionId: HLCTimestamp.Zero, snapshotTs: snapshotTs, currentTime: now,
            isSnapshotRead: true, memItems: memItems,
            memEnd: null, memEndInclusive: false, memBatch: 512, memMaybeMore: false,
            diskCursor: "r/", promise: promise)
        {
            ScanDiskResult = []   // no disk rows: all results come from the in-memory snapshot
        };

        handler.Execute(MakeResumeMsg(cont));

        Assert.True(promise.Task.IsCompleted);
        KeyValueResponse? resp = await promise.Task;
        Assert.NotNull(resp);
        Assert.Equal(KeyValueResponseType.Get, resp!.Type);
        Assert.NotNull(resp.RangeResult);

        Dictionary<string, (long Rev, string Val)> byKey = resp.RangeResult!.Items
            .ToDictionary(i => i.Item1, i => (i.Item2.Revision, Encoding.UTF8.GetString(i.Item2.Value!)));

        // r/a visible at its current revision.
        Assert.True(byKey.ContainsKey("r/a"));
        Assert.Equal((1L, "a-latest"), byKey["r/a"]);

        // r/b projected back to the as-of revision, not its latest.
        Assert.True(byKey.ContainsKey("r/b"));
        Assert.Equal((2L, "b-v2"), byKey["r/b"]);

        // r/c excluded — did not exist as of the snapshot.
        Assert.False(byKey.ContainsKey("r/c"));
    }

    // ── Range scan re-queries memory in bounded batches (no O(N) stage-1 copy) ────────────

    [Fact]
    public async Task RangeScan_ResidentRangeExceedsBatch_ReQueriesRemainingBatchesInOrder()
    {
        (ResumeReadHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Five resident keys under "m/", but the continuation is handed only the first bounded
        // batch of two — as stage 1 would with a small batch. The merge must re-query the store
        // for the remaining keys rather than dropping them.
        string[] keys = ["m/a", "m/b", "m/c", "m/d", "m/e"];
        foreach (string k in keys)
            context.InsertStoreEntry(k, new KeyValueEntry
            {
                Value = Encoding.UTF8.GetBytes(k + "-val"),
                Revision = 1,
                LastUsed = now,
                LastModified = now,
                State = KeyValueState.Set
            });

        List<(string, KeyValueEntry)> firstBatch =
            [("m/a", context.Store.Get("m/a")!), ("m/b", context.Store.Get("m/b")!)];

        var promise = new TaskCompletionSource<KeyValueResponse?>();
        var cont = new RangeScanContinuation(
            prefix: "m/", limit: 10, durability: KeyValueDurability.Persistent, partitionId: 0,
            startInclusive: true, startKey: null, endKey: null, endInclusive: false,
            transactionId: HLCTimestamp.Zero, snapshotTs: HLCTimestamp.Zero, currentTime: now,
            isSnapshotRead: false, memItems: firstBatch,
            memEnd: null, memEndInclusive: false, memBatch: 2, memMaybeMore: true,
            diskCursor: "m/", promise: promise)
        {
            ScanDiskResult = []   // disk empty: the whole result comes from re-queried memory batches
        };

        handler.Execute(MakeResumeMsg(cont));

        Assert.True(promise.Task.IsCompleted);
        KeyValueResponse? resp = await promise.Task;
        Assert.NotNull(resp);
        Assert.Equal(KeyValueResponseType.Get, resp!.Type);

        // All five keys must be returned in order despite the initial batch holding only two.
        List<string> returned = resp.RangeResult!.Items.Select(i => i.Item1).ToList();
        Assert.Equal(keys, returned);
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static KeyValueRequest MakeResumeMsg(ReadContinuation cont) =>
        new(KeyValueRequestType.ResumeRead) { Continuation = cont };

    private static (ResumeReadHandler, KeyValueContext, RaftManager) CreateHandler()
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
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

        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "resume-read-test",
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

        return (new ResumeReadHandler(context), context, raft);
    }

    /// <summary>
    /// A two-page test continuation: first Execute defers (does not resolve the promise);
    /// second Execute resolves. Simulates a multi-page scan without requiring a live actor.
    /// </summary>
    private sealed class TwoPageTestContinuation : ReadContinuation
    {
        internal int ExecuteCount { get; private set; }

        internal TwoPageTestContinuation(TaskCompletionSource<KeyValueResponse?> promise)
            : base(promise) { }

        internal override void Execute(KeyValueContext context)
        {
            ExecuteCount++;

            if (ExecuteCount >= 2)
            {
                // Final page — resolve with a synthetic result.
                Promise.TrySetResult(new KeyValueResponse(
                    KeyValueResponseType.Get,
                    new ReadOnlyKeyValueEntry(null, 0, HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set)
                ));
            }
            // First call: accumulate page results (simulated), defer resolution.
        }
    }

    /// <summary>
    /// A continuation whose Execute always throws, used to prove the ResumeRead handler's
    /// failure path: a stage-3 exception must fail every waiter and remove the in-flight key.
    /// RemovePendingKey mirrors what a real coalescing continuation does, so Fail() can undo
    /// the stage-1 registration.
    /// </summary>
    private sealed class ThrowingTestContinuation : ReadContinuation
    {
        private readonly (string, long, bool) pendingKey;

        internal ThrowingTestContinuation(
            (string, long, bool) pendingKey, TaskCompletionSource<KeyValueResponse?> promise)
            : base(promise)
        {
            this.pendingKey = pendingKey;
        }

        internal override void RemovePendingKey(KeyValueContext context) =>
            context.PendingReads.Remove(pendingKey);

        internal override void Execute(KeyValueContext context) =>
            throw new InvalidOperationException("simulated stage-3 continuation failure");
    }
}

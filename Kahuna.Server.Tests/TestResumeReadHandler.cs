
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
    public void PointRead_NoConcurrentWrite_DiskValueCachedAndServed()
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
        KeyValueResponse? resp = promise.Task.Result;
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
    public void PointRead_ConcurrentWriteLanded_ResidentHigherRevisionWins()
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
        KeyValueResponse? resp = promise.Task.Result;
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
    public void PointRead_DiskMiss_NoResident_DoesNotExistReturned()
    {
        (ResumeReadHandler handler, KeyValueContext context, _) = CreateHandler();

        var promise = new TaskCompletionSource<KeyValueResponse?>();
        // DiskResult stays null (key not on disk).
        var cont = new PointReadContinuation("missing", KeyValueResponseType.Get, promise);
        handler.Execute(MakeResumeMsg(cont));

        Assert.True(promise.Task.IsCompleted);
        KeyValueResponse? resp = promise.Task.Result;
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
    public void PointRead_DiskTombstone_ReturnsDoesNotExist()
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
        Assert.Equal(KeyValueResponseType.DoesNotExist, promise.Task.Result!.Type);

        // Tombstone is inserted into the cache for housekeeping (eviction/expiry paths need it),
        // but must not be served as a live value.
        Assert.True(context.Store.ContainsKey("k"));
        Assert.Equal(KeyValueState.Deleted, context.Store.Get("k")!.State);
    }

    [Fact]
    public void PointRead_ResidentTombstone_ReturnsDoesNotExist()
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
        Assert.Equal(KeyValueResponseType.DoesNotExist, promise.Task.Result!.Type);

        // Cache must still hold the resident tombstone (not overwritten with the stale live value).
        Assert.Equal(KeyValueState.Deleted, context.Store.Get("k")!.State);
        Assert.Equal(5L, context.Store.Get("k")!.Revision);
    }

    [Fact]
    public void PointRead_DiskExpiredEntry_ReturnsDoesNotExist()
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
        Assert.Equal(KeyValueResponseType.DoesNotExist, promise.Task.Result!.Type);
    }

    // ── (c) Multi-page continuation — first call defers, second call resolves ─────────────

    [Fact]
    public void MultiPage_FirstCallDefers_SecondCallResolvesWithAggregatedResult()
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

        KeyValueResponse? resp = promise.Task.Result;
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
}

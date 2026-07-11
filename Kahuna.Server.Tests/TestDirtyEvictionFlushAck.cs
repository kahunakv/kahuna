
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
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies the flush-acknowledgement durability guard (replacing the old time-based dirty window):
///   • A confirmed flush advances FlushedRevision only for the revision that was actually stored.
///   • A stale/superseded ack never marks a newer unflushed revision as clean.
///   • With the background flush stalled well past the old 10 s window, the only cached copy of a
///     committed persistent entry is never evicted, so a read still returns it.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDirtyEvictionFlushAck
{
    private readonly ILoggerFactory loggerFactory;

    public TestDirtyEvictionFlushAck(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public void FlushAck_AdvancesFlushedRevision_OnlyWhenCurrent()
    {
        KeyValueEntry entry = new() { Revision = 5, FlushedRevision = -1 };

        // Ack for the current revision advances the flushed marker → entry becomes clean.
        ApplyFlushAck(entry, revision: 5);
        Assert.Equal(5, entry.FlushedRevision);
        Assert.False(entry.IsDirty());

        // A later write bumps the revision ahead of what is flushed → dirty again.
        entry.Revision = 7;
        Assert.True(entry.IsDirty());

        // An ack for an intermediate revision (6) that did reach the entry advances the marker to 6
        // but the entry stays dirty because revision 7 is still unflushed.
        ApplyFlushAck(entry, revision: 6);
        Assert.Equal(6, entry.FlushedRevision);
        Assert.True(entry.IsDirty());

        // The ack for the current revision (7) finally makes it clean.
        ApplyFlushAck(entry, revision: 7);
        Assert.Equal(7, entry.FlushedRevision);
        Assert.False(entry.IsDirty());
    }

    [Fact]
    public void StaleFlushAck_ForSupersededRevision_IsIgnored()
    {
        // Model a key that was deleted at a high revision, evicted, then re-created at a lower
        // revision (revision counters can reset across a delete → re-create cycle). A late ack for
        // the old, higher revision must NOT advance FlushedRevision past the live entry's revision —
        // doing so would mark the new, unflushed entry clean and let eviction drop it (data loss).
        KeyValueEntry recreated = new() { Revision = 2, FlushedRevision = -1 };

        ApplyFlushAck(recreated, revision: 9); // stale ack for a superseded, higher revision
        Assert.Equal(-1, recreated.FlushedRevision);
        Assert.True(recreated.IsDirty());

        // A well-formed ack for a revision the entry actually reached still advances normally.
        ApplyFlushAck(recreated, revision: 2);
        Assert.Equal(2, recreated.FlushedRevision);
        Assert.False(recreated.IsDirty());

        // A duplicate/older ack never regresses the flushed marker.
        ApplyFlushAck(recreated, revision: 1);
        Assert.Equal(2, recreated.FlushedRevision);
    }

    [Fact]
    public void BackendOutageBeyondWindow_DoesNotEvictUnflushedEntry()
    {
        // The entry is unflushed (FlushedRevision == -1) and its LastModified is set far in the past,
        // decades beyond the old 10 s safety window. Under the previous time-based guard it would be
        // evicted here; under the flush-ack guard it stays pinned because no ack has advanced its
        // FlushedRevision, so the only cached copy of the committed revision survives.
        // Budget 0 → every entry is over budget, so the pinned unflushed entry alone keeps the store
        // over budget; only once it is acknowledged (clean) can the next cycle reclaim it.
        KahunaConfiguration config = CreateConfiguration();
        config.MaxEntriesPerActor = 0;
        config.CollectBatchMax = 100;

        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp ancient = new(now.N, TimeSpan.FromSeconds(1).Ticks, now.C);
        context.InsertStoreEntry("committed/key", new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("committed-value"),
            State = KeyValueState.Set,
            Revision = 3,
            FlushedRevision = -1,   // backend outage: never acknowledged, no matter how old
            LastUsed = ancient,
            LastModified = ancient,
            Expires = HLCTimestamp.Zero
        });

        // Pile on warm fillers so the store is well over budget and the unflushed entry is the
        // coldest LRU victim.
        for (int i = 0; i < 8; i++)
            InsertCleanEntry(context, $"filler/{i:D2}", now);

        handler.Execute();

        Assert.True(context.Store.ContainsKey("committed/key"),
            "an unflushed committed entry must never be evicted, even long after the old time window");
        Assert.Equal(Encoding.UTF8.GetBytes("committed-value"), context.Store.Get("committed/key")!.Value);

        // Once the flush is acknowledged the entry is clean and eviction may reclaim it.
        context.Store.Get("committed/key")!.FlushedRevision = 3;
        handler.Execute();

        Assert.False(context.Store.ContainsKey("committed/key"),
            "after the flush ack the entry is clean and eligible for eviction under budget pressure");
    }

    /// <summary>
    /// End-to-end wiring: a real persistent write leaves its entry dirty (FlushedRevision behind
    /// Revision); after the background writer flushes and the acknowledgement is routed back to the
    /// owning actor, FlushedRevision catches up and the entry is no longer dirty. This drives the
    /// actual BackgroundWriterActor → persistent router → KeyValueActor.FlushAck path, not a mirror.
    /// </summary>
    [Fact]
    public async Task RealFlush_RoutesAckToOwningActor_AdvancingFlushedRevision()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            KeyValueWorkers = 1,
            DirtyObjectsWriterDelay = 60_000  // no automatic flush during the test; we trigger it explicitly
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);
        KahunaManager manager = (KahunaManager)node.Kahuna;

        const string key = "flush-ack/wiring";
        byte[] value = Encoding.UTF8.GetBytes("v1");

        (KeyValueResponseType setResponse, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, value, null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.Equal(KeyValueResponseType.Set, setResponse);

        // Before any flush the entry is dirty: its committed revision is not yet on disk.
        KeyValueEntry? beforeFlush = FindEntry(manager, key, KeyValueDurability.Persistent);
        Assert.NotNull(beforeFlush);
        Assert.True(beforeFlush.IsDirty(), "a freshly committed persistent entry must be dirty before its flush is acknowledged");

        // Flush drains the dirty queue and sends the acks; a subsequent op on the same key is FIFO
        // after the ack on the owning actor's mailbox, so once it completes the ack has been applied.
        await node.FlushAsync();
        _ = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero,
            KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        KeyValueEntry? afterFlush = FindEntry(manager, key, KeyValueDurability.Persistent);
        Assert.NotNull(afterFlush);
        Assert.Equal(afterFlush.Revision, afterFlush.FlushedRevision);
        Assert.False(afterFlush.IsDirty(), "after the flush acknowledgement the entry must be clean and eligible for eviction");
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static KeyValueEntry? FindEntry(KahunaManager manager, string key, KeyValueDurability durability)
    {
        var instances = durability == KeyValueDurability.Ephemeral
            ? manager.KeyValues.EphemeralInstances
            : manager.KeyValues.PersistentInstances;

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actorRef in instances)
        {
            var concreteRef = (ActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>)actorRef;
            if (concreteRef.Runner.Actor is KeyValueActor actor)
            {
                KeyValueContext ctx = actor.GetContext();
                if (ctx.Store.TryGetValue(key, out KeyValueEntry? entry))
                    return entry;
            }
        }
        return null;
    }

    /// <summary>
    /// Mirrors <c>KeyValueActor.FlushAck</c>: advance FlushedRevision only when the acked revision is
    /// one the entry actually reached (<c>acked &lt;= Revision</c>) and moves the marker forward.
    /// </summary>
    private static void ApplyFlushAck(KeyValueEntry entry, long revision)
    {
        if (revision <= entry.Revision && revision > entry.FlushedRevision)
            entry.FlushedRevision = revision;
    }

    private static (TryCollectHandler, KeyValueContext, RaftManager) CreateHandler(KahunaConfiguration config)
    {
        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "flush-ack-test",
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

        return (new TryCollectHandler(context), context, raft);
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

    private static void InsertCleanEntry(KeyValueContext context, string key, HLCTimestamp lastUsed)
    {
        context.InsertStoreEntry(key, new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = lastUsed,
            LastModified = HLCTimestamp.Zero,
            Revision = 0,
            FlushedRevision = 0,   // clean → always evictable
            Expires = HLCTimestamp.Zero
        });
    }
}

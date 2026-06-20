
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
/// Tests for Phase A.3 — expiry-heap and tombstone-queue driven reclamation.
/// Verifies that collect cycles are O(reclaimed) rather than O(store-size).
/// </summary>
public sealed class TestKeyValueReclamation
{
    private readonly ILoggerFactory loggerFactory;

    public TestKeyValueReclamation(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    // ── expiry heap ──────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Insert N entries with a mix of already-elapsed and future expiries.
    /// After one collect cycle only the elapsed entries must be evicted.
    /// The heap guarantees the cycle visits only the reclaimed entries, not the full store.
    /// </summary>
    [Fact]
    public void ExpiryReclamation_OnlyEvictsElapsedEntries()
    {
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Elapsed expiry (stale timestamp well before now).
        HLCTimestamp elapsed = new(now.N, now.L - TimeSpan.FromMinutes(1).Ticks, now.C);

        // Future expiry (one hour from now).
        HLCTimestamp future = new(now.N, now.L + TimeSpan.FromHours(1).Ticks, now.C);

        for (int i = 0; i < 20; i++)
            InsertEntry(context, $"expired/{i:D2}", KeyValueState.Set, now, elapsed);

        for (int i = 0; i < 30; i++)
            InsertEntry(context, $"live/{i:D2}", KeyValueState.Set, now, future);

        Assert.Equal(50, context.Store.Count);
        Assert.Equal(50, context.ExpiryHeap.Count); // all non-zero-expiry entries are in the heap

        handler.Execute();

        Assert.Equal(30, context.Store.Count);
        for (int i = 0; i < 20; i++)
            Assert.False(context.Store.ContainsKey($"expired/{i:D2}"), $"expired/{i:D2} must be evicted");
        for (int i = 0; i < 30; i++)
            Assert.True(context.Store.ContainsKey($"live/{i:D2}"), $"live/{i:D2} must survive");
    }

    /// <summary>
    /// A dirty expired entry (Revision > FlushedRevision, LastModified ≈ now) is re-enqueued in the
    /// heap so it can be reclaimed in a later cycle once the flush window elapses.
    /// In the current cycle it must not be evicted.
    /// </summary>
    [Fact]
    public void ExpiryReclamation_DirtyExpiredEntry_ReEnqueuedNotEvicted()
    {
        KahunaConfiguration config = CreateConfiguration(dirtyObjectsWriterDelay: 5000);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp elapsed = new(now.N, now.L - TimeSpan.FromMinutes(1).Ticks, now.C);

        // Freshly written entry with an already-elapsed TTL — dirty (FlushedRevision = -1).
        context.InsertStoreEntry("dirty/expired", new KeyValueEntry
        {
            Value = [1],
            State = KeyValueState.Set,
            Revision = 1,
            FlushedRevision = -1,
            LastUsed = now,
            LastModified = now,
            Expires = elapsed
        });

        Assert.Equal(1, context.ExpiryHeap.Count);

        handler.Execute();

        // Not evicted — still dirty.
        Assert.True(context.Store.ContainsKey("dirty/expired"),
            "dirty expired entry must not be evicted before the flush window elapses");

        // Re-enqueued so it will be retried in the next cycle.
        Assert.Equal(1, context.ExpiryHeap.Count);
    }

    /// <summary>
    /// An entry whose expiry is updated via TryExtend gets a fresh heap node.
    /// The old heap node (for the original expiry) is stale and must be discarded on pop.
    /// </summary>
    [Fact]
    public void ExpiryReclamation_StaleHeapNodeDiscarded_AfterExtend()
    {
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp elapsed = new(now.N, now.L - TimeSpan.FromMinutes(1).Ticks, now.C);
        HLCTimestamp extended = new(now.N, now.L + TimeSpan.FromHours(1).Ticks, now.C);

        InsertEntry(context, "extend/key", KeyValueState.Set, now, elapsed);

        // Simulate a TryExtend: update Expires in place and enqueue the new expiry.
        KeyValueEntry entry = context.Store.Get("extend/key")!;
        entry.Expires = extended;
        context.EnqueueExpiry("extend/key", extended); // new node with future expiry

        // Heap now contains two nodes for "extend/key": old (elapsed) + new (extended).
        handler.Execute();

        // The old elapsed node is stale (entry.Expires != elapsed) → discarded.
        // The new node has a future expiry → entry must survive.
        Assert.True(context.Store.ContainsKey("extend/key"),
            "extended entry must survive; old stale heap node must be discarded");
    }

    /// <summary>
    /// Zero-expiry entries must never be added to the heap and must never be evicted by the
    /// expiry drain (they have no TTL and are only candidates for LRU).
    /// </summary>
    [Fact]
    public void ExpiryReclamation_ZeroExpiry_NotAddedToHeap()
    {
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 10; i++)
            InsertEntry(context, $"noexpiry/{i}", KeyValueState.Set, now, HLCTimestamp.Zero);

        Assert.Equal(0, context.ExpiryHeap.Count);

        handler.Execute();

        Assert.Equal(10, context.Store.Count); // none evicted by expiry drain
    }

    // ── tombstone queue ──────────────────────────────────────────────────────────────────

    /// <summary>
    /// Insert K Deleted entries and N live entries. A single collect cycle must drain only the
    /// tombstones; live entries must survive without being touched.
    /// </summary>
    [Fact]
    public void TombstoneReclamation_OnlyEvictsTombstones()
    {
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 15; i++)
            InsertEntry(context, $"tomb/{i:D2}", KeyValueState.Deleted, now, HLCTimestamp.Zero);

        for (int i = 0; i < 25; i++)
            InsertEntry(context, $"live/{i:D2}", KeyValueState.Set, now, HLCTimestamp.Zero);

        Assert.Equal(40, context.Store.Count);
        Assert.Equal(15, context.TombstoneQueue.Count); // 15 tombstones enqueued

        handler.Execute();

        Assert.Equal(25, context.Store.Count);
        for (int i = 0; i < 15; i++)
            Assert.False(context.Store.ContainsKey($"tomb/{i:D2}"), $"tomb/{i:D2} must be evicted");
        for (int i = 0; i < 25; i++)
            Assert.True(context.Store.ContainsKey($"live/{i:D2}"), $"live/{i:D2} must survive");
    }

    /// <summary>
    /// A Deleted entry is re-set (overwritten) after its tombstone was enqueued.
    /// The queue entry is now stale and must be silently discarded by the drain.
    /// </summary>
    [Fact]
    public void TombstoneReclamation_StaleQueueEntry_DiscardedAfterReSet()
    {
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Insert as Deleted — auto-enqueues tombstone.
        InsertEntry(context, "reset/key", KeyValueState.Deleted, now, HLCTimestamp.Zero);
        Assert.Single(context.TombstoneQueue);

        // Re-set the entry (simulates a write after delete).
        KeyValueEntry entry = context.Store.Get("reset/key")!;
        entry.State = KeyValueState.Set;
        entry.Value = Encoding.UTF8.GetBytes("written");

        handler.Execute();

        // Stale tombstone queue entry discarded; live Set entry survives.
        Assert.True(context.Store.ContainsKey("reset/key"),
            "re-set entry must survive; stale tombstone queue entry must be discarded");
    }

    /// <summary>
    /// A dirty (unflushed) tombstone is re-enqueued so it can be retried after the safety window.
    /// The entry must not be evicted during the cycle in which it is still dirty.
    /// </summary>
    [Fact]
    public void TombstoneReclamation_DirtyTombstone_ReEnqueuedNotEvicted()
    {
        KahunaConfiguration config = CreateConfiguration(dirtyObjectsWriterDelay: 5000);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Freshly committed Deleted entry — dirty (FlushedRevision = -1, LastModified = now).
        context.InsertStoreEntry("dirty/tomb", new KeyValueEntry
        {
            Value = null,
            State = KeyValueState.Deleted,
            Revision = 1,
            FlushedRevision = -1,   // unflushed
            LastUsed = now,
            LastModified = now,
            Expires = HLCTimestamp.Zero
        });

        Assert.Single(context.TombstoneQueue);

        handler.Execute();

        // Entry must still be in the store.
        Assert.True(context.Store.ContainsKey("dirty/tomb"),
            "dirty tombstone must not be evicted before its flush window elapses");

        // Must have been re-enqueued so it can be retried in the next cycle.
        Assert.Single(context.TombstoneQueue);
    }

    // ── combined ─────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Both drains run in the same cycle and together bring the store to the expected count.
    /// </summary>
    [Fact]
    public void BothDrains_RunInSameCycle()
    {
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp elapsed = new(now.N, now.L - TimeSpan.FromMinutes(1).Ticks, now.C);

        // 5 deleted tombstones, 5 expired entries, 10 live.
        for (int i = 0; i < 5; i++)
            InsertEntry(context, $"tomb/{i}", KeyValueState.Deleted, now, HLCTimestamp.Zero);
        for (int i = 0; i < 5; i++)
            InsertEntry(context, $"exp/{i}", KeyValueState.Set, now, elapsed);
        for (int i = 0; i < 10; i++)
            InsertEntry(context, $"live/{i}", KeyValueState.Set, now, HLCTimestamp.Zero);

        handler.Execute();

        Assert.Equal(10, context.Store.Count);
        for (int i = 0; i < 10; i++)
            Assert.True(context.Store.ContainsKey($"live/{i}"), $"live/{i} must survive");
    }

    // ── budget accounting ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Heap nodes from repeated TryExtend calls accumulate without triggering budget pressure
    /// under the old count-only check. The byte-based heap overhead must fold into IsOverStoreBudget
    /// so that stale-node accumulation eventually causes collection to fire.
    /// </summary>
    [Fact]
    public void HeapBloat_FoldsIntoBytesBudget()
    {
        // Configure a tight bytes budget (512 KB) so we can hit it with heap overhead alone.
        // Each heap node ≈ 40 B; 14 000 nodes = 560 000 B > 524 288 B (512 KB).
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 100_000,       // large — entry count alone won't trigger
            MaxBytesPerActor = 512 * 1024,      // 512 KB
            CollectBatchMax = 1000,
            RevisionRetention = 16,
        });
        (_, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        HLCTimestamp future = new(now.N, now.L + TimeSpan.FromHours(1).Ticks, now.C);

        // Insert a handful of small entries — store bytes well under 512 KB.
        for (int i = 0; i < 10; i++)
            InsertEntry(context, $"key/{i}", KeyValueState.Set, now, future);

        Assert.False(context.IsOverStoreBudget(), "small store should be under budget");

        // Simulate TryExtend hammering: each extend pushes a new node onto the heap without
        // removing the old one. 40 B per node × 14 000 nodes = 560 000 B > 524 288 B (512 KB).
        HLCTimestamp extended = new(now.N, now.L + TimeSpan.FromHours(2).Ticks, now.C);
        for (int i = 0; i < 14_000; i++)
            context.EnqueueExpiry("key/0", extended);

        Assert.True(context.IsOverStoreBudget(),
            "heap-node overhead must fold into bytes budget so bloat triggers collection");
    }

    /// <summary>
    /// After collection drains the stale heap nodes, IsOverStoreBudget should return false again.
    /// </summary>
    [Fact]
    public void HeapBloat_DrainedByCollect_BudgetRestored()
    {
        KahunaConfiguration config = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = 100_000,
            MaxBytesPerActor = 512 * 1024,  // 512 KB — same tight budget as the bloat test
            CollectBatchMax = 100_000,
            RevisionRetention = 16,
        });
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Insert one entry with an already-elapsed TTL so the heap drain can consume all nodes.
        HLCTimestamp elapsed = new(now.N, now.L - TimeSpan.FromMinutes(1).Ticks, now.C);
        InsertEntry(context, "key/bloat", KeyValueState.Set, now, elapsed);

        // Simulate many extends that all produced heap nodes, all with elapsed timestamps.
        for (int i = 0; i < 14_000; i++)
            context.EnqueueExpiry("key/bloat", elapsed);

        Assert.True(context.IsOverStoreBudget(), "must be over budget before collect");

        handler.Execute();

        // Heap drain consumed all nodes (entry was evicted — elapsed + clean).
        // Budget must be restored.
        Assert.False(context.IsOverStoreBudget(), "budget must be restored after stale nodes are drained");
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static (TryCollectHandler, KeyValueContext, RaftManager) CreateHandler(KahunaConfiguration? config = null)
    {
        config ??= CreateConfiguration();

        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "reclamation-test",
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

    private static KahunaConfiguration CreateConfiguration(int dirtyObjectsWriterDelay = 1000)
    {
        KahunaConfiguration cfg = ConfigurationValidator.Validate(new()
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
            RevisionRetention = 16,
        });
        cfg.DirtyObjectsWriterDelay = dirtyObjectsWriterDelay;
        return cfg;
    }

    private static void InsertEntry(
        KeyValueContext context,
        string key,
        KeyValueState state,
        HLCTimestamp lastUsed,
        HLCTimestamp expires
    )
    {
        context.InsertStoreEntry(key, new KeyValueEntry
        {
            Value = state == KeyValueState.Set ? Encoding.UTF8.GetBytes("v") : null,
            State = state,
            LastUsed = lastUsed,
            LastModified = HLCTimestamp.Zero, // old LastModified → outside any dirty window
            Revision = 0,
            FlushedRevision = 0,
            Expires = expires
        });
    }
}

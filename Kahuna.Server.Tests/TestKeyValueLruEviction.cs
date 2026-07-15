
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
/// Tests for Phase B — intrusive O(1) LRU eviction replacing the approximate sampler.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestKeyValueLruEviction
{
    // ── B.1: LRU list maintenance ────────────────────────────────────────────────────────

    [Fact]
    public void InsertLinksTailward_RemoveUnlinks()
    {
        (_, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 5; i++)
            InsertEntry(context, $"k{i}", now);

        // List must have exactly 5 nodes; head and tail must be non-null.
        Assert.NotNull(context.LruHead);
        KeyValueEntry head = context.LruHead!;
        int count = 0;
        KeyValueEntry? cur = head;
        while (cur is not null) { count++; cur = cur.LruNext; }
        Assert.Equal(5, count);

        // Tail's LruNext is null; head's LruPrev is null.
        Assert.Null(head.LruPrev);
        KeyValueEntry? tail = head;
        while (tail!.LruNext is not null) tail = tail.LruNext;
        Assert.Null(tail.LruNext);

        // Remove middle entry and verify list integrity.
        string middleKey = "k2";
        context.RemoveStoreEntry(middleKey);

        count = 0;
        cur = context.LruHead;
        while (cur is not null) { count++; cur = cur.LruNext; }
        Assert.Equal(4, count);
    }

    /// <summary>
    /// InsertStoreEntry called with the same entry object that is already in the store (same-object
    /// re-insert) must not double-link the entry. Without the guard, Unlink is skipped but
    /// LinkAtTail still runs, leaving the entry with two "tail" pointers and corrupting the list.
    /// </summary>
    [Fact]
    public void InsertStoreEntry_SameObjectReInsert_DoesNotDoubleLink()
    {
        (_, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        InsertEntry(context, "a", now);
        InsertEntry(context, "b", now);
        InsertEntry(context, "c", now);

        // Grab the entry currently stored at "b" and re-insert the same object.
        KeyValueEntry bEntry = context.Store.Get("b")!;
        context.InsertStoreEntry("b", bEntry); // same-object re-insert: must be a no-op for the list

        // List must still have exactly 3 nodes with no cycles.
        int count = 0;
        KeyValueEntry? cur = context.LruHead;
        HashSet<KeyValueEntry> seen = [];
        while (cur is not null && seen.Add(cur)) { count++; cur = cur.LruNext; }
        Assert.Equal(3, count);

        // Tail's LruNext must be null; head's LruPrev must be null.
        Assert.Null(context.LruHead!.LruPrev);
        KeyValueEntry? tail = context.LruHead;
        while (tail!.LruNext is not null) tail = tail.LruNext;
        Assert.Null(tail.LruNext);
    }

    [Fact]
    public void TouchEntry_MovesToTail()
    {
        (_, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp t0 = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp t1 = new(t0.N, t0.L + 1, t0.C);

        for (int i = 0; i < 4; i++)
            InsertEntry(context, $"k{i}", t0);

        // k0 is at head (coldest after sequential inserts and no touches).
        Assert.Equal("k0", context.LruHead!.StoreKey);

        // Touch k0 — it should move to tail.
        KeyValueEntry k0entry = context.Store.Get("k0")!;
        context.TouchEntry(k0entry, t1);

        // Head is no longer k0.
        Assert.NotEqual("k0", context.LruHead!.StoreKey);

        // Tail is now k0.
        KeyValueEntry? tail = context.LruHead;
        while (tail!.LruNext is not null) tail = tail.LruNext;
        Assert.Equal("k0", tail.StoreKey);
    }

    // ── B.1 regression: TouchEntry on an unlinked (disk/scan) entry ─────────────────────

    /// <summary>
    /// TryGetByRangeHandler calls GetKeyValueEntry with populateCache:false for disk-resident
    /// keys during a scan.  The returned entry has StoreKey==null and is not in the LRU list.
    /// TouchEntry must update LastUsed without touching the list — calling Unlink on it would
    /// null both lruHead and lruTail (the "phantom takes over" corruption), silently disabling
    /// all future LRU eviction.
    /// </summary>
    [Fact]
    public void TouchEntry_OnUnlinkedDiskEntry_LeavesListIntact()
    {
        KahunaConfiguration config = CreateConfiguration(maxEntries: 3);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Build a 4-entry store (one over budget).
        for (int i = 0; i < 4; i++)
            InsertEntry(context, $"live/{i}", now);

        // Simulate a scan returning a disk-resident entry that is NOT in the store.
        // This is the populateCache:false path in TryGetByRangeHandler.
        KeyValueEntry diskEntry = new()
        {
            Value = [1],
            State = KeyValueState.Set,
            Revision = 1,
            FlushedRevision = 1,
            LastUsed = now,
            LastModified = now
            // StoreKey, LruPrev, LruNext are all null — entry is not linked
        };

        // Capture the real list head before the call.
        KeyValueEntry? headBefore = context.LruHead;
        Assert.NotNull(headBefore);

        // This must not corrupt the list.
        context.TouchEntry(diskEntry, now);

        // List head and tail must be unchanged.
        Assert.Same(headBefore, context.LruHead);

        // Walk the list — must still have exactly 4 reachable linked entries.
        int count = 0;
        KeyValueEntry? cur = context.LruHead;
        while (cur is not null) { count++; cur = cur.LruNext; }
        Assert.Equal(4, count);

        // LRU eviction must still work: 4 entries vs budget 3 → 1 evicted.
        handler.Execute();
        Assert.Equal(3, context.Store.Count);
    }

    // ── B.3: head-pop eviction correctness ──────────────────────────────────────────────

    /// <summary>
    /// Insert 10 entries. Touch the last 3 (making them hot). With MaxEntriesPerActor=7,
    /// exactly 3 entries must be evicted — and they must be the 3 coldest (first inserted).
    /// </summary>
    [Fact]
    public void LruEviction_EvictsColdest_PreservesHot()
    {
        KahunaConfiguration config = CreateConfiguration(maxEntries: 7);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp t0 = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp t1 = new(t0.N, t0.L + 1, t0.C);

        for (int i = 0; i < 10; i++)
            InsertEntry(context, $"key/{i:D2}", t0);

        // Touch key/07, key/08, key/09 (move to hot end).
        foreach (string k in new[] { "key/07", "key/08", "key/09" })
            context.TouchEntry(context.Store.Get(k)!, t1);

        // List order from head (coldest): key/00, key/01, key/02, key/03, key/04, key/05, key/06,
        // key/07, key/08, key/09 (hot end after touching).
        Assert.Equal(10, context.Store.Count);

        handler.Execute();

        Assert.Equal(7, context.Store.Count);

        // Cold entries evicted.
        for (int i = 0; i < 3; i++)
            Assert.False(context.Store.ContainsKey($"key/{i:D2}"), $"key/{i:D2} must be evicted");

        // Hot entries survive.
        for (int i = 3; i < 10; i++)
            Assert.True(context.Store.ContainsKey($"key/{i:D2}"), $"key/{i:D2} must survive");
    }

    /// <summary>
    /// An intent-held entry at the cold end of the list must be skipped, not evicted.
    /// The next eligible cold entry is evicted instead.
    /// </summary>
    [Fact]
    public void LruEviction_SkipsIntentHeld_EvictsNextEligible()
    {
        KahunaConfiguration config = CreateConfiguration(maxEntries: 4);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Insert 5 entries in order; head (coldest) = k0.
        for (int i = 0; i < 5; i++)
            InsertEntry(context, $"k{i}", now);

        // Give k0 a live write intent — it must be skipped.
        context.Store.Get("k0")!.WriteIntent = new KeyValueWriteIntent
        {
            TransactionId = now,
            Expires = new HLCTimestamp(now.N, now.L + TimeSpan.FromMinutes(1).Ticks, now.C)
        };

        handler.Execute();

        Assert.Equal(4, context.Store.Count);
        Assert.True(context.Store.ContainsKey("k0"), "intent-held k0 must not be evicted");
        Assert.False(context.Store.ContainsKey("k1"), "next cold entry k1 must be evicted instead");
    }

    /// <summary>
    /// A dirty (unflushed) entry at the cold end must be skipped; the next eligible entry is evicted.
    /// </summary>
    [Fact]
    public void LruEviction_SkipsDirty_EvictsNextEligible()
    {
        KahunaConfiguration config = CreateConfiguration(maxEntries: 4, dirtyWindowMs: 10_000);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 5; i++)
            InsertEntry(context, $"k{i}", now);

        // Make k0 dirty.
        KeyValueEntry k0 = context.Store.Get("k0")!;
        k0.Revision = 1;
        k0.FlushedRevision = -1;
        k0.LastModified = now; // recently modified → inside safety window

        handler.Execute();

        Assert.Equal(4, context.Store.Count);
        Assert.True(context.Store.ContainsKey("k0"), "dirty k0 must not be evicted");
        Assert.False(context.Store.ContainsKey("k1"), "next cold entry k1 must be evicted instead");
    }

    /// <summary>
    /// Verify that the LRU list position (not LastUsed timestamp) drives eviction order.
    /// An entry inserted early but touched recently must be considered hotter than a later-inserted
    /// entry that was never touched.
    /// </summary>
    [Fact]
    public void LruEviction_ListPositionDrivesOrder_NotLastUsedTimestamp()
    {
        // 3 entries, budget=2 → 1 eviction needed.
        KahunaConfiguration config = CreateConfiguration(maxEntries: 2);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp t0 = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp t1 = new(t0.N, t0.L + 1, t0.C);

        InsertEntry(context, "early", t0);   // inserted first → head (coldest)
        InsertEntry(context, "middle", t0);
        InsertEntry(context, "late", t0);    // inserted last → tail

        // Touch "early" — moves it to tail (hottest).
        // List order from head: middle, late, early
        context.TouchEntry(context.Store.Get("early")!, t1);

        // "middle" is now the coldest → must be the one evicted.
        handler.Execute();

        Assert.Equal(2, context.Store.Count);
        Assert.False(context.Store.ContainsKey("middle"), "middle must be evicted (coldest)");
        Assert.True(context.Store.ContainsKey("early"), "early must survive (recently touched)");
        Assert.True(context.Store.ContainsKey("late"), "late must survive");
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
                NodeName = "lru-test",
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

    private static KahunaConfiguration CreateConfiguration(
        int maxEntries = 50_000,
        long dirtyWindowMs = 1_000,
        TimeSpan? cacheEntryTtl = null,
        int collectBatchMax = 1000
    )
    {
        KahunaConfiguration cfg = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = cacheEntryTtl ?? TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = maxEntries,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = collectBatchMax,
            RevisionRetention = 16
        });
        cfg.DirtyObjectsWriterDelay = (int)dirtyWindowMs;
        return cfg;
    }

    private static void InsertEntry(KeyValueContext context, string key, HLCTimestamp lastUsed)
    {
        context.InsertStoreEntry(key, new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = lastUsed,
            LastModified = HLCTimestamp.Zero, // old → outside dirty window
            Revision = 0,
            FlushedRevision = 0
        });
    }

    // ── B.idle: CacheEntryTtl idle eviction ─────────────────────────────────────────────

    /// <summary>
    /// An entry whose LastUsed is far older than CacheEntryTtl must be evicted even when the
    /// actor is well below its count and byte budgets — TTL eviction is budget-independent.
    /// </summary>
    [Fact]
    public void IdleEntry_EvictedWhenAgeExceedsTtl_UnderBudget()
    {
        // TTL = 1 second; the idle entry has LastUsed = HLCTimestamp.Zero (epoch), so its age
        // in milliseconds equals currentTime.L, which is current wall-clock ms — orders of
        // magnitude above 1 s. The actor is well under the 50 000-entry budget.
        KahunaConfiguration config = CreateConfiguration(cacheEntryTtl: TimeSpan.FromSeconds(1));
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        InsertEntry(context, "idle-key", HLCTimestamp.Zero); // last used at epoch → always stale
        InsertEntry(context, "fresh-key", now);              // last used now → within TTL

        handler.Execute();

        Assert.False(context.Store.ContainsKey("idle-key"), "idle entry must be evicted by TTL sweep");
        Assert.True(context.Store.ContainsKey("fresh-key"), "fresh entry must survive TTL sweep");
    }

    /// <summary>
    /// A dirty idle entry (unflushed revision) must not be evicted by the TTL sweep — the
    /// dirty-eviction guard applies regardless of idle age, matching the budget-LRU behavior.
    /// </summary>
    [Fact]
    public void DirtyIdleEntry_NotEvictedByIdleSweep_EvenWhenStale()
    {
        KahunaConfiguration config = CreateConfiguration(cacheEntryTtl: TimeSpan.FromSeconds(1));
        (TryCollectHandler handler, KeyValueContext context, _) = CreateHandler(config);

        context.InsertStoreEntry("dirty-idle", new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = HLCTimestamp.Zero,    // stale
            LastModified = HLCTimestamp.Zero,
            Revision = 5,
            FlushedRevision = 3              // dirty: Revision > FlushedRevision
        });

        handler.Execute();

        Assert.True(context.Store.ContainsKey("dirty-idle"), "dirty idle entry must not be evicted");
    }

    /// <summary>
    /// When CacheEntryTtl is zero (disabled), no idle eviction occurs — entries are retained
    /// even if their LastUsed is older than the typical TTL window.
    /// </summary>
    [Fact]
    public void IdleEviction_Disabled_WhenCacheEntryTtlIsZero()
    {
        // TimeSpan.Zero disables idle eviction; the budget is huge so budget-LRU doesn't run either.
        KahunaConfiguration config = CreateConfiguration(cacheEntryTtl: TimeSpan.Zero);
        (TryCollectHandler handler, KeyValueContext context, _) = CreateHandler(config);

        context.InsertStoreEntry("old-key", new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = HLCTimestamp.Zero,    // ancient
            LastModified = HLCTimestamp.Zero,
            Revision = 0,
            FlushedRevision = 0
        });

        handler.Execute();

        Assert.True(context.Store.ContainsKey("old-key"), "entry must not be evicted when TTL is disabled");
        Assert.Equal(0, handler.LastCycleStats.IdleEvicted);
    }

    /// <summary>
    /// An idle entry that holds a live write intent must not be idle-evicted — the intent guard
    /// applies regardless of age, mirroring the budget-LRU walk. Evicting it would drop the
    /// in-memory prepare state of an in-flight transaction.
    /// </summary>
    [Fact]
    public void IdleEntry_WithLiveWriteIntent_NotEvictedByIdleSweep()
    {
        KahunaConfiguration config = CreateConfiguration(cacheEntryTtl: TimeSpan.FromSeconds(1));
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);

        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        context.InsertStoreEntry("intent-idle", new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = HLCTimestamp.Zero,        // stale
            LastModified = HLCTimestamp.Zero,
            Revision = 0,
            FlushedRevision = 0,
            WriteIntent = new KeyValueWriteIntent { TransactionId = now, Expires = new(now.N, now.L + 60_000, 0) }
        });

        handler.Execute();

        Assert.True(context.Store.ContainsKey("intent-idle"), "idle entry with a live write intent must not be evicted");
        Assert.Equal(0, handler.LastCycleStats.IdleEvicted);
    }

    /// <summary>
    /// The idle sweep is bounded per turn: with a small inspection budget it evicts at most that
    /// many stale entries per cycle and resumes on the next cycle via its cursor, so a large store
    /// is never swept in a single mailbox turn. Successive cycles eventually evict every idle entry.
    /// </summary>
    [Fact]
    public void IdleSweep_IsBoundedPerTurn_AndResumesUntilComplete()
    {
        KahunaConfiguration config = CreateConfiguration(cacheEntryTtl: TimeSpan.FromSeconds(1), collectBatchMax: 2);
        (TryCollectHandler handler, KeyValueContext context, _) = CreateHandler(config);

        const int total = 5;
        for (int i = 0; i < total; i++)
            InsertEntry(context, $"idle/{i}", HLCTimestamp.Zero); // all stale (epoch LastUsed)

        // One bounded cycle inspects at most the budget (2), so it cannot evict all five.
        handler.Execute();
        Assert.Equal(2, handler.LastCycleStats.IdleEvicted);
        Assert.Equal(total - 2, context.Store.Count);

        // Successive cycles resume from the cursor and complete the sweep.
        for (int i = 0; i < total && context.Store.Count > 0; i++)
            handler.Execute();

        Assert.Equal(0, context.Store.Count);
    }
}

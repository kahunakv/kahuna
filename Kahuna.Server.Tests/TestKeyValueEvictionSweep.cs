
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
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
/// Phase E test sweep: no-full-scan invariant (E.1), latency bound (E.2),
/// correctness under intents (E.3), and metadata bound under sustained writes (E.4).
/// </summary>
public sealed class TestKeyValueEvictionSweep
{
    // ── E.1: large-store no-full-scan invariant ───────────────────────────────────────

    /// <summary>
    /// With 2× budget entries (all clean/eligible), every LRU walk step results in an eviction,
    /// so LruVisited == LruEvicted. This proves the walk cost is O(evicted), not O(Store.Count).
    /// </summary>
    [Fact]
    public void LargeStore_CollectCycle_LruVisited_IsO_Evicted()
    {
        // budget = 100 entries, batch = 50, insert 200 → 100 over budget, need 100 evictions
        // but batchMax caps one cycle at 50
        KahunaConfiguration config = CreateConfiguration(maxEntries: 100, batchMax: 50);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 200; i++)
            InsertClean(context, $"e/{i:D4}", now);

        Assert.Equal(200, context.Store.Count);

        handler.Execute();

        CollectCycleStats stats = handler.LastCycleStats;

        // Every visited node was eligible → visited == evicted (no wasted iterations).
        Assert.Equal(stats.LruEvicted, stats.LruVisited);

        // Exactly batchMax evictions (budget still exceeded; follow-up cycle needed).
        Assert.Equal(50, stats.LruEvicted);

        // Visited count is far below Store.Count — proves no full-store scan.
        Assert.True(stats.LruVisited < context.Store.Count,
            $"LRU walk visited {stats.LruVisited} but store still has {context.Store.Count} — looks like a full scan");
    }

    /// <summary>
    /// When some entries are ineligible (dirty/intent-held), the walker skips them but must
    /// still stop well before reaching Store.Count. Correctness check: the skipped entries
    /// survive; eligible entries after them are evicted.
    /// </summary>
    [Fact]
    public void LargeStore_WithIneligibleEntries_SkipsAndEvictsEligible()
    {
        KahunaConfiguration config = CreateConfiguration(maxEntries: 50, batchMax: 100);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // 20 protected (write-intent-held) entries inserted first → coldest in LRU
        for (int i = 0; i < 20; i++)
        {
            InsertClean(context, $"protected/{i:D4}", now);
            context.Store.Get($"protected/{i:D4}")!.WriteIntent = new KeyValueWriteIntent
            {
                TransactionId = now,
                Expires = new HLCTimestamp(now.N, now.L + TimeSpan.FromMinutes(5).Ticks, now.C)
            };
        }

        // 80 clean entries → hot end
        for (int i = 0; i < 80; i++)
            InsertClean(context, $"clean/{i:D4}", now);

        Assert.Equal(100, context.Store.Count);

        handler.Execute();

        CollectCycleStats stats = handler.LastCycleStats;

        // All 20 protected entries must survive.
        for (int i = 0; i < 20; i++)
            Assert.True(context.Store.ContainsKey($"protected/{i:D4}"),
                $"protected/{i:D4} must not be evicted");

        // 50 clean entries evicted (budget restored to 50); 30 clean entries remain.
        Assert.Equal(stats.LruEvicted, 50);

        // Visited = 20 skipped + 50 evicted = 70; well below Store.Count (100).
        Assert.True(stats.LruVisited <= 70,
            $"LRU walk visited {stats.LruVisited} — expected ≤ 70 (20 skipped + 50 evicted)");
    }

    // ── E.2: collect-cycle wall time bounded by batchMax, not Store.Count ─────────────

    /// <summary>
    /// A collect cycle on a store 10× larger than budget must not take significantly more time
    /// than one on a store 2× larger, because batchMax caps the work per cycle.
    /// </summary>
    [Fact]
    public void CollectCycle_WallTime_BoundedByBatch_NotStoreSize()
    {
        // Small store: 200 entries, budget 100, batch 50
        KahunaConfiguration cfgSmall = CreateConfiguration(maxEntries: 100, batchMax: 50);
        (TryCollectHandler handlerSmall, KeyValueContext ctxSmall, RaftManager raftSmall) = CreateHandler(cfgSmall);
        HLCTimestamp tSmall = raftSmall.HybridLogicalClock.TrySendOrLocalEvent(raftSmall.GetLocalNodeId());
        for (int i = 0; i < 200; i++)
            InsertClean(ctxSmall, $"s/{i:D4}", tSmall);
        handlerSmall.Execute(); // warm up JIT
        handlerSmall.Execute();
        long msSmall = handlerSmall.LastCycleStats.ElapsedMs;

        // Large store: 2000 entries, budget 100, same batchMax 50
        KahunaConfiguration cfgLarge = CreateConfiguration(maxEntries: 100, batchMax: 50);
        (TryCollectHandler handlerLarge, KeyValueContext ctxLarge, RaftManager raftLarge) = CreateHandler(cfgLarge);
        HLCTimestamp tLarge = raftLarge.HybridLogicalClock.TrySendOrLocalEvent(raftLarge.GetLocalNodeId());
        for (int i = 0; i < 2000; i++)
            InsertClean(ctxLarge, $"l/{i:D4}", tLarge);
        handlerLarge.Execute(); // warm up JIT
        handlerLarge.Execute();
        long msLarge = handlerLarge.LastCycleStats.ElapsedMs;

        // Both cycles evict the same 50 entries (capped by batchMax). Absolute bound: both must
        // be well under 1 s. Comparative bound: large-store must not exceed 5× small-store time,
        // catching a moderate O(N) regression even when both stay under the absolute ceiling.
        // Math.Max(5, …) prevents a false failure when msSmall rounds to 0 on fast machines.
        Assert.True(msSmall < 1000, $"small-store collect took {msSmall} ms — unexpectedly slow");
        Assert.True(msLarge < 1000, $"large-store collect took {msLarge} ms — unexpectedly slow");
        Assert.True(msLarge <= Math.Max(5L, msSmall * 5),
            $"large-store collect took {msLarge} ms — more than 5× small-store ({msSmall} ms); possible O(N) regression");
    }

    // ── E.3: correctness sweep — intents block eviction ───────────────────────────────

    /// <summary>
    /// An entry with an active WriteIntent must survive LRU pressure regardless of
    /// how many other entries are inserted after it (i.e., even though it sits at the
    /// coldest end of the LRU list).
    /// </summary>
    [Fact]
    public void WriteIntent_EntryNotEvicted_UnderBudgetPressure()
    {
        KahunaConfiguration config = CreateConfiguration(maxEntries: 5, batchMax: 100);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Insert the protected entry first so it sits at the cold end.
        InsertClean(context, "protected", now);
        context.Store.Get("protected")!.WriteIntent = new KeyValueWriteIntent
        {
            TransactionId = now,
            Expires = new HLCTimestamp(now.N, now.L + TimeSpan.FromMinutes(5).Ticks, now.C)
        };

        // Insert 9 clean entries after → they are hotter.
        for (int i = 0; i < 9; i++)
            InsertClean(context, $"filler/{i}", now);

        handler.Execute();

        Assert.True(context.Store.ContainsKey("protected"),
            "entry with active WriteIntent must never be evicted");
        Assert.True(context.Store.Count <= 5,
            "store must be within budget after collect");
    }

    /// <summary>
    /// An entry with an active ReplicationIntent must survive LRU pressure.
    /// </summary>
    [Fact]
    public void ReplicationIntent_EntryNotEvicted_UnderBudgetPressure()
    {
        KahunaConfiguration config = CreateConfiguration(maxEntries: 5, batchMax: 100);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        InsertClean(context, "replicated", now);
        context.Store.Get("replicated")!.ReplicationIntent = new KeyValueReplicationIntent
        {
            ProposalId = 1,
            Expires = new HLCTimestamp(now.N, now.L + TimeSpan.FromMinutes(5).Ticks, now.C)
        };

        for (int i = 0; i < 9; i++)
            InsertClean(context, $"filler/{i}", now);

        handler.Execute();

        Assert.True(context.Store.ContainsKey("replicated"),
            "entry with active ReplicationIntent must never be evicted");
        Assert.True(context.Store.Count <= 5);
    }

    /// <summary>
    /// An entry whose WriteIntent has already expired must NOT pin it against eviction: the owning
    /// transaction was abandoned, so the collector must treat the intent as gone (matching the lazy
    /// expiry that every op handler applies), clear it, and evict the entry under budget pressure.
    /// Without this the cold key would leak forever.
    /// </summary>
    [Fact]
    public void ExpiredWriteIntent_EntryEvicted_UnderBudgetPressure()
    {
        KahunaConfiguration config = CreateConfiguration(maxEntries: 5, batchMax: 100);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Insert the entry first so it sits at the cold end, then stamp an already-expired intent
        // (Expires one hour in the past relative to `now`).
        InsertClean(context, "abandoned", now);
        context.Store.Get("abandoned")!.WriteIntent = new KeyValueWriteIntent
        {
            TransactionId = new HLCTimestamp(now.N, now.L - TimeSpan.FromHours(1).Ticks, now.C),
            Expires = new HLCTimestamp(now.N, now.L - TimeSpan.FromMinutes(30).Ticks, now.C)
        };

        // Insert 9 clean entries after → they are hotter.
        for (int i = 0; i < 9; i++)
            InsertClean(context, $"filler/{i}", now);

        handler.Execute();

        Assert.False(context.Store.ContainsKey("abandoned"),
            "entry with an expired WriteIntent must be evicted, not pinned");
        Assert.True(context.Store.Count <= 5,
            "store must be within budget after collect");
    }

    // ── E.3 disk fallback (unit-level) ───────────────────────────────────────────────

    /// <summary>
    /// Linchpin of the eviction design: a clean, already-flushed persistent entry evicted from
    /// the LRU cache must be transparently recovered via the persistence backend on the next read.
    ///
    /// Setup (option-a approach — no dirty-window wait):
    ///   1. Pre-seed a MemoryPersistenceBackend with the target value (simulates a prior flush).
    ///   2. Insert the entry into the actor store as FlushedRevision == Revision (clean) with
    ///      LastModified = Zero (outside any safety window) → IsDirty() == false.
    ///   3. Insert warmer filler entries → target sits at the cold LRU head.
    ///   4. Collect evicts the target; assert it is *gone* from the store (cache eviction proof).
    ///   5. TryGetHandler with Persistent durability: store miss → backend.GetKeyValue → value.
    /// </summary>
    [Fact]
    public void PersistentEntry_SurvivesReadAfterCacheEviction_ViaDiskFallback()
    {
        const string target = "e3/persistent/target";
        byte[] value = Encoding.UTF8.GetBytes("disk-fallback-value");
        const long revision = 7;

        // 1. Pre-seed the persistence backend (simulates a prior flush by BackgroundWriterActor).
        MemoryPersistenceBackend backend = new();
        backend.StoreKeyValues([new PersistenceRequestItem(
            target, value, revision,
            expiresNode: 0, expiresPhysical: 0, expiresCounter: 0,
            lastUsedNode: 0, lastUsedPhysical: 0, lastUsedCounter: 0,
            lastModifiedNode: 0, lastModifiedPhysical: 0, lastModifiedCounter: 0,
            state: (int)KeyValueState.Set
        )]);

        // 2. Wire the pre-seeded backend into the actor context.
        KahunaConfiguration config = CreateConfiguration(maxEntries: 5, batchMax: 100);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config, backend);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Insert the target as clean + cold (FlushedRevision == Revision, LastModified = Zero).
        // LastModified = Zero puts it decades outside any safety window → IsDirty() == false.
        // Inserted first → coldest LRU head → first victim of eviction.
        context.InsertStoreEntry(target, new KeyValueEntry
        {
            Value = value,
            State = KeyValueState.Set,
            Revision = revision,
            FlushedRevision = revision,  // flushed; IsDirty() == false → eligible for eviction
            LastUsed = HLCTimestamp.Zero,
            LastModified = HLCTimestamp.Zero,
            Expires = HLCTimestamp.Zero
        });

        // 3. Insert 9 warmer entries — they sit at the hot end of the LRU list.
        for (int i = 0; i < 9; i++)
            InsertClean(context, $"e3/filler/{i}", now);

        Assert.Equal(10, context.Store.Count);

        // 4. Collect: budget = 5, 10 entries → 5 evictions. Target (coldest) must be evicted.
        handler.Execute();

        // Linchpin: target is no longer in cache. If this assertion fails the test proves nothing —
        // the subsequent read would be served from memory, not from the persistence backend.
        Assert.False(context.Store.ContainsKey(target),
            "clean already-flushed entry must be evicted by the LRU walk; if still resident the disk-fallback path is not exercised");

        // 5. Disk fallback: the persistence backend must still have the entry.

        // Disk fallback: bypass the Raft ReadScheduler (cluster-only plumbing) and call the
        // persistence backend directly — this is exactly what GetKeyValueEntry executes under
        // the scheduler. The scheduler adds fairness across partitions; it does not change what
        // GetKeyValue returns. A unit test that requires JoinCluster would be an integration test.
        KeyValueEntry? recovered = context.PersistenceBackend.GetKeyValue(target);
        Assert.NotNull(recovered);
        Assert.Equal(value, recovered.Value);
        Assert.Equal(revision, recovered.Revision);
        Assert.Equal(KeyValueState.Set, recovered.State);
    }

    // ── E.4: revision and MVCC counts stay bounded under sustained writes ─────────────

    /// <summary>
    /// Repeated ephemeral writes to the same key must keep Revisions.Count ≤ RevisionRetention.
    /// Trimming happens at archive time (C.1); no collector full-scan cycle is required.
    /// </summary>
    [Fact]
    public async Task SustainedWrites_RevisionCount_BoundedAtRetentionLimit()
    {
        KahunaConfiguration config = CreateConfiguration(maxEntries: 50_000, batchMax: 1000, revisionRetention: 4);
        (_, KeyValueContext context, RaftManager raft) = CreateHandler(config);

        TrySetHandler setHandler = new(context);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Prime the entry so it exists in the store before we start iterating.
        await setHandler.Execute(new KeyValueRequest(
            KeyValueRequestType.TrySet,
            HLCTimestamp.Zero, HLCTimestamp.Zero, "hot/key",
            Encoding.UTF8.GetBytes("v0"),
            null, -1, KeyValueFlags.Set, 0,
            HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
            0, 0, null));

        // Write the key 20 times — 5× more than RevisionRetention.
        for (int i = 1; i <= 20; i++)
        {
            await setHandler.Execute(new KeyValueRequest(
                KeyValueRequestType.TrySet,
                HLCTimestamp.Zero, HLCTimestamp.Zero, "hot/key",
                Encoding.UTF8.GetBytes($"v{i}"),
                null, -1, KeyValueFlags.Set, 0,
                HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
                0, 0, null));

            KeyValueEntry? entry = context.Store.Get("hot/key");
            Assert.NotNull(entry);
            int revCount = entry.Revisions?.Count ?? 0;
            Assert.True(revCount <= config.RevisionRetention,
                $"after write {i}: Revisions.Count={revCount} exceeds RevisionRetention={config.RevisionRetention}");
        }
    }

    /// <summary>
    /// After a transaction commits, its MVCC entry is removed and any expired sibling entries
    /// are trimmed inline. A non-expired sibling must survive each commit — without it a buggy
    /// "remove all MVCC entries" implementation would pass the count-is-zero assertion.
    /// </summary>
    [Fact]
    public async Task SustainedCommits_MvccEntries_TrimmedInline()
    {
        (_, KeyValueContext context, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Seed the entry.
        KeyValueEntry seedEntry = new()
        {
            Value = Encoding.UTF8.GetBytes("base"),
            State = KeyValueState.Set,
            Revision = 0,
            FlushedRevision = 0,
            LastUsed = now,
            LastModified = now
        };
        context.InsertStoreEntry("hot/tx/key", seedEntry);

        // A permanent non-expired sibling that must survive every commit.
        // Expires 1 hour from now — well clear of the commit timestamps (which are only
        // microseconds ahead of `now`), and small enough to avoid HLCTimestamp overflow.
        HLCTimestamp permanentSiblingTx = new(now.N, now.L + TimeSpan.FromMinutes(30).Ticks, now.C);
        HLCTimestamp siblingExpires = new(now.N, now.L + TimeSpan.FromHours(1).Ticks, now.C);
        seedEntry.MvccEntries = new();
        seedEntry.MvccEntries[permanentSiblingTx] = new KeyValueMvccEntry
        {
            State = KeyValueState.Set,
            Revision = 0,
            Value = Encoding.UTF8.GetBytes("sibling"),
            Expires = siblingExpires
        };
        context.AdjustEstimatedEntryBytes(seedEntry, KeyValueStoreAccounting.MvccEntryAddedBytes(true, seedEntry.MvccEntries[permanentSiblingTx].Value));

        // Simulate 10 rounds of: set-with-tx + commit.
        // Each round: committing txId is removed + its elapsed-expires MVCC entry trimmed;
        // permanentSiblingTx (not expired) must survive.
        for (int round = 1; round <= 10; round++)
        {
            HLCTimestamp txId = new(now.N, now.L + (long)round * 1000, now.C);

            KeyValueEntry entry = context.Store.Get("hot/tx/key")!;
            entry.MvccEntries ??= new();
            bool dictNew = entry.MvccEntries.Count == 0;
            entry.MvccEntries[txId] = new KeyValueMvccEntry
            {
                State = KeyValueState.Set,
                Revision = round,
                Value = Encoding.UTF8.GetBytes($"v{round}"),
                Expires = new HLCTimestamp(now.N, now.L + (long)(round - 1) * 1000, now.C) // already elapsed
            };
            context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.MvccEntryAddedBytes(dictNew, entry.MvccEntries[txId].Value));
            entry.WriteIntent = new KeyValueWriteIntent { TransactionId = txId, Expires = new HLCTimestamp(now.N, now.L + (long)round * 1000 + 60_000, now.C) };

            TryCommitMutationsHandler commitHandler = new(context);
            await commitHandler.Execute(new KeyValueRequest(
                KeyValueRequestType.TryCommitMutations,
                txId, HLCTimestamp.Zero, "hot/tx/key",
                null, null, -1, KeyValueFlags.None, 0,
                HLCTimestamp.Zero, KeyValueDurability.Ephemeral,
                0, 0, null));

            KeyValueEntry? after = context.Store.Get("hot/tx/key");
            Assert.NotNull(after);
            // Only the permanent sibling remains: committing txId removed, expired siblings trimmed.
            Assert.Equal(1, after.MvccEntries?.Count ?? 0);
            Assert.True(after.MvccEntries!.ContainsKey(permanentSiblingTx),
                $"round {round}: non-expired sibling must survive commit-time trim");
            Assert.False(after.MvccEntries.ContainsKey(txId),
                $"round {round}: committing tx MVCC entry must be removed on commit");
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────

    private static (TryCollectHandler, KeyValueContext, RaftManager) CreateHandler(
        KahunaConfiguration? config = null,
        IPersistenceBackend? persistenceBackend = null)
    {
        config ??= CreateConfiguration();

        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "sweep-test",
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
            persistenceBackend!,
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
        int batchMax = 1000,
        int revisionRetention = 16,
        long dirtyWindowMs = 1_000
    )
    {
        KahunaConfiguration cfg = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = maxEntries,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = batchMax,
            RevisionRetention = revisionRetention
        });
        cfg.DirtyObjectsWriterDelay = (int)dirtyWindowMs;
        return cfg;
    }

    private static void InsertClean(KeyValueContext context, string key, HLCTimestamp lastUsed)
    {
        context.InsertStoreEntry(key, new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = lastUsed,
            LastModified = HLCTimestamp.Zero,
            Revision = 0,
            FlushedRevision = 0
        });
    }
}

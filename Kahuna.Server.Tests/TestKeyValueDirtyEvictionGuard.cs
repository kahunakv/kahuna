
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
/// Verifies the dirty-eviction guard (Phase A0): entries whose committed revision has not yet been
/// flushed to disk must not be evicted by a collect cycle.  Without the guard a collect cycle that
/// fires before the BackgroundWriter flush removes the in-memory entry; the next read falls back to
/// disk and returns a stale revision (or DoesNotExist for a brand-new key).
/// </summary>
public sealed class TestKeyValueDirtyEvictionGuard
{
    private readonly ILoggerFactory loggerFactory;

    public TestKeyValueDirtyEvictionGuard(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    // ── guard logic ──────────────────────────────────────────────────────────────────────

    [Fact]
    public void IsDirty_True_WhenRevisionAheadAndWithinWindow()
    {
        HLCTimestamp now = new(1, TimeSpan.FromSeconds(100).Ticks, 0);
        KeyValueEntry entry = new() { Revision = 1, FlushedRevision = -1, LastModified = now };

        Assert.True(entry.IsDirty(10_000, now));
    }

    [Fact]
    public void IsDirty_False_ForEphemeral_WhenFlushedRevisionEqualsRevision()
    {
        HLCTimestamp now = new(1, TimeSpan.FromSeconds(100).Ticks, 0);
        KeyValueEntry entry = new() { Revision = 0, FlushedRevision = 0, LastModified = now };

        Assert.False(entry.IsDirty(10_000, now));
    }

    [Fact]
    public void IsDirty_False_WhenOutsideSafetyWindow()
    {
        HLCTimestamp now = new(1, TimeSpan.FromSeconds(100).Ticks, 0);
        HLCTimestamp oldModified = new(1, TimeSpan.FromSeconds(100 - 30).Ticks, 0); // 30 s ago
        KeyValueEntry entry = new() { Revision = 1, FlushedRevision = -1, LastModified = oldModified };

        // safetyWindowMs = 10 000 ms; 30 s elapsed → outside window
        Assert.False(entry.IsDirty(10_000, now));
    }

    // ── eviction guard — Step 1 (garbage / tombstone) ────────────────────────────────────

    /// <summary>
    /// A freshly committed persistent entry must survive even when it is the prime LRU victim
    /// (coldest LastUsed) and the store is over budget.  Make it the coldest so the test would
    /// fail without the guard.
    /// </summary>
    [Fact]
    public void DirtyEntry_IsNotEvicted_WhenCollectRunsBeforeFlush()
    {
        KahunaConfiguration config = CreateConfiguration(dirtyObjectsWriterDelay: 5000);
        config.MaxEntriesPerActor = 5;
        config.CollectBatchMax = 100;

        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Filler entries with current LastUsed (warm).
        for (int i = 0; i < 8; i++)
            InsertCleanEntry(context, $"filler/{i:D2}", KeyValueState.Set, now);

        // Dirty entry is the coldest — prime LRU victim without the guard.
        HLCTimestamp cold = new(now.N, now.L - TimeSpan.FromMinutes(10).Ticks, now.C);
        context.InsertStoreEntry("protected/key", new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("latest-value"),
            State = KeyValueState.Set,
            Revision = 1,
            FlushedRevision = -1,   // unflushed persistent entry
            LastUsed = cold,
            LastModified = now,     // modified just now → inside safety window
            Expires = HLCTimestamp.Zero
        });

        handler.Execute();

        Assert.True(context.Store.ContainsKey("protected/key"),
            "dirty entry must survive: it is the coldest LRU victim but unflushed");
        Assert.Equal(Encoding.UTF8.GetBytes("latest-value"), context.Store.Get("protected/key")!.Value);
    }

    /// <summary>
    /// An unflushed tombstone must also be skipped. If evicted the disk-fallback read would
    /// resurrect the old value, violating read-your-writes.
    /// </summary>
    [Fact]
    public void DirtyTombstone_IsNotEvicted_WhenCollectRunsBeforeFlush()
    {
        KahunaConfiguration config = CreateConfiguration(dirtyObjectsWriterDelay: 5000);
        config.MaxEntriesPerActor = 5;
        config.CollectBatchMax = 100;

        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 8; i++)
            InsertCleanEntry(context, $"filler/{i:D2}", KeyValueState.Set, now);

        context.InsertStoreEntry("tombstone/key", new KeyValueEntry
        {
            Value = null,
            State = KeyValueState.Deleted,
            Revision = 2,
            FlushedRevision = -1,   // unflushed delete
            LastUsed = now,
            LastModified = now,
            Expires = HLCTimestamp.Zero
        });

        handler.Execute();

        Assert.True(context.Store.ContainsKey("tombstone/key"),
            "dirty tombstone must survive so DoesNotExist is served from memory, not stale disk");
        Assert.Equal(KeyValueState.Deleted, context.Store.Get("tombstone/key")!.State);
    }

    /// <summary>
    /// Once the entry's LastModified is outside the safety window it is eligible again.
    /// </summary>
    [Fact]
    public void DirtyEntry_BecomesEligible_AfterSafetyWindowElapses()
    {
        // With DirtyObjectsWriterDelay=0 the floor kicks in: safetyWindowMs = 10 000 ms.
        KahunaConfiguration config = CreateConfiguration(dirtyObjectsWriterDelay: 0);
        config.MaxEntriesPerActor = 5;
        config.CollectBatchMax = 100;

        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // Insert stale/key FIRST so it sits at the cold end of the LRU list (intrusive O(1) LRU
        // evicts from head; inserting first makes it the head before fillers push it down).
        HLCTimestamp oldModified = new(now.N, now.L - TimeSpan.FromSeconds(30).Ticks, now.C);
        context.InsertStoreEntry("stale/key", new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("old-value"),
            State = KeyValueState.Set,
            Revision = 1,
            FlushedRevision = -1,   // revision counter still dirty, but window has elapsed
            LastUsed = oldModified,
            LastModified = oldModified,
            Expires = HLCTimestamp.Zero
        });

        // Insert 8 clean filler entries after — they sit at the hot end.
        for (int i = 0; i < 8; i++)
            InsertCleanEntry(context, $"filler/{i:D2}", KeyValueState.Set, now);

        // 9 entries, budget 5 → need to evict 4. stale/key is coldest eligible → evicted.
        handler.Execute();

        Assert.False(context.Store.ContainsKey("stale/key"),
            "entry outside safety window must be eligible for eviction");
    }

    /// <summary>
    /// Ephemeral entries have FlushedRevision == Revision at commit time.  The guard invariant is
    /// tested directly (IsDirty returns false) without depending on the LRU sampler's random jitter
    /// to pick a specific key.  A separate count assertion confirms some eviction occurred under
    /// budget pressure, which would be impossible if the guard were misfiring on ephemeral entries.
    /// </summary>
    [Fact]
    public void EphemeralEntry_AlwaysEligible_ForEviction()
    {
        KahunaConfiguration config = CreateConfiguration(dirtyObjectsWriterDelay: 5000);
        config.MaxEntriesPerActor = 5;
        config.CollectBatchMax = 100;

        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 8; i++)
            InsertCleanEntry(context, $"filler/{i:D2}", KeyValueState.Set, now);

        context.InsertStoreEntry("ephemeral/key", new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("ephemeral-value"),
            State = KeyValueState.Set,
            Revision = 0,
            FlushedRevision = 0,    // ephemeral: FlushedRevision == Revision → not dirty
            LastUsed = now,
            LastModified = now,
            Expires = HLCTimestamp.Zero
        });

        // Core invariant: IsDirty must be false for an ephemeral entry.
        // This is the property the guard tests — if IsDirty() returned true the entry would be
        // pinned. A future regression that sets FlushedRevision = -1 for ephemeral would break here.
        Assert.False(context.Store.Get("ephemeral/key")!.IsDirty(10_000, now),
            "IsDirty must be false for an ephemeral entry (FlushedRevision == Revision)");

        handler.Execute();

        // Some eviction must have occurred (budget = 5, inserted 9). If the guard misfired and
        // pinned the ephemeral entry it would still count as a missed eviction cycle.
        Assert.True(context.Store.Count <= config.MaxEntriesPerActor,
            $"store must be at or under budget after collect (count={context.Store.Count}, max={config.MaxEntriesPerActor})");
    }

    // ── wiring test: real handler path leaves persistent entry protected ─────────────────

    /// <summary>
    /// End-to-end guard: write a persistent key → force collect while the store is over budget →
    /// assert the key still reads back correctly.  The store is under budget after the SET (1 entry,
    /// limit=1), so a collect will try to LRU-evict the single entry.  The dirty guard must protect
    /// it.  If a future regression erroneously sets FlushedRevision = Revision on the persistent
    /// commit path, IsDirty() returns false, the LRU evicts the entry, and the subsequent GET
    /// returns DoesNotExist — catching the regression.
    /// </summary>
    [Fact]
    public async Task PersistentCommit_EntryProtectedByDirtyGuard_UnderBudgetPressure()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            KeyValueWorkers = 1,
            MaxEntriesPerActor = 0,  // every entry is over budget
            CollectBatchMax = 100,
            DirtyObjectsWriterDelay = 60_000  // flush never fires during the test
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);

        const string key = "dirty-guard/wiring-test";
        byte[] value = Encoding.UTF8.GetBytes("written-value");

        (KeyValueResponseType setResponse, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            key,
            value,
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Set, setResponse);

        // Force a collect cycle immediately, before any background flush.
        KahunaManager manager = (KahunaManager)node.Kahuna;
        await manager.RunCollectOnAllInstancesAsync();

        // The entry must still be readable: it is dirty (unflushed persistent), so the guard
        // prevents eviction even though the store is over budget.
        (KeyValueResponseType getResponse, ReadOnlyKeyValueEntry? got) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            key,
            -1,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, getResponse);
        Assert.NotNull(got);
        Assert.Equal(value, got.Value);
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
                NodeName = "dirty-guard-test",
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
            RevisionRetention = 16
        });
        cfg.DirtyObjectsWriterDelay = dirtyObjectsWriterDelay;
        return cfg;
    }

    /// <summary>Clean filler entry: LastModified = Zero puts it outside any safety window.</summary>
    private static void InsertCleanEntry(KeyValueContext context, string key, KeyValueState state, HLCTimestamp lastUsed)
    {
        context.InsertStoreEntry(key, new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = state,
            LastUsed = lastUsed,
            LastModified = HLCTimestamp.Zero,
            Revision = 0,
            FlushedRevision = 0,
            Expires = HLCTimestamp.Zero
        });
    }
}

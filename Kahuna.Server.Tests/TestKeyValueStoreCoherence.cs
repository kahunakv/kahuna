
using System.Text;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The resident store keeps an ordered tree and a point-lookup hash index over the same entries. Both must hold the
/// same key set and the same entry instances after every mutation the actor performs: an insert, a replace of the
/// entry object behind a key, a remove, a collect cycle that evicts tombstones and cold entries, the eviction of a
/// whole partition, and a leader-state drop. Point lookups are answered by the index, ordered walks by the tree,
/// so a divergence would make a key visible to one kind of read and invisible to the other.
/// </summary>
public sealed class TestKeyValueStoreCoherence : RaftTrackingTest
{
    [Fact]
    public void InsertReplaceRemove_KeepBothStructuresInStep()
    {
        KeyValueStore store = new(expectedEntries: 16);
        Assert.Null(store.FindIncoherence());
        Assert.Equal(0, store.Count);

        KeyValueEntry a = new() { Revision = 1 };
        KeyValueEntry b = new() { Revision = 2 };
        KeyValueEntry c = new() { Revision = 3 };
        store.Insert("s/a", a);
        store.Insert("s/b", b);
        store.Insert("t/c", c);
        Assert.Equal(3, store.Count);
        Assert.Null(store.FindIncoherence());

        Assert.True(store.TryGetValue("s/b", out KeyValueEntry? found));
        Assert.Same(b, found);
        Assert.Same(c, store.Get("t/c"));
        Assert.True(store.ContainsKey("s/a"));
        Assert.False(store.ContainsKey("s/zzz"));
        Assert.Null(store.Get("s/zzz"));

        // A replace swaps the instance behind the key in both structures.
        KeyValueEntry b2 = new() { Revision = 20 };
        store.Insert("s/b", b2);
        Assert.Equal(3, store.Count);
        Assert.Same(b2, store.Get("s/b"));
        Assert.Null(store.FindIncoherence());

        Assert.Equal(["s/a", "s/b"], store.GetByBucket("s").Select(kv => kv.Key).ToArray());
        Assert.Equal(["s/b", "t/c"], store.GetByRange("s/b", true, null, true, int.MaxValue).Select(kv => kv.Key).ToArray());
        Assert.Equal(["s/a", "s/b", "t/c"], store.GetItems().Select(kv => kv.Key).ToArray());

        Assert.True(store.Remove("s/a"));
        Assert.False(store.Remove("s/a"));
        Assert.Equal(2, store.Count);
        Assert.False(store.ContainsKey("s/a"));
        Assert.Null(store.FindIncoherence());
    }

    [Fact]
    public void InitialIndexCapacity_IsAnEighthOfTheEntryCap_WithAFloor()
    {
        Assert.Equal(1024, KeyValueStore.InitialIndexCapacity(0));
        Assert.Equal(1024, KeyValueStore.InitialIndexCapacity(100));
        Assert.Equal(6_250, KeyValueStore.InitialIndexCapacity(50_000));
        Assert.Equal(125_000, KeyValueStore.InitialIndexCapacity(1_000_000));
    }

    [Fact]
    public void ContextFunnels_ChargeTheIndexSlot_AndStayCoherent()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);

        long before = context.ApproximateStoreBytes;
        KeyValueEntry entry = Insert(context, "f/1", "v1", 1, now);
        long charged = context.ApproximateStoreBytes - before;
        Assert.Equal(KeyValueStoreAccounting.StoreKeyBytes("f/1") + entry.CachedBytes, charged);
        Assert.Null(context.Store.FindIncoherence());

        // Replacing the entry object behind the key refunds the old charge and takes the new one.
        KeyValueEntry replacement = Insert(context, "f/1", "a longer value than before", 2, now);
        Assert.Same(replacement, context.Store.Get("f/1"));
        Assert.Null(entry.Owner);
        Assert.Equal(before + KeyValueStoreAccounting.StoreKeyBytes("f/1") + replacement.CachedBytes, context.ApproximateStoreBytes);
        Assert.Null(context.Store.FindIncoherence());

        Assert.True(context.RemoveStoreEntry("f/1"));
        Assert.Equal(before, context.ApproximateStoreBytes);
        Assert.Equal(0, context.Store.Count);
        Assert.Null(context.Store.FindIncoherence());
    }

    [Fact]
    public void Collect_EvictsThroughTheFunnel_AndBothStructuresAgree()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.MaxEntriesPerActor = 4;
        (KeyValueContext context, RaftManager raft) = CreateContext(config);
        TryCollectHandler collector = new(context);
        HLCTimestamp stale = new(1, 1, 0);
        HLCTimestamp now = Now(raft);

        for (int i = 0; i < 10; i++)
            Insert(context, $"c/{i:D2}", "v", 1, stale, lastUsed: stale);
        for (int i = 0; i < 3; i++)
        {
            KeyValueEntry tombstone = Insert(context, $"t/{i}", "v", 1, now, lastUsed: now);
            tombstone.State = KeyValueState.Deleted;
            context.EnqueueTombstone($"t/{i}");
        }
        context.FlushHeadPublications();
        Assert.Equal(13, context.Store.Count);
        Assert.Null(context.Store.FindIncoherence());

        collector.Execute();
        context.FlushHeadPublications();

        Assert.True(context.Store.Count < 13);
        for (int i = 0; i < 3; i++)
            Assert.False(context.Store.ContainsKey($"t/{i}"), "a clean tombstone is reclaimed");
        Assert.Null(context.Store.FindIncoherence());
        Assert.Equal(context.Store.Count, context.Store.GetItems().Count());
    }

    [Fact]
    public void EvictPartition_AndDropLeaderState_LeaveBothStructuresInStep()
    {
        (KeyValueContext context, RaftManager raft) = CreateContext();
        HLCTimestamp now = Now(raft);

        for (int i = 0; i < 8; i++)
            Insert(context, $"p/{i}", "v", 1, now);
        KeyValueEntry locked = Insert(context, "p/locked", "v", 1, now);
        locked.WriteIntent = new KeyValueWriteIntent { TransactionId = now, Expires = now + 60_000 };
        locked.MvccEntries ??= new();
        context.FlushHeadPublications();
        Assert.Null(context.Store.FindIncoherence());

        new DropLeaderStateHandler(context).Execute(new KeyValueRequest(KeyValueRequestType.DropLeaderState, HLCTimestamp.Zero, HLCTimestamp.Zero, "p/locked",
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, OwnerOf(context, raft, "p/locked"), default));
        context.FlushHeadPublications();

        Assert.Null(locked.WriteIntent);
        Assert.Equal(9, context.Store.Count);
        Assert.Null(context.Store.FindIncoherence());

        new EvictPartitionHandler(context).Execute(new KeyValueRequest(KeyValueRequestType.EvictPartition, HLCTimestamp.Zero, HLCTimestamp.Zero, "",
            null, null, -1, KeyValueFlags.None, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, 0, OwnerOf(context, raft, "p/0"), default));
        context.FlushHeadPublications();

        Assert.Equal(0, context.Store.Count);
        Assert.Empty(context.Store.GetItems());
        Assert.False(context.Store.ContainsKey("p/0"));
        Assert.Null(context.Store.FindIncoherence());
    }

    // ── helpers ──────────────────────────────────────────────────────────────────────────

    private static int OwnerOf(KeyValueContext context, RaftManager raft, string key) =>
        PartitionDataEnumerator.OwnerOfKey(context.RangeMapStore.Current, key, raft.Configuration.InitialPartitions);

    private static HLCTimestamp Now(RaftManager raft) => raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

    private static KeyValueEntry Insert(KeyValueContext context, string key, string value, long revision, HLCTimestamp lastModified, HLCTimestamp? lastUsed = null)
    {
        KeyValueEntry entry = new()
        {
            Bucket = KeyValueKeySpace.OfKey(key),
            Value = Encoding.UTF8.GetBytes(value),
            Revision = revision,
            FlushedRevision = revision,
            LastModified = lastModified,
            LastUsed = lastUsed ?? lastModified,
            State = KeyValueState.Set
        };
        context.InsertStoreEntry(key, entry);
        return entry;
    }

    private static KahunaConfiguration CreateConfiguration() => ConfigurationValidator.Validate(new()
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

    private (KeyValueContext, RaftManager) CreateContext(KahunaConfiguration? config = null)
    {
        config ??= CreateConfiguration();
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = Track(new RaftManager(
            new RaftConfiguration
            {
                NodeName = "store-coherence",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false,
                PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger));

        KeyValueContext context = new(
            null!,
            new KeyValueStore(KeyValueStore.DefaultOrder, config.MaxEntriesPerActor),
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,
            null!,
            new MemoryPersistenceBackend(),
            raft,
            raft.ReadScheduler,
            new KeySpaceRegistry(),
            new RangeMapStore(raft, null, null, logger),
            config,
            logger);

        return (context, raft);
    }
}

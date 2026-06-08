using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
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

namespace Kahuna.Tests.Server;

public sealed class TestKeyValueCollection
{
    private readonly ILoggerFactory loggerFactory;

    public TestKeyValueCollection(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public void TestTryCollectHandlerReclaimsGarbageWithoutEntryFloor()
    {
        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp stale = new(1, 1, 0);

        InsertEntry(context, "live/a", KeyValueState.Set, now, HLCTimestamp.Zero);
        InsertEntry(context, "live/b", KeyValueState.Set, now, HLCTimestamp.Zero);
        InsertEntry(context, "live/c", KeyValueState.Set, now, HLCTimestamp.Zero);
        InsertEntry(context, "garbage/deleted", KeyValueState.Deleted, now, HLCTimestamp.Zero);
        InsertEntry(context, "garbage/expired", KeyValueState.Set, now, stale);

        handler.Execute();

        Assert.Equal(3, context.Store.Count);
        Assert.False(context.Store.ContainsKey("garbage/deleted"));
        Assert.False(context.Store.ContainsKey("garbage/expired"));
    }

    [Fact]
    public void TestTryCollectHandlerDoesNotEvictReplicationIntentEntryWhenOverBudget()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.MaxEntriesPerActor = 199;

        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp stale = new(1, 1, 0);

        for (int i = 0; i < 199; i++)
            InsertEntry(context, $"filler/{i:D3}", KeyValueState.Set, now, HLCTimestamp.Zero);

        InsertEntry(
            context,
            "replication/protected",
            KeyValueState.Set,
            stale,
            HLCTimestamp.Zero,
            new KeyValueReplicationIntent { ProposalId = 42, Expires = Future(now, TimeSpan.FromMinutes(10)) }
        );

        handler.Execute();

        Assert.True(context.Store.ContainsKey("replication/protected"));
        Assert.NotNull(context.Store.Get("replication/protected")?.ReplicationIntent);
    }

    [Fact]
    public void TestTryCollectHandlerDoesNotEvictLiveEntriesUnderBudget()
    {
        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 200; i++)
            InsertEntry(context, $"live/{i:D3}", KeyValueState.Set, now, HLCTimestamp.Zero);

        handler.Execute();

        Assert.Equal(200, context.Store.Count);
    }

    [Fact]
    public void TestTryCollectHandlerEnforcesMaxEntriesPerActor()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.MaxEntriesPerActor = 5;
        config.CollectBatchMax = 100;

        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 12; i++)
            InsertEntry(context, $"key/{i:D2}", KeyValueState.Set, now, HLCTimestamp.Zero);

        handler.Execute();

        Assert.True(context.Store.Count <= config.MaxEntriesPerActor);
    }

    [Fact]
    public void TestTryCollectHandlerEnforcesMaxBytesPerActor()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.MaxEntriesPerActor = 10_000;
        config.MaxBytesPerActor = 700;
        config.CollectBatchMax = 100;

        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        byte[] payload = Encoding.UTF8.GetBytes(new string('x', 200));

        for (int i = 0; i < 4; i++)
        {
            context.InsertStoreEntry($"bytes/{i}", new KeyValueEntry
            {
                Value = payload,
                State = KeyValueState.Set,
                LastUsed = now,
                Expires = HLCTimestamp.Zero
            });
        }

        Assert.True(context.ApproximateStoreBytes > config.MaxBytesPerActor);

        handler.Execute();

        Assert.True(context.ApproximateStoreBytes <= config.MaxBytesPerActor);
    }

    [Fact]
    public void TestTryCollectHandlerContinuesEvictionWhenBatchCapReached()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.MaxEntriesPerActor = 5;
        config.CollectBatchMax = 2;

        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 10; i++)
            InsertEntry(context, $"key/{i:D2}", KeyValueState.Set, now, HLCTimestamp.Zero);

        handler.Execute();
        Assert.Equal(8, context.Store.Count);

        handler.Execute();
        Assert.Equal(6, context.Store.Count);

        handler.Execute();
        Assert.True(context.Store.Count <= config.MaxEntriesPerActor);
    }

    [Fact]
    public void TestTryCollectHandlerSingleSweepRespectsCollectBatchMax()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.MaxEntriesPerActor = 5;
        config.CollectBatchMax = 3;

        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 20; i++)
            InsertEntry(context, $"key/{i:D2}", KeyValueState.Set, now, HLCTimestamp.Zero);

        handler.Execute();

        Assert.Equal(17, context.Store.Count);
    }

    [Fact]
    public void TestTryCollectHandlerTrimsRevisionMetadata()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.RevisionRetention = 4;
        config.MetadataTrimInterval = 1;

        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry entry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = now,
            Revisions = new()
        };

        for (int i = 0; i < 20; i++)
            entry.Revisions[i] = new KeyValueRevisionEntry(Encoding.UTF8.GetBytes("revision-" + i), now, HLCTimestamp.Zero, KeyValueState.Set);

        context.InsertStoreEntry("hot/key", entry);

        handler.Execute();

        KeyValueEntry? surviving = context.Store.Get("hot/key");
        Assert.NotNull(surviving?.Revisions);
        Assert.Equal(4, surviving.Revisions.Count);
    }

    [Fact]
    public void TestTryCollectHandlerSkipsMetadataTrimOffInterval()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.RevisionRetention = 4;
        config.MetadataTrimInterval = 2;

        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry entry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = now,
            Revisions = new()
        };

        for (int i = 0; i < 20; i++)
            entry.Revisions[i] = new KeyValueRevisionEntry(Encoding.UTF8.GetBytes("revision-" + i), now, HLCTimestamp.Zero, KeyValueState.Set);

        context.InsertStoreEntry("hot/key", entry);

        handler.Execute();
        Assert.Equal(20, context.Store.Get("hot/key")!.Revisions!.Count);

        handler.Execute();
        Assert.Equal(4, context.Store.Get("hot/key")!.Revisions!.Count);
    }

    [Fact]
    public void TestTryCollectHandlerReconcilesByteAccountingAfterMetadataTrim()
    {
        KahunaConfiguration config = CreateConfiguration();
        config.RevisionRetention = 4;
        config.MetadataTrimInterval = 1;

        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        KeyValueEntry entry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = now,
            Revisions = new()
        };

        byte[] revisionPayload = new byte[1024];
        for (int i = 0; i < 20; i++)
            entry.Revisions[i] = new KeyValueRevisionEntry(revisionPayload, now, HLCTimestamp.Zero, KeyValueState.Set);

        context.InsertStoreEntry("hot/key", entry);
        long bytesBeforeTrim = context.ApproximateStoreBytes;

        handler.Execute();

        Assert.Equal(4, context.Store.Get("hot/key")!.Revisions!.Count);
        Assert.True(context.ApproximateStoreBytes < bytesBeforeTrim);
    }

    [Fact]
    public void TestKeyValueCollectSamplerReturnsVictimsFromSmallStore()
    {
        BTree<string, KeyValueEntry> store = new(32);
        HLCTimestamp currentTime = new(1, 100, 0);
        HLCTimestamp lastUsed = new(1, 90, 0);

        for (int i = 0; i < 12; i++)
        {
            store.Insert($"key/{i:D2}", new KeyValueEntry
            {
                State = KeyValueState.Set,
                LastUsed = lastUsed
            });
        }

        HashSet<string> excluded = [];
        string? cursor = null;

        List<string> victims = KeyValueCollectSampler.SampleOldestVictims(
            store,
            excluded,
            currentTime,
            sampleSize: 5,
            scanMax: 256,
            maxVictims: 3,
            ref cursor,
            jitterOverride: 0
        );

        Assert.NotEmpty(victims);
    }

    [Fact]
    public void TestKeyValueCollectSamplerFindsGloballyOldestKeyOutsideInitialWindow()
    {
        BTree<string, KeyValueEntry> store = new(32);
        HLCTimestamp currentTime = new(1, 100, 0);
        HLCTimestamp recent = new(1, 90, 0);
        HLCTimestamp oldest = new(1, 10, 0);

        for (int i = 0; i < 500; i++)
        {
            store.Insert($"key/{i:D4}", new KeyValueEntry
            {
                State = KeyValueState.Set,
                LastUsed = recent
            });
        }

        store.Insert("key/4999", new KeyValueEntry
        {
            State = KeyValueState.Set,
            LastUsed = oldest
        });

        Assert.Equal(oldest, store.Get("key/4999")!.LastUsed);

        HashSet<string> excluded = [];
        string? cursor = "key/0499";

        List<string> victims = KeyValueCollectSampler.SampleOldestVictims(
            store,
            excluded,
            currentTime,
            sampleSize: 5,
            scanMax: 256,
            maxVictims: 1,
            ref cursor,
            jitterOverride: 0
        );

        Assert.Contains("key/4999", victims);
    }

    [Fact]
    public void TestKeyValueCollectSamplerRotatesThroughKeyspace()
    {
        BTree<string, KeyValueEntry> store = new(32);
        HLCTimestamp currentTime = new(1, 100, 0);
        HLCTimestamp recent = new(1, 90, 0);
        HLCTimestamp oldest = new(1, 10, 0);

        for (int i = 0; i < 500; i++)
        {
            store.Insert($"key/{i:D4}", new KeyValueEntry
            {
                State = KeyValueState.Set,
                LastUsed = recent
            });
        }

        store.Insert("key/4999", new KeyValueEntry
        {
            State = KeyValueState.Set,
            LastUsed = oldest
        });

        HashSet<string> excluded = [];
        string? cursor = null;
        bool sawOldestHighKey = false;

        for (int i = 0; i < 256; i++)
        {
            List<string> victims = KeyValueCollectSampler.SampleOldestVictims(
                store,
                excluded,
                currentTime,
                sampleSize: 5,
                scanMax: 256,
                maxVictims: 1,
                ref cursor,
                jitterOverride: 0
            );

            if (victims.Contains("key/4999"))
            {
                sawOldestHighKey = true;
                break;
            }
        }

        Assert.True(sawOldestHighKey);
    }

    [Fact]
    public void TestTryCollectHandlerPreservesOptimisticMvccEntriesWithoutWriteIntent()
    {
        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp liveTx = now;

        KeyValueEntry entry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = now,
            MvccEntries = new()
            {
                [liveTx] = new KeyValueMvccEntry
                {
                    State = KeyValueState.Set,
                    Revision = 0,
                    Expires = Future(now, TimeSpan.FromMinutes(1))
                }
            }
        };

        context.InsertStoreEntry("optimistic/key", entry);

        handler.Execute();

        KeyValueEntry? surviving = context.Store.Get("optimistic/key");
        Assert.NotNull(surviving?.MvccEntries);
        Assert.Single(surviving.MvccEntries);
        Assert.True(surviving.MvccEntries.ContainsKey(liveTx));
    }

    [Fact]
    public void TestTryCollectHandlerTrimsStaleMvccEntries()
    {
        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp liveTx = now;
        HLCTimestamp staleTx = new(now.N, now.L + 1, now.C);
        HLCTimestamp staleExpires = new(1, 1, 0);

        KeyValueEntry entry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = now,
            MvccEntries = new()
            {
                [liveTx] = new KeyValueMvccEntry
                {
                    State = KeyValueState.Set,
                    Revision = 1,
                    Expires = Future(now, TimeSpan.FromMinutes(1))
                },
                [staleTx] = new KeyValueMvccEntry
                {
                    State = KeyValueState.Set,
                    Revision = 0,
                    Expires = staleExpires
                }
            }
        };

        context.InsertStoreEntry("tx/key", entry);

        handler.Execute();

        KeyValueEntry? surviving = context.Store.Get("tx/key");
        Assert.NotNull(surviving?.MvccEntries);
        Assert.Single(surviving.MvccEntries);
        Assert.True(surviving.MvccEntries.ContainsKey(liveTx));
    }

    [Fact]
    public async Task TestCollectMessageDoesNotDisturbLiveEntries()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            KeyValueWorkers = 1
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);

        const string key = "tenant/collect/live";
        byte[] value = Encoding.UTF8.GetBytes("still-here");

        (KeyValueResponseType response, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            key,
            value,
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Set, response);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        await manager.RunCollectOnAllInstancesAsync();

        (response, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            key,
            -1,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Get, response);
        Assert.NotNull(entry);
        Assert.Equal(value, entry.Value);
    }

    [Fact]
    public async Task TestCollectEnforcesMaxEntriesBudgetOnEphemeralStore()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            KeyValueWorkers = 1,
            MaxEntriesPerActor = 50,
            CollectBatchMax = 100
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);

        const string prefix = "tenant/collect/budget";

        for (int i = 0; i < 100; i++)
        {
            (KeyValueResponseType response, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero,
                $"{prefix}/{i:D3}",
                Encoding.UTF8.GetBytes("v"),
                null,
                -1,
                KeyValueFlags.Set,
                0,
                KeyValueDurability.Ephemeral,
                TestContext.Current.CancellationToken);

            Assert.Equal(KeyValueResponseType.Set, response);
        }

        KahunaManager manager = (KahunaManager)node.Kahuna;

        int surviving = 100;
        for (int round = 0; round < 20 && surviving > 50; round++)
        {
            await manager.RunCollectOnAllInstancesAsync();

            surviving = 0;
            for (int i = 0; i < 100; i++)
            {
                (KeyValueResponseType responseType, _) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero,
                    $"{prefix}/{i:D3}",
                    -1,
                    KeyValueDurability.Ephemeral,
                    TestContext.Current.CancellationToken);

                if (responseType == KeyValueResponseType.Get)
                    surviving++;
            }
        }

        Assert.InRange(surviving, 0, 50);
        Assert.True(surviving < 100);
    }

    private static (TryCollectHandler Handler, KeyValueContext Context, BTree<string, KeyValueEntry> Store, RaftManager Raft) CreateHandler(
        KahunaConfiguration? configuration = null
    )
    {
        configuration ??= CreateConfiguration();

        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "collect-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1
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
            configuration,
            logger
        );

        return (new TryCollectHandler(context), context, store, raft);
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
            RevisionRetention = 16,
            LruSampleSize = 5,
            LruSampleScanMax = 256,
            MetadataTrimInterval = 1
        });
    }

    private static void InsertEntry(
        KeyValueContext context,
        string key,
        KeyValueState state,
        HLCTimestamp lastUsed,
        HLCTimestamp expires,
        KeyValueReplicationIntent? replicationIntent = null
    )
    {
        context.InsertStoreEntry(key, new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = state,
            LastUsed = lastUsed,
            Expires = expires,
            ReplicationIntent = replicationIntent
        });
    }

    private static HLCTimestamp Future(HLCTimestamp timestamp, TimeSpan amount) =>
        new(timestamp.N, timestamp.L + amount.Ticks, timestamp.C);
}

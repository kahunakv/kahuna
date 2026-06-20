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

    /// <summary>
    /// Phase C.3: the collector no longer has a metadata pass — stale MVCC entries with elapsed
    /// Expires are not trimmed by the collector. Trimming now happens at transaction resolve time
    /// (Phase C.2). An entry carrying both a live and an expired MVCC entry must be left intact
    /// by the collector.
    /// </summary>
    [Fact]
    public void Collector_DoesNotTrimStaleMvccEntries()
    {
        (TryCollectHandler handler, KeyValueContext context, _, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp liveTx = now;
        HLCTimestamp staleTx = new(now.N, now.L + 1000, now.C);
        HLCTimestamp staleExpires = new(1, 1, 0); // far in the past

        KeyValueEntry entry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = now,
            MvccEntries = new()
            {
                [liveTx] = new KeyValueMvccEntry { State = KeyValueState.Set, Revision = 1, Expires = Future(now, TimeSpan.FromMinutes(1)) },
                [staleTx] = new KeyValueMvccEntry { State = KeyValueState.Set, Revision = 0, Expires = staleExpires }
            }
        };

        context.InsertStoreEntry("tx/key", entry);

        handler.Execute();

        // Collector leaves both entries untouched — MVCC trim is the resolver's job.
        KeyValueEntry? surviving = context.Store.Get("tx/key");
        Assert.NotNull(surviving?.MvccEntries);
        Assert.Equal(2, surviving.MvccEntries.Count);
    }

    /// <summary>
    /// Phase C.2: TryRollbackMutationsHandler must trim expired-sibling MVCC entries at resolve
    /// time, but leave non-expired siblings intact (selectivity check).
    /// </summary>
    [Fact]
    public async Task Rollback_TrimsExpiredSiblingMvccEntries()
    {
        (_, KeyValueContext context, _, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp rollingBackTx = now;
        HLCTimestamp staleTx = new(now.N, now.L + 1000, now.C);
        HLCTimestamp liveSiblingTx = new(now.N, now.L + 2000, now.C);
        HLCTimestamp staleExpires = new(1, 1, 0); // far in the past

        KeyValueEntry entry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            Revision = 1,
            FlushedRevision = 1,
            LastUsed = now,
            LastModified = now,
            WriteIntent = new KeyValueWriteIntent
            {
                TransactionId = rollingBackTx,
                Expires = Future(now, TimeSpan.FromMinutes(1))
            },
            MvccEntries = new()
            {
                [rollingBackTx] = new KeyValueMvccEntry { State = KeyValueState.Set, Revision = 2, Expires = Future(now, TimeSpan.FromMinutes(1)) },
                [staleTx]       = new KeyValueMvccEntry { State = KeyValueState.Set, Revision = 0, Expires = staleExpires },
                [liveSiblingTx] = new KeyValueMvccEntry { State = KeyValueState.Set, Revision = 0, Expires = Future(now, TimeSpan.FromMinutes(5)) }
            }
        };

        context.InsertStoreEntry("tx/key", entry);

        TryRollbackMutationsHandler rollbackHandler = new(context);
        KeyValueResponse response = await rollbackHandler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryRollbackMutations,
            rollingBackTx,
            HLCTimestamp.Zero,
            "tx/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral,
            0, 0, null
        ));

        Assert.Equal(KeyValueResponseType.RolledBack, response.Type);

        KeyValueEntry? surviving = context.Store.Get("tx/key");
        Assert.NotNull(surviving);
        Assert.Null(surviving.WriteIntent);
        // rollingBackTx and staleTx removed; liveSiblingTx (not expired) must survive.
        Assert.NotNull(surviving.MvccEntries);
        Assert.Equal(1, surviving.MvccEntries.Count);
        Assert.True(surviving.MvccEntries.ContainsKey(liveSiblingTx),
            "non-expired sibling MVCC entry must not be trimmed");
        Assert.False(surviving.MvccEntries.ContainsKey(staleTx),
            "expired sibling MVCC entry must be trimmed at rollback time");
    }

    /// <summary>
    /// Phase C.2 / lock-release path: TryReleaseExclusiveLockHandler must also trim expired
    /// sibling MVCC entries at resolve time, and must leave non-expired siblings intact.
    /// </summary>
    [Fact]
    public async Task LockRelease_TrimsExpiredSiblingMvccEntries()
    {
        (_, KeyValueContext context, _, RaftManager raft) = CreateHandler();
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());
        HLCTimestamp releasingTx = now;
        HLCTimestamp staleTx = new(now.N, now.L + 1000, now.C);
        HLCTimestamp liveSiblingTx = new(now.N, now.L + 2000, now.C);
        HLCTimestamp staleExpires = new(1, 1, 0); // far in the past

        KeyValueEntry entry = new()
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            Revision = 1,
            FlushedRevision = 1,
            LastUsed = now,
            LastModified = now,
            WriteIntent = new KeyValueWriteIntent
            {
                TransactionId = releasingTx,
                Expires = Future(now, TimeSpan.FromMinutes(1))
            },
            MvccEntries = new()
            {
                [releasingTx]   = new KeyValueMvccEntry { State = KeyValueState.Set, Revision = 2, Expires = Future(now, TimeSpan.FromMinutes(1)) },
                [staleTx]       = new KeyValueMvccEntry { State = KeyValueState.Set, Revision = 0, Expires = staleExpires },
                [liveSiblingTx] = new KeyValueMvccEntry { State = KeyValueState.Set, Revision = 0, Expires = Future(now, TimeSpan.FromMinutes(5)) }
            }
        };

        context.InsertStoreEntry("lock/key", entry);

        TryReleaseExclusiveLockHandler lockReleaseHandler = new(context);
        KeyValueResponse response = await lockReleaseHandler.Execute(new KeyValueRequest(
            KeyValueRequestType.TryReleaseExclusiveLock,
            releasingTx,
            HLCTimestamp.Zero,
            "lock/key",
            null, null, -1, KeyValueFlags.None, 0,
            HLCTimestamp.Zero,
            KeyValueDurability.Ephemeral,
            0, 0, null
        ));

        Assert.Equal(KeyValueResponseType.Unlocked, response.Type);

        KeyValueEntry? surviving = context.Store.Get("lock/key");
        Assert.NotNull(surviving);
        Assert.Null(surviving.WriteIntent);
        // releasingTx and staleTx removed; liveSiblingTx (not expired) must survive.
        Assert.NotNull(surviving.MvccEntries);
        Assert.Equal(1, surviving.MvccEntries.Count);
        Assert.True(surviving.MvccEntries.ContainsKey(liveSiblingTx),
            "non-expired sibling MVCC entry must not be trimmed");
        Assert.False(surviving.MvccEntries.ContainsKey(staleTx),
            "expired sibling MVCC entry must be trimmed at lock release time");
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
            HLCTimestamp.Zero,
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
                    HLCTimestamp.Zero,
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
                InitialPartitions = 1,
                EnableQuiescence = false // standalone test node: keep the pre-upgrade heartbeat model, no SWIM dependency
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
            RevisionRetention = 16
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

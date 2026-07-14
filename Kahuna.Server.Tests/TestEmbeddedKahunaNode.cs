using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kommander;
using Kommander.Time;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

[Collection("ClusterTests")]
public sealed class TestEmbeddedKahunaNode
{
    private readonly ILoggerFactory loggerFactory;

    public TestEmbeddedKahunaNode(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task TestEmbeddedOptionsPassThroughKommanderConfiguration()
    {
        EmbeddedKahunaOptions options = new()
        {
            NodeName = "embedded-config-test",
            NodeId = 42,
            Host = "localhost",
            Port = 0,
            InitialPartitions = 1,
            Storage = "memory",
            WalStorage = "memory",
            HttpScheme = "http://",
            HttpAuthBearerToken = "raft-token",
            HttpTimeout = 9,
            HttpVersion = "1.1",
            HeartbeatInterval = TimeSpan.FromMilliseconds(75),
            RecentHeartbeat = TimeSpan.FromMilliseconds(25),
            VotingTimeout = TimeSpan.FromMilliseconds(350),
            CheckLeaderInterval = TimeSpan.FromMilliseconds(40),
            TimerInitialDelay = TimeSpan.FromMilliseconds(25),
            UpdateNodesInterval = TimeSpan.FromMilliseconds(125),
            StartElectionTimeout = 150,
            EndElectionTimeout = 300,
            StartElectionTimeoutIncrement = 11,
            EndElectionTimeoutIncrement = 22,
            SlowRaftStateMachineLog = 33,
            SlowRaftWALMachineLog = 44,
            ReadIOThreads = 3,
            WriteIOThreads = 2,
            CompactEveryOperations = 123,
            CompactNumberEntries = 12,
            MaxEntriesPerCompaction = 345
        };

        await using EmbeddedKahunaNode node = new(options, loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        RaftConfiguration config = node.Raft.Configuration;

        Assert.Equal(options.NodeName, config.NodeName);
        Assert.Equal(options.NodeId, config.NodeId);
        Assert.Equal(options.Host, config.Host);
        Assert.Equal(options.Port, config.Port);
        Assert.Equal(options.InitialPartitions, config.InitialPartitions);
        Assert.Equal(options.HttpScheme, config.HttpScheme);
        Assert.Equal(options.HttpAuthBearerToken, config.HttpAuthBearerToken);
        Assert.Equal(options.HttpTimeout, config.HttpTimeout);
        Assert.Equal(options.HttpVersion, config.HttpVersion);
        Assert.Equal(options.HeartbeatInterval, config.HeartbeatInterval);
        Assert.Equal(options.RecentHeartbeat, config.RecentHeartbeat);
        Assert.Equal(options.VotingTimeout, config.VotingTimeout);
        Assert.Equal(options.CheckLeaderInterval, config.CheckLeaderInterval);
        Assert.Equal(options.TimerInitialDelay, config.TimerInitialDelay);
        Assert.Equal(options.UpdateNodesInterval, config.UpdateNodesInterval);
        Assert.Equal(options.StartElectionTimeout, config.StartElectionTimeout);
        Assert.Equal(options.EndElectionTimeout, config.EndElectionTimeout);
        Assert.Equal(options.StartElectionTimeoutIncrement, config.StartElectionTimeoutIncrement);
        Assert.Equal(options.EndElectionTimeoutIncrement, config.EndElectionTimeoutIncrement);
        Assert.Equal(options.SlowRaftStateMachineLog, config.SlowRaftStateMachineLog);
        Assert.Equal(options.SlowRaftWALMachineLog, config.SlowRaftWALMachineLog);
        Assert.Equal(options.ReadIOThreads, config.ReadIOThreads);
        Assert.Equal(options.WriteIOThreads, config.WriteIOThreads);
        Assert.Equal(options.CompactEveryOperations, config.CompactEveryOperations);
        Assert.Equal(options.CompactNumberEntries, config.CompactNumberEntries);
        Assert.Equal(options.MaxEntriesPerCompaction, config.MaxEntriesPerCompaction);
    }

    [Fact]
    public async Task TestEmbeddedNodeCanStartUseKeyValuesAndDispose()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);

        string leader = await node.WaitForLeaderForKeyAsync("tenant/table/key-a", TestContext.Current.CancellationToken);
        Assert.Equal(node.Raft.GetLocalEndpoint(), leader);

        byte[] valueA = Encoding.UTF8.GetBytes("value-a");
        (KeyValueResponseType response, long revision, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            "tenant/table/key-a",
            valueA,
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);

        (response, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            "tenant/table/key-a",
            -1,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Get, response);
        Assert.NotNull(entry);
        Assert.Equal(valueA, entry.Value);

        await SetValue(node, "tenant/table/key-c", "value-c");
        await SetValue(node, "tenant/table/key-b", "value-b");

        KeyValueGetByBucketResult bucket = await node.Kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero,
            "tenant/table",
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Get, bucket.Type);
        Assert.Equal(
            ["tenant/table/key-a", "tenant/table/key-b", "tenant/table/key-c"],
            bucket.Items.Select(item => item.Item1).ToArray()
        );

        TransactionHandle txHandle;
        (response, txHandle) = await node.Kahuna.LocateAndStartTransaction(
            new()
            {
                CoordinatorKey = "tenant/tx",
                Timeout = 5000,
                Locking = KeyValueTransactionLocking.Pessimistic
            },
            TestContext.Current.CancellationToken
        );
        HLCTimestamp transactionId = txHandle.TransactionId;

        Assert.Equal(KeyValueResponseType.Set, response);

        string txKey = "tenant/table/key-tx";
        (response, _, _, _) = await node.Kahuna.LocateAndTryAcquireExclusiveLock(
            transactionId,
            txKey,
            5000,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken,
            coordinatorKey: txHandle.CoordinatorKey,
            operationId: TransactionOperationId.NewRandom()
        );

        Assert.Equal(KeyValueResponseType.Locked, response);

        (response, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            transactionId,
            txKey,
            Encoding.UTF8.GetBytes("value-tx"),
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken,
            coordinatorKey: txHandle.CoordinatorKey,
            operationId: TransactionOperationId.NewRandom()
        );

        Assert.Equal(KeyValueResponseType.Set, response);

        (response, _) = await node.Kahuna.LocateAndCommitTransaction(txHandle, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Committed, response);

        (response, entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            txKey,
            -1,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Get, response);
        Assert.NotNull(entry);
        Assert.Equal("value-tx", Encoding.UTF8.GetString(entry.Value ?? []));
    }

    [Fact]
    public async Task TestEmbeddedNodeCanBeCreatedTwice()
    {
        await using (EmbeddedKahunaNode first = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory))
        {
            await first.StartAsync(TestContext.Current.CancellationToken);
        }

        await using EmbeddedKahunaNode second = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await second.StartAsync(TestContext.Current.CancellationToken);
        string leader = await second.WaitForLeaderForKeyAsync("tenant/table/key-a", TestContext.Current.CancellationToken);

        Assert.Equal(second.Raft.GetLocalEndpoint(), leader);
    }

    [Fact]
    public async Task TestFlushPersistsPendingKeyValues()
    {
        string storagePath = CreateTempStoragePath();

        try
        {
            byte[] value = Encoding.UTF8.GetBytes("flush-value");

            await using (EmbeddedKahunaNode node = new(CreateSqliteOptions(storagePath), loggerFactory))
            {
                await node.StartAsync(TestContext.Current.CancellationToken);

                (KeyValueResponseType response, long revision, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero,
                    "tenant/flush/key-a",
                    value,
                    null,
                    -1,
                    KeyValueFlags.Set,
                    0,
                    KeyValueDurability.Persistent,
                    TestContext.Current.CancellationToken
                );

                Assert.Equal(KeyValueResponseType.Set, response);
                Assert.Equal(0, revision);

                await node.FlushAsync();
            }

            PersistentKeyValue? persisted = ReadPersistedKeyValue(storagePath, "tenant/flush/key-a");

            Assert.NotNull(persisted);
            Assert.Equal(0, persisted.Revision);
            Assert.Equal(value, persisted.Value);
            Assert.Equal((int)KeyValueState.Set, persisted.State);
        }
        finally
        {
            TryDeleteDirectory(storagePath);
        }
    }

    [Fact]
    public async Task TestFlushPersistsPendingLocks()
    {
        string storagePath = CreateTempStoragePath();

        try
        {
            byte[] owner = Encoding.UTF8.GetBytes("flush-owner");

            await using (EmbeddedKahunaNode node = new(CreateSqliteOptions(storagePath), loggerFactory))
            {
                await node.StartAsync(TestContext.Current.CancellationToken);

                (LockResponseType response, long fencingToken) = await node.Kahuna.LocateAndTryLock(
                    "tenant/flush/lock-a",
                    owner,
                    30000,
                    LockDurability.Persistent,
                    TestContext.Current.CancellationToken
                );

                Assert.Equal(LockResponseType.Locked, response);
                Assert.Equal(0, fencingToken);

                await node.FlushAsync();
            }

            PersistentLock? persisted = ReadPersistedLock(storagePath, "tenant/flush/lock-a");

            Assert.NotNull(persisted);
            Assert.Equal(owner, persisted.Owner);
            Assert.Equal(0, persisted.FencingToken);
            Assert.Equal((int)LockState.Locked, persisted.State);
        }
        finally
        {
            TryDeleteDirectory(storagePath);
        }
    }

    [Fact]
    public async Task TestFullSweepPrunesRevisionsNotReachedByTargetedCleanup()
    {
        string storagePath = CreateTempStoragePath();

        try
        {
            const string key = "sweep/pre-existing/key1";
            const string storageRevision = "sweep-tests";

            // Phase 1: write revisions with retention disabled so nothing is pruned on write.
            EmbeddedKahunaOptions phaseOneOptions = new()
            {
                Storage = "sqlite",
                StoragePath = storagePath,
                StorageRevision = storageRevision,
                WalStorage = "memory",
                InitialPartitions = 1,
                DirtyObjectsWriterDelay = 60000   // Prevent timer from firing during writes.
            };

            await using (EmbeddedKahunaNode node = new(phaseOneOptions, loggerFactory))
            {
                await node.StartAsync(TestContext.Current.CancellationToken);

                for (int i = 0; i < 6; i++)
                {
                    await node.Kahuna.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, key,
                        Encoding.UTF8.GetBytes($"v{i}"),
                        null, -1, KeyValueFlags.Set, 0,
                        KeyValueDurability.Persistent,
                        TestContext.Current.CancellationToken
                    );
                }

                await node.FlushAsync();
            }

            Assert.Equal(6, CountRevisionRows(storagePath, key, storageRevision));

            // Phase 2: restart with retention enabled.
            // DirtyObjectsWriterDelay = 100 ms → timer fires at 100 ms and every 100 ms after.
            // CleanupInterval = 50 ms → after the first 100 ms tick, (100 ms − 0 ms) ≥ 50 ms,
            // so the sweep fires on the first cycle. CleanupOnWrite is off so only the sweep prunes.
            EmbeddedKahunaOptions phaseTwoOptions = new()
            {
                Storage = "sqlite",
                StoragePath = storagePath,
                StorageRevision = storageRevision,
                WalStorage = "memory",
                InitialPartitions = 1,
                DirtyObjectsWriterDelay = 100,
                PersistentRevisionRetentionCount = 2,
                PersistentRevisionCleanupOnWrite = false,
                PersistentRevisionCleanupBatchSize = 1000,
                PersistentRevisionCleanupInterval = TimeSpan.FromMilliseconds(50)
            };

            await using (EmbeddedKahunaNode node = new(phaseTwoOptions, loggerFactory))
            {
                await node.StartAsync(TestContext.Current.CancellationToken);

                // 500 ms gives several timer cycles; the sweep fires on the first one where the
                // 50 ms interval has elapsed since startup.
                await Task.Delay(500, TestContext.Current.CancellationToken);
            }

            Assert.Equal(2, CountRevisionRows(storagePath, key, storageRevision));

            PersistentKeyValue? current = ReadPersistedKeyValue(storagePath, key, storageRevision);
            Assert.NotNull(current);
        }
        finally
        {
            TryDeleteDirectory(storagePath);
        }
    }

    [Fact]
    public async Task TestFullSweepDoesNotRunWhenRetentionDisabled()
    {
        string storagePath = CreateTempStoragePath();

        try
        {
            const string key = "sweep/disabled/key1";
            const string storageRevision = "sweep-disabled-tests";

            // Fast timer, retention disabled.
            EmbeddedKahunaOptions options = new()
            {
                Storage = "sqlite",
                StoragePath = storagePath,
                StorageRevision = storageRevision,
                WalStorage = "memory",
                InitialPartitions = 1,
                DirtyObjectsWriterDelay = 100
            };

            await using (EmbeddedKahunaNode node = new(options, loggerFactory))
            {
                await node.StartAsync(TestContext.Current.CancellationToken);

                for (int i = 0; i < 4; i++)
                {
                    await node.Kahuna.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, key,
                        Encoding.UTF8.GetBytes($"v{i}"),
                        null, -1, KeyValueFlags.Set, 0,
                        KeyValueDurability.Persistent,
                        TestContext.Current.CancellationToken
                    );
                }

                await node.FlushAsync();

                // Let the periodic timer fire several times; without retention config the
                // sweep guard returns early immediately.
                await Task.Delay(500, TestContext.Current.CancellationToken);
            }

            Assert.Equal(4, CountRevisionRows(storagePath, key, storageRevision));
        }
        finally
        {
            TryDeleteDirectory(storagePath);
        }
    }

    [Fact]
    public async Task TestFullSweepDoesNotRunBeforeInterval()
    {
        string storagePath = CreateTempStoragePath();

        try
        {
            const string key = "sweep/interval/key1";
            const string storageRevision = "sweep-interval-tests";

            // Fast periodic timer (100 ms) but a 1-hour sweep interval.
            // lastFullSweepUtc is set to DateTime.UtcNow at startup, so the first sweep
            // cannot fire until at least 1 hour has elapsed — well beyond the 500 ms window.
            EmbeddedKahunaOptions options = new()
            {
                Storage = "sqlite",
                StoragePath = storagePath,
                StorageRevision = storageRevision,
                WalStorage = "memory",
                InitialPartitions = 1,
                DirtyObjectsWriterDelay = 100,
                PersistentRevisionRetentionCount = 1,
                PersistentRevisionCleanupOnWrite = false,
                PersistentRevisionCleanupBatchSize = 1000,
                PersistentRevisionCleanupInterval = TimeSpan.FromHours(1)
            };

            await using (EmbeddedKahunaNode node = new(options, loggerFactory))
            {
                await node.StartAsync(TestContext.Current.CancellationToken);

                for (int i = 0; i < 5; i++)
                {
                    await node.Kahuna.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero, key,
                        Encoding.UTF8.GetBytes($"v{i}"),
                        null, -1, KeyValueFlags.Set, 0,
                        KeyValueDurability.Persistent,
                        TestContext.Current.CancellationToken
                    );
                }

                await node.FlushAsync();

                // Let several periodic Flush cycles fire; the 1-hour interval gate must block
                // the sweep on every one of them.
                await Task.Delay(500, TestContext.Current.CancellationToken);
            }

            // Sweep never fired, so all 5 revision rows must still be present.
            Assert.Equal(5, CountRevisionRows(storagePath, key, storageRevision));
        }
        finally
        {
            TryDeleteDirectory(storagePath);
        }
    }

    [Fact]
    public async Task TestEmbeddedNodeSupportsRocksDbStorage()
    {
        string storagePath = CreateTempStoragePath();

        try
        {
            byte[] value = Encoding.UTF8.GetBytes("rocksdb-value");
            byte[] owner = Encoding.UTF8.GetBytes("rocksdb-owner");

            await using (EmbeddedKahunaNode first = new(CreateRocksDbOptions(storagePath), loggerFactory))
            {
                await first.StartAsync(TestContext.Current.CancellationToken);

                (KeyValueResponseType response, long revision, _) = await first.Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero,
                    "tenant/rocksdb/key-a",
                    value,
                    null,
                    -1,
                    KeyValueFlags.Set,
                    0,
                    KeyValueDurability.Persistent,
                    TestContext.Current.CancellationToken
                );

                Assert.Equal(KeyValueResponseType.Set, response);
                Assert.Equal(0, revision);

                (LockResponseType lockResponse, long fencingToken) = await first.Kahuna.LocateAndTryLock(
                    "tenant/rocksdb/lock-a",
                    owner,
                    30000,
                    LockDurability.Persistent,
                    TestContext.Current.CancellationToken
                );

                Assert.Equal(LockResponseType.Locked, lockResponse);
                Assert.Equal(0, fencingToken);

                await first.FlushAsync();
            }

            await using EmbeddedKahunaNode second = new(CreateRocksDbOptions(storagePath), loggerFactory);
            await second.StartAsync(TestContext.Current.CancellationToken);

            (KeyValueResponseType responseType, ReadOnlyKeyValueEntry? entry) = await second.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero,
                "tenant/rocksdb/key-a",
                -1,
                HLCTimestamp.Zero,
                KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken
            );

            Assert.Equal(KeyValueResponseType.Get, responseType);
            Assert.NotNull(entry);
            Assert.Equal(value, entry.Value);

            (LockResponseType lockResponseType, ReadOnlyLockEntry? lockEntry) = await second.Kahuna.LocateAndGetLock(
                "tenant/rocksdb/lock-a",
                LockDurability.Persistent,
                TestContext.Current.CancellationToken
            );

            Assert.Equal(LockResponseType.Got, lockResponseType);
            Assert.NotNull(lockEntry);
            Assert.Equal(owner, lockEntry.Owner);
            Assert.Equal(0, lockEntry.FencingToken);
        }
        finally
        {
            TryDeleteDirectory(storagePath);
        }
    }

    private static EmbeddedKahunaOptions CreatePersistentWalOptions(string storagePath, string walPath) => new()
    {
        Storage = "sqlite",
        StoragePath = storagePath,
        StorageRevision = "receipt-restart",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "receipt-restart-wal",
        InitialPartitions = 1,
        DirtyObjectsWriterDelay = 60000
    };

    /// <summary>
    /// A persistent-participant completion receipt survives a cold restart: with a durable WAL, the
    /// committed 2PC record (which carries the transaction id) replays on startup and rebuilds the
    /// receipt, so a re-commit after restart resolves Committed rather than MustRetry.
    /// </summary>
    [Fact]
    public async Task CompletionReceipt_SurvivesColdRestart_ViaPersistentWalReplay()
    {
        string storagePath = CreateTempStoragePath();
        string walPath = CreateTempStoragePath();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;
            const string key = "tenant/receipt/restart-a";
            byte[] value = "committed-value"u8.ToArray();

            HLCTimestamp txId;
            HLCTimestamp ticketId;

            await using (EmbeddedKahunaNode first = new(CreatePersistentWalOptions(storagePath, walPath), loggerFactory))
            {
                await first.StartAsync(ct);

                var clock = first.Raft.HybridLogicalClock;
                txId = clock.TrySendOrLocalEvent(first.Raft.GetLocalNodeId());
                HLCTimestamp commitId = clock.TrySendOrLocalEvent(first.Raft.GetLocalNodeId());

                (KeyValueResponseType setType, _, _) = await first.Kahuna.TrySetKeyValue(
                    txId, key, value, null, -1, KeyValueFlags.None, 0, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Set, setType);

                (KeyValueResponseType prepType, HLCTimestamp ticket, _, _) = await first.Kahuna.TryPrepareMutations(
                    txId, commitId, key, KeyValueDurability.Persistent, recordAnchorKey: key);
                Assert.Equal(KeyValueResponseType.Prepared, prepType);
                ticketId = ticket;

                (KeyValueResponseType commitType, _) = await first.Kahuna.TryCommitMutations(
                    txId, key, ticketId, KeyValueDurability.Persistent);
                Assert.Equal(KeyValueResponseType.Committed, commitType);

                Assert.True(((KahunaManager)first.Kahuna).CompletionReceiptStore.Contains(txId, key, KeyValueDurability.Persistent));
            }

            // Cold restart over the same durable WAL + storage: the committed record replays.
            await using EmbeddedKahunaNode second = new(CreatePersistentWalOptions(storagePath, walPath), loggerFactory);
            await second.StartAsync(ct);

            var store = ((KahunaManager)second.Kahuna).CompletionReceiptStore;

            long deadline = Environment.TickCount64 + 10_000;
            while (!store.Contains(txId, key, KeyValueDurability.Persistent) && Environment.TickCount64 < deadline)
                await Task.Delay(50, ct);

            Assert.True(store.Contains(txId, key, KeyValueDurability.Persistent), "receipt was not rebuilt from WAL replay after cold restart");

            // The re-commit resolves Committed via the rebuilt receipt (the node never re-prepared).
            (KeyValueResponseType recommit, _) = await second.Kahuna.TryCommitMutations(
                txId, key, ticketId, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Committed, recommit);
        }
        finally
        {
            TryDeleteDirectory(storagePath);
            TryDeleteDirectory(walPath);
        }
    }

    private static async Task SetValue(EmbeddedKahunaNode node, string key, string value)
    {
        (KeyValueResponseType response, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            key,
            Encoding.UTF8.GetBytes(value),
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Set, response);
    }

    [Fact]
    public async Task TestFlushRunsTargetedRevisionCleanupAfterWrite()
    {
        string storagePath = CreateTempStoragePath();

        try
        {
            const string key = "retention/targeted/key1";
            const string storageRevision = "cleanup-tests";

            EmbeddedKahunaOptions options = new()
            {
                Storage = "sqlite",
                StoragePath = storagePath,
                StorageRevision = storageRevision,
                WalStorage = "memory",
                InitialPartitions = 1,
                DirtyObjectsWriterDelay = 60000,
                PersistentRevisionRetentionCount = 3,
                PersistentRevisionCleanupOnWrite = true,
                PersistentRevisionCleanupBatchSize = 1000
            };

            await using (EmbeddedKahunaNode node = new(options, loggerFactory))
            {
                await node.StartAsync(TestContext.Current.CancellationToken);

                // Write 7 revisions; with retention count 3, 4 old ones should be pruned.
                for (int i = 0; i < 7; i++)
                {
                    byte[] value = Encoding.UTF8.GetBytes($"value-{i}");

                    (KeyValueResponseType response, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero,
                        key,
                        value,
                        null,
                        -1,
                        KeyValueFlags.Set,
                        0,
                        KeyValueDurability.Persistent,
                        TestContext.Current.CancellationToken
                    );

                    Assert.Equal(KeyValueResponseType.Set, response);
                }

                // FlushAsync triggers FlushKeyValues → tracks key → RunTargetedRevisionCleanup.
                await node.FlushAsync();
            }

            // Current row in keys table should survive.
            PersistentKeyValue? current = ReadPersistedKeyValue(storagePath, key, storageRevision);
            Assert.NotNull(current);
            Assert.Equal("value-6", Encoding.UTF8.GetString(current.Value!));

            // Only 3 revision rows should remain.
            Assert.Equal(3, CountRevisionRows(storagePath, key, storageRevision));
        }
        finally
        {
            TryDeleteDirectory(storagePath);
        }
    }

    [Fact]
    public async Task TestFlushWithRetentionDisabledDoesNotPruneRevisions()
    {
        string storagePath = CreateTempStoragePath();

        try
        {
            const string key = "retention/disabled/key1";
            const string storageRevision = "no-cleanup-tests";

            EmbeddedKahunaOptions options = new()
            {
                Storage = "sqlite",
                StoragePath = storagePath,
                StorageRevision = storageRevision,
                WalStorage = "memory",
                InitialPartitions = 1,
                DirtyObjectsWriterDelay = 60000
                // PersistentRevisionRetentionCount and Age are 0 by default — retention disabled.
            };

            await using (EmbeddedKahunaNode node = new(options, loggerFactory))
            {
                await node.StartAsync(TestContext.Current.CancellationToken);

                for (int i = 0; i < 5; i++)
                {
                    byte[] value = Encoding.UTF8.GetBytes($"value-{i}");

                    await node.Kahuna.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero,
                        key,
                        value,
                        null,
                        -1,
                        KeyValueFlags.Set,
                        0,
                        KeyValueDurability.Persistent,
                        TestContext.Current.CancellationToken
                    );
                }

                await node.FlushAsync();
            }

            // All 5 revision rows must remain because retention is disabled.
            Assert.Equal(5, CountRevisionRows(storagePath, key, storageRevision));
        }
        finally
        {
            TryDeleteDirectory(storagePath);
        }
    }

    [Fact]
    public async Task TestFlushWithCleanupOnWriteDisabledDoesNotPruneRevisions()
    {
        string storagePath = CreateTempStoragePath();

        try
        {
            const string key = "retention/onwrite-off/key1";
            const string storageRevision = "onwrite-off-tests";

            EmbeddedKahunaOptions options = new()
            {
                Storage = "sqlite",
                StoragePath = storagePath,
                StorageRevision = storageRevision,
                WalStorage = "memory",
                InitialPartitions = 1,
                DirtyObjectsWriterDelay = 60000,
                PersistentRevisionRetentionCount = 2,
                PersistentRevisionCleanupOnWrite = false,
                PersistentRevisionCleanupBatchSize = 1000
            };

            await using (EmbeddedKahunaNode node = new(options, loggerFactory))
            {
                await node.StartAsync(TestContext.Current.CancellationToken);

                for (int i = 0; i < 5; i++)
                {
                    byte[] value = Encoding.UTF8.GetBytes($"value-{i}");

                    await node.Kahuna.LocateAndTrySetKeyValue(
                        HLCTimestamp.Zero,
                        key,
                        value,
                        null,
                        -1,
                        KeyValueFlags.Set,
                        0,
                        KeyValueDurability.Persistent,
                        TestContext.Current.CancellationToken
                    );
                }

                await node.FlushAsync();
            }

            // CleanupOnWrite is off so no pruning should have occurred.
            Assert.Equal(5, CountRevisionRows(storagePath, key, storageRevision));
        }
        finally
        {
            TryDeleteDirectory(storagePath);
        }
    }

    private static EmbeddedKahunaOptions CreateSqliteOptions(string storagePath)
    {
        return new()
        {
            Storage = "sqlite",
            StoragePath = storagePath,
            StorageRevision = "flush-tests",
            WalStorage = "memory",
            InitialPartitions = 1,
            DirtyObjectsWriterDelay = 60000
        };
    }

    private static EmbeddedKahunaOptions CreateRocksDbOptions(string storagePath)
    {
        return new()
        {
            Storage = "rocksdb",
            StoragePath = storagePath,
            StorageRevision = "rocksdb-tests",
            WalStorage = "memory",
            InitialPartitions = 1,
            DirtyObjectsWriterDelay = 60000
        };
    }

    private static string CreateTempStoragePath()
    {
        string storagePath = Path.Combine(Path.GetTempPath(), "kahuna-flush-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(storagePath);
        return storagePath;
    }

    private static PersistentKeyValue? ReadPersistedKeyValue(string storagePath, string key) =>
        ReadPersistedKeyValue(storagePath, key, "flush-tests");

    private static PersistentKeyValue? ReadPersistedKeyValue(string storagePath, string key, string storageRevision)
    {
        foreach (string databasePath in Directory.EnumerateFiles(storagePath, $"kahuna*_{storageRevision}.db"))
        {
            using SqliteConnection connection = new($"Data Source={databasePath};Mode=ReadOnly");
            connection.Open();

            using SqliteCommand command = connection.CreateCommand();
            command.CommandText = "SELECT revision, value, state FROM keys WHERE key = $key";
            command.Parameters.AddWithValue("$key", key);

            using SqliteDataReader reader = command.ExecuteReader();
            if (!reader.Read())
                continue;

            return new(
                reader.GetInt64(0),
                reader.IsDBNull(1) ? null : (byte[])reader["value"],
                reader.GetInt32(2)
            );
        }

        return null;
    }

    private static int CountRevisionRows(string storagePath, string key, string storageRevision)
    {
        int total = 0;

        foreach (string databasePath in Directory.EnumerateFiles(storagePath, $"kahuna*_{storageRevision}.db"))
        {
            using SqliteConnection connection = new($"Data Source={databasePath};Mode=ReadOnly");
            connection.Open();

            using SqliteCommand command = connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*) FROM keys_revisions WHERE key = $key";
            command.Parameters.AddWithValue("$key", key);

            total += Convert.ToInt32(command.ExecuteScalar());
        }

        return total;
    }

    private static PersistentLock? ReadPersistedLock(string storagePath, string resource)
    {
        foreach (string databasePath in Directory.EnumerateFiles(storagePath, "kahuna*_flush-tests.db"))
        {
            using SqliteConnection connection = new($"Data Source={databasePath};Mode=ReadOnly");
            connection.Open();

            using SqliteCommand command = connection.CreateCommand();
            command.CommandText = "SELECT owner, fencingToken, state FROM locks WHERE resource = $resource";
            command.Parameters.AddWithValue("$resource", resource);

            using SqliteDataReader reader = command.ExecuteReader();
            if (!reader.Read())
                continue;

            return new(
                reader.IsDBNull(0) ? null : (byte[])reader["owner"],
                reader.GetInt64(1),
                reader.GetInt32(2)
            );
        }

        return null;
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private sealed record PersistentKeyValue(long Revision, byte[]? Value, int State);

    private sealed record PersistentLock(byte[]? Owner, long FencingToken, int State);
}

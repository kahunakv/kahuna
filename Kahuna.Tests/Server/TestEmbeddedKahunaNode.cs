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

namespace Kahuna.Tests.Server;

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
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Get, bucket.Type);
        Assert.Equal(
            ["tenant/table/key-a", "tenant/table/key-b", "tenant/table/key-c"],
            bucket.Items.Select(item => item.Item1).ToArray()
        );

        (response, HLCTimestamp transactionId) = await node.Kahuna.LocateAndStartTransaction(
            new()
            {
                UniqueId = "tenant/tx",
                Timeout = 5000,
                Locking = KeyValueTransactionLocking.Pessimistic
            },
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Set, response);

        string txKey = "tenant/table/key-tx";
        (response, _, _) = await node.Kahuna.LocateAndTryAcquireExclusiveLock(
            transactionId,
            txKey,
            5000,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
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
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Set, response);

        response = await node.Kahuna.LocateAndCommitTransaction(
            "tenant/tx",
            transactionId,
            [new() { Key = txKey, Durability = KeyValueDurability.Persistent }],
            [new() { Key = txKey, Durability = KeyValueDurability.Persistent }],
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Committed, response);

        (response, entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            txKey,
            -1,
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

    private static PersistentKeyValue? ReadPersistedKeyValue(string storagePath, string key)
    {
        foreach (string databasePath in Directory.EnumerateFiles(storagePath, "kahuna*_flush-tests.db"))
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

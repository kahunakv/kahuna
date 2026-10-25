using System.Text;

using Microsoft.Extensions.Logging;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.System;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The hash pool is the set of user partitions created at the cluster's first boot, which the committed
/// partition map records. The configured <c>InitialPartitions</c> is what that first boot writes into the
/// map and nothing more: a later boot with another value must route exactly as before. Routing over the
/// configured value instead sent every placement group the larger pool moved to a partition that does not
/// exist, where its rows could not be read and its writes never landed.
/// </summary>
public sealed class TestHashPoolFromCommittedMap : IDisposable
{
    private static readonly TimeSpan RunDeadline = TimeSpan.FromMinutes(3);

    /// <summary>How long one read or write may keep answering MustRetry before the test calls it broken.</summary>
    private static readonly TimeSpan SettleBudget = TimeSpan.FromSeconds(20);

    private readonly RecordingLogProvider recorder = new();

    private readonly ILoggerFactory loggerFactory;

    public TestHashPoolFromCommittedMap(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true, recorder: recorder);
    }

    public void Dispose()
    {
        loggerFactory.Dispose();
        recorder.Dispose();
    }

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(path);

        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort cleanup.
        }
    }

    /// <summary>
    /// A node whose store and log survive a restart. The revision strings are fixed, not generated: an
    /// empty revision defaults to a fresh identifier per construction, which would reopen empty stores
    /// and make the restarted node a first boot in disguise.
    /// </summary>
    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath, int initialPartitions) => new()
    {
        NodeName = "hash-pool",
        TimerInitialDelay = TimeSpan.FromMilliseconds(50),
        InitialPartitions = initialPartitions,
        ReadIOThreads = 1,
        WriteIOThreads = 1,
        PartitionExecutorPoolSize = 1,
        Storage = "sqlite",
        StoragePath = storagePath,
        StorageRevision = "hash-pool",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "hash-pool-wal",
        WalSyncWrites = true
    };

    /// <summary>Keys in eight placement groups, so a pool larger than one moves most groups to another partition.</summary>
    private static string[] HashKeys(string suffix) =>
        [.. Enumerable.Range(0, 8).SelectMany(group => Enumerable.Range(0, 3).Select(row => $"pool:g{group}/{suffix}{row}"))];

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>
    /// Writes <paramref name="key"/> through the node's routed path. MustRetry is the documented
    /// no-durable-effect outcome, so it is retried within <see cref="SettleBudget"/>: a write that keeps
    /// answering it is routed to a partition nothing leads.
    /// </summary>
    private static async Task WriteAsync(IKahuna kahuna, string key, string value, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + (long)SettleBudget.TotalMilliseconds;
        KeyValueResponseType type;

        while (true)
        {
            (type, _, _) = await kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, V(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

            if (type == KeyValueResponseType.Set)
                return;

            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 > deadline)
                break;

            await Task.Delay(50, ct);
        }

        Assert.Fail($"The write of '{key}' did not land: {type}");
    }

    /// <summary>Reads <paramref name="key"/> through the node's routed path, with the same retry contract as <see cref="WriteAsync"/>.</summary>
    private static async Task<string> ReadAsync(IKahuna kahuna, string key, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + (long)SettleBudget.TotalMilliseconds;
        KeyValueResponseType type;

        while (true)
        {
            ReadOnlyKeyValueEntry? entry;
            (type, entry) = await kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            if (type == KeyValueResponseType.Get && entry?.Value is not null)
                return Encoding.UTF8.GetString(entry.Value);

            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 > deadline)
                break;

            await Task.Delay(50, ct);
        }

        Assert.Fail($"The read of '{key}' did not answer: {type}");
        return null!;
    }

    /// <summary>
    /// A persistent node bootstrapped with one pool size and restarted with another keeps routing over
    /// the pool its first boot created: the router, the published routing metadata, and the committed
    /// map agree on the bootstrap count, every row written before the restart reads back, and new rows
    /// land in every placement group.
    /// </summary>
    [Theory]
    [InlineData(1, 3)]
    [InlineData(3, 1)]
    [InlineData(3, 5)]
    public async Task RestartWithAnotherInitialPartitions_KeepsTheBootstrapPool(int bootstrapPool, int restartPool)
    {
        using CancellationTokenSource cts = new(RunDeadline);
        CancellationToken ct = cts.Token;

        string storagePath = CreateTempDir("kahuna-hash-pool-store-");
        string walPath = CreateTempDir("kahuna-hash-pool-wal-");

        string[] keys = HashKeys("row");

        try
        {
            await using (EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath, bootstrapPool), loggerFactory))
            {
                await node.StartAsync(ct);

                KahunaManager kahuna = (KahunaManager)node.Kahuna;

                // The first boot is what commits the configured value, so here both agree.
                Assert.Equal(bootstrapPool, kahuna.HashPoolSize);
                Assert.Equal(bootstrapPool, DataPartitionRouter.HashPoolSizeOf(node.Raft.GetPartitionMap()));

                foreach (string key in keys)
                    await WriteAsync(node.Kahuna, key, "before:" + key, ct);

                await node.Kahuna.FlushPersistenceAsync();
            }

            Assert.Empty(recorder.Containing("Hash pool:"));

            await using (EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath, restartPool), loggerFactory))
            {
                await node.StartAsync(ct);

                KahunaManager kahuna = (KahunaManager)node.Kahuna;

                // The committed map still holds the bootstrap partitions, and every reader of the pool follows it.
                Assert.Equal(bootstrapPool, DataPartitionRouter.HashPoolSizeOf(node.Raft.GetPartitionMap()));
                Assert.Equal(bootstrapPool, kahuna.HashPoolSize);
                Assert.Equal(bootstrapPool, new DataPartitionRouter(node.Raft).PoolSize);
                Assert.Equal(restartPool, node.Raft.Configuration.InitialPartitions);

                KahunaRoutingMetadataResponse metadata = await node.Kahuna.GetRoutingMetadata();
                Assert.Equal(bootstrapPool, metadata.HashPoolSize);
                Assert.Equal(bootstrapPool, metadata.Leaders.Count);

                // The operator learns about the ignored setting from the log, not from failing reads.
                (LogLevel Level, string Message) warning = Assert.Single(recorder.Containing("Hash pool:"));
                Assert.Equal(LogLevel.Warning, warning.Level);
                Assert.Contains($"bootstrapped with {bootstrapPool} data partition", warning.Message, StringComparison.Ordinal);
                Assert.Contains($"configured as {restartPool}", warning.Message, StringComparison.Ordinal);

                foreach (string key in keys)
                    Assert.Equal("before:" + key, await ReadAsync(node.Kahuna, key, ct));

                foreach (string key in HashKeys("new"))
                {
                    await WriteAsync(node.Kahuna, key, "after:" + key, ct);
                    Assert.Equal("after:" + key, await ReadAsync(node.Kahuna, key, ct));
                }
            }
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    /// <summary>
    /// A member of a running cluster that restarts with another configured value takes the pool from
    /// the map the others committed: it resolves every key to the same partition they do, serves the
    /// rows they hold, and takes writes.
    /// </summary>
    [Fact]
    public async Task ClusterMemberRestartedWithAnotherValue_RoutesLikeTheOthers()
    {
        using CancellationTokenSource cts = new(RunDeadline);
        CancellationToken ct = cts.Token;

        EmbeddedKahunaOptions options = new()
        {
            NodeName = "hash-pool-cluster",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 3,
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1
        };

        await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(3, options, loggerFactory, ct);

        string[] keys = HashKeys("row");

        foreach (string key in keys)
            await TestEmbeddedKahunaCluster.CommitWriteAsync(cluster.GetNode(0), key, "before:" + key, ct);

        const int restarted = 2;

        await cluster.StopNodeAsync(restarted);
        cluster.GetNodeOptions(restarted).InitialPartitions = 5;
        await cluster.RestartNodeAsync(restarted, ct);

        EmbeddedKahunaNode member = cluster.GetNode(restarted);
        EmbeddedKahunaNode peer = cluster.GetNode(0);

        Assert.Equal(5, member.Raft.Configuration.InitialPartitions);
        Assert.Equal(3, ((KahunaManager)member.Kahuna).HashPoolSize);
        Assert.Equal(3, ((KahunaManager)peer.Kahuna).HashPoolSize);

        DataPartitionRouter memberRouter = new(member.Raft);
        DataPartitionRouter peerRouter = new(peer.Raft);

        foreach (string key in keys)
        {
            Assert.Equal(peerRouter.Locate(key), memberRouter.Locate(key));
            Assert.InRange(memberRouter.Locate(key), 1, 3);
        }

        foreach (string key in keys)
            Assert.Equal("before:" + key, await ReadAsync(member.Kahuna, key, ct));

        foreach (string key in HashKeys("new"))
        {
            await TestEmbeddedKahunaCluster.CommitWriteAsync(member, key, "after:" + key, ct);
            Assert.Equal("after:" + key, await ReadAsync(peer.Kahuna, key, ct));
        }

        (LogLevel Level, string Message) warning = Assert.Single(recorder.Containing("Hash pool:"));
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Contains("bootstrapped with 3 data partition", warning.Message, StringComparison.Ordinal);
        Assert.Contains("configured as 5", warning.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A key-range split creates a partition above the pool. It joins the committed map as an unrouted
    /// partition, so the hash pool keeps its bootstrap size and hash keys keep their partitions.
    /// </summary>
    [Fact]
    public async Task SplitCreatedPartition_DoesNotJoinTheHashPool()
    {
        using CancellationTokenSource cts = new(RunDeadline);
        CancellationToken ct = cts.Token;

        const string space = "pool:ranged";
        const string splitKey = space + "/m";

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            NodeName = "hash-pool-split",
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 2,
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1
        }, loggerFactory);

        await node.StartAsync(ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        DataPartitionRouter router = new(node.Raft);
        string[] hashKeys = HashKeys("row");
        Dictionary<string, int> before = hashKeys.ToDictionary(key => key, router.Locate, StringComparer.Ordinal);

        Assert.True(await kahuna.RegisterKeyRangeAsync(space, ct));

        while (kahuna.GetRangeMap(space).KeySpaces.Count == 0 || kahuna.GetRangeMap(space).KeySpaces[0].Descriptors.Count != 1)
            await Task.Delay(25, ct);

        // The splitter refuses a split that would leave either half empty.
        foreach (string key in (string[])[space + "/a", space + "/b", space + "/p", space + "/q"])
            await WriteAsync(node.Kahuna, key, "v", ct);

        SplitOutcome outcome = default;
        for (int attempt = 0; attempt < 5; attempt++)
        {
            outcome = await kahuna.ForceSplitAtKeyAsync(space, splitKey, null, ct);

            if (outcome.Status is not (SplitStatus.CutoverFailed or SplitStatus.ConcurrentSplit))
                break;

            await Task.Delay(100, ct);
        }

        Assert.True(outcome.IsSuccess, $"split failed: {outcome.Status}");
        Assert.True(outcome.NewPartitionId > 2, "the split partition must be allocated above the pool");

        while (kahuna.GetRangeMap(space).KeySpaces[0].Descriptors.Count != 2)
            await Task.Delay(25, ct);

        IReadOnlyList<RaftPartitionRange> map = node.Raft.GetPartitionMap();

        Assert.Equal(3, map.Count);
        Assert.Contains(map, range => range.PartitionId == outcome.NewPartitionId && range.RoutingMode == RaftRoutingMode.Unrouted);
        Assert.Equal(2, DataPartitionRouter.HashPoolSizeOf(map));
        Assert.Equal(2, kahuna.HashPoolSize);

        KahunaRoutingMetadataResponse metadata = await node.Kahuna.GetRoutingMetadata();
        Assert.Equal(2, metadata.HashPoolSize);
        Assert.Contains(metadata.Leaders, leader => leader.PartitionId == outcome.NewPartitionId);

        foreach (string key in hashKeys)
            Assert.Equal(before[key], router.Locate(key));

        Assert.Empty(recorder.Containing("Hash pool:"));
    }

    /// <summary>The pool a committed map describes is its live hash-range entries: not the partitions splits
    /// created, and not a removed tombstone.</summary>
    [Fact]
    public void HashPoolSizeOf_CountsLiveHashRangePartitionsOnly()
    {
        RaftPartitionRange[] map =
        [
            new() { PartitionId = 1, RoutingMode = RaftRoutingMode.HashRange, State = RaftPartitionState.Active },
            new() { PartitionId = 2, RoutingMode = RaftRoutingMode.HashRange, State = RaftPartitionState.Active },
            new() { PartitionId = 3, RoutingMode = RaftRoutingMode.HashRange, State = RaftPartitionState.Active },
            new() { PartitionId = 4, RoutingMode = RaftRoutingMode.Unrouted, State = RaftPartitionState.Active },
            new() { PartitionId = 5, RoutingMode = RaftRoutingMode.Unrouted, State = RaftPartitionState.Splitting },
            new() { PartitionId = 6, RoutingMode = RaftRoutingMode.HashRange, State = RaftPartitionState.Removed }
        ];

        Assert.Equal(3, DataPartitionRouter.HashPoolSizeOf(map));
        Assert.Equal(0, DataPartitionRouter.HashPoolSizeOf([]));

        DataPartitionRouter fixedPool = new(3);
        Assert.Equal(3, fixedPool.PoolSize);
        Assert.InRange(fixedPool.Locate("pool:g1/row0"), 1, 3);
    }
}

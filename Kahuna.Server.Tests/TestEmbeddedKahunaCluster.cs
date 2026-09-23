using System.Diagnostics.Metrics;
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Routing;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// An in-memory embedded cluster runs several full nodes in one process. These tests drive it on one
/// thread, as the browser event loop does: every node writes and reads through the public entry
/// points, the leader of the meta partition is stopped and another node takes over, and the stopped
/// node restarts empty and catches up.
/// </summary>
public sealed class TestEmbeddedKahunaCluster
{
    private static readonly TimeSpan RunDeadline = TimeSpan.FromMinutes(3);

    private readonly ILoggerFactory loggerFactory;

    private readonly ITestOutputHelper output;

    public TestEmbeddedKahunaCluster(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    /// <summary>Marks a phase of a run in the test output, so a run that hits its deadline names the phase it was in.</summary>
    private void Phase(string name) => output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] {name}");

    internal static EmbeddedKahunaOptions ClusterOptions() => new()
    {
        NodeName = "cluster",
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 3,
        ReadIOThreads = 1,
        WriteIOThreads = 1,
        PartitionExecutorPoolSize = 1
    };

    [Fact]
    public async Task Cluster_ElectsLeaders_AndCommitsThroughEveryNode()
    {
        await SingleThreadedContext.RunAsync(async () =>
        {
            using CancellationTokenSource cts = new(RunDeadline);

            await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(3, ClusterOptions(), loggerFactory, cts.Token);

            Assert.Equal(3, cluster.NodeCount);
            Assert.Equal(3, new HashSet<string>(Enumerable.Range(0, 3).Select(cluster.GetEndpoint)).Count);

            for (int partitionId = 0; partitionId < cluster.PartitionCount; partitionId++)
                Assert.InRange(await cluster.GetLeaderIndexAsync(partitionId, cts.Token), 0, 2);

            for (int i = 0; i < cluster.NodeCount; i++)
            {
                string key = $"cluster/through-{i}";
                await CommitWriteAsync(cluster.GetNode(i), key, $"value-{i}", cts.Token);

                // Every node reads every committed value, whichever node leads the key's partition.
                for (int reader = 0; reader < cluster.NodeCount; reader++)
                    Assert.Equal($"value-{i}", await ReadAsync(cluster.GetNode(reader), key, cts.Token));
            }
        }, RunDeadline + TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task StoppingTheMetaLeader_FailsOver_AndTheRestartedNodeCatchesUp()
    {
        await SingleThreadedContext.RunAsync(async () =>
        {
            using CancellationTokenSource cts = new(RunDeadline);

            Phase("create cluster");
            await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(3, ClusterOptions(), loggerFactory, cts.Token);

            Phase("write failover/before");
            await CommitWriteAsync(cluster.GetNode(0), "failover/before", "before", cts.Token);

            int stopped = await cluster.GetLeaderIndexAsync(0, cts.Token);

            // Prefer a key whose partition the stopped node also leads, so the write below needs a
            // data-partition failover too, not only a meta-partition one.
            string key = await PickKeyLedByAsync(cluster, stopped, cts.Token);

            Phase($"stop node {stopped} (meta leader); key {key}");
            await cluster.StopNodeAsync(stopped);
            Assert.False(cluster.IsRunning(stopped));

            Phase("wait for the new meta leader");
            int newLeader = await cluster.GetLeaderIndexAsync(0, cts.Token);
            Assert.NotEqual(stopped, newLeader);

            int writer = (stopped + 1) % cluster.NodeCount;
            Phase($"write {key} through node {writer}");
            await CommitWriteAsync(cluster.GetNode(writer), key, "after", cts.Token);

            Phase($"restart node {stopped}");
            await cluster.RestartNodeAsync(stopped, cts.Token);
            Assert.True(cluster.IsRunning(stopped));

            EmbeddedKahunaNode restarted = cluster.GetNode(stopped);

            // The restarted node started with an empty log: the values below reach it only through
            // replication from the other members.
            Phase("read through the restarted node");
            Assert.Equal("after", await ReadAsync(restarted, key, cts.Token));
            Assert.Equal("before", await ReadAsync(restarted, "failover/before", cts.Token));
            Phase("wait for the restarted node's local replica");
            await WaitForLocalReplicaAsync(restarted, key, "after", cts.Token);

            // The restarted node takes writes again.
            Phase("write through the restarted node");
            await CommitWriteAsync(restarted, "failover/after-restart", "again", cts.Token);
            Phase("read the restarted node's write through the writer");
            Assert.Equal("again", await ReadAsync(cluster.GetNode(writer), "failover/after-restart", cts.Token));
            Phase("done");
        }, RunDeadline + TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task IsolatingTheLeader_LeavesTheMinorityUnableToCommit_AndTheMajorityElectsAndCommits()
    {
        await SingleThreadedContext.RunAsync(async () =>
        {
            using CancellationTokenSource cts = new(RunDeadline);
            CancellationToken ct = cts.Token;

            await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(3, ClusterOptions(), loggerFactory, ct);

            const string key = "partition/key";

            // The partition Kahuna routes the key to: data partitions start at 1, after the meta partition.
            int partitionId = 1 + HashPlacement.BucketOfKey(key, ClusterOptions().InitialPartitions);
            int isolated = await cluster.GetLeaderIndexAsync(partitionId, ct);

            await CommitWriteAsync(cluster.GetNode(isolated), key, "before", ct);

            cluster.IsolateNode(isolated);
            Assert.True(cluster.IsLinkBlocked(isolated, (isolated + 1) % cluster.NodeCount));
            Assert.True(cluster.IsLinkBlocked((isolated + 2) % cluster.NodeCount, isolated));

            // The isolated node holds a minority: its write reaches no quorum, so it never commits.
            using CancellationTokenSource minority = CancellationTokenSource.CreateLinkedTokenSource(ct);
            minority.CancelAfter(TimeSpan.FromSeconds(15));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                CommitWriteAsync(cluster.GetNode(isolated), key, "from-minority", minority.Token));

            // The majority elects a leader of its own and commits.
            int majorityLeader = await WaitForLeaderAmongAsync(cluster, partitionId, isolated, ct);
            await CommitWriteAsync(cluster.GetNode(majorityLeader), key, "from-majority", ct);

            // Healed, the isolated node rejoins and catches up.
            cluster.UnblockAllLinks();
            Assert.False(cluster.IsLinkBlocked(isolated, majorityLeader));

            Assert.Equal("from-majority", await ReadAsync(cluster.GetNode(isolated), key, ct));
            await WaitForLocalReplicaAsync(cluster.GetNode(isolated), key, "from-majority", ct);
        }, RunDeadline + TimeSpan.FromSeconds(30));
    }

    /// <summary>
    /// Waits for a leader of <paramref name="partitionId"/> among the members other than
    /// <paramref name="excluded"/>. A node cut from the majority keeps believing that it leads, so the
    /// cluster-wide lookup can still name it.
    /// </summary>
    private static async Task<int> WaitForLeaderAmongAsync(EmbeddedKahunaCluster cluster, int partitionId, int excluded, CancellationToken ct)
    {
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            for (int i = 0; i < cluster.NodeCount; i++)
            {
                if (i == excluded || !cluster.IsRunning(i))
                    continue;

                try
                {
                    if (await cluster.GetNode(i).Raft.AmILeader(partitionId, ct))
                        return i;
                }
                catch (Exception ex) when (ex is Kommander.RaftException or Kommander.PartitionNotHostedException)
                {
                }
            }

            await Task.Delay(50, ct);
        }
    }

    private static async Task<string> PickKeyLedByAsync(EmbeddedKahunaCluster cluster, int index, CancellationToken ct)
    {
        int dataPartitions = cluster.PartitionCount - 1;

        for (int i = 0; i < 64; i++)
        {
            string candidate = $"failover/key-{i}";

            // The partition Kahuna routes the key to: data partitions start at 1, after the meta partition.
            int partitionId = 1 + HashPlacement.BucketOfKey(candidate, dataPartitions);

            if (await cluster.GetLeaderIndexAsync(partitionId, ct) == index)
                return candidate;
        }

        return "failover/key-0";
    }

    /// <summary>
    /// Runs one interactive transaction (begin, write, commit) through <paramref name="node"/>, retrying
    /// while the cluster fails over: <c>MustRetry</c>, and a <see cref="KahunaServerException"/> from a
    /// forward to a node that just stopped, both mean "try again".
    /// </summary>
    internal static async Task CommitWriteAsync(EmbeddedKahunaNode node, string key, string value, CancellationToken ct)
    {
        string lastOutcome = "none";

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            try
            {
                (KeyValueResponseType started, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
                    new KeyValueTransactionOptions { Locking = KeyValueTransactionLocking.Pessimistic, Timeout = 5000 }, ct);

                if (started == KeyValueResponseType.Set)
                {
                    (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                        handle.TransactionId, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0,
                        KeyValueDurability.Persistent, ct, 0, handle.CoordinatorKey, TransactionOperationId.NewRandom());

                    if (set == KeyValueResponseType.Set)
                    {
                        (KeyValueResponseType committed, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
                        if (committed == KeyValueResponseType.Committed)
                            return;

                        lastOutcome = $"commit {committed}";
                    }
                    else
                    {
                        lastOutcome = $"write {set}";
                        await node.Kahuna.LocateAndRollbackTransaction(handle, ct);
                    }
                }
                else
                {
                    lastOutcome = $"begin {started}";
                }
            }
            catch (KahunaServerException ex)
            {
                lastOutcome = ex.Message;
            }

            if (ct.IsCancellationRequested)
                throw new TimeoutException($"The write of '{key}' did not commit; last outcome: {lastOutcome}");

            await Task.Delay(50, ct);
        }
    }

    /// <summary>Reads <paramref name="key"/> through <paramref name="node"/>, retrying until it exists.</summary>
    internal static async Task<string> ReadAsync(EmbeddedKahunaNode node, string key, CancellationToken ct)
    {
        while (true)
        {
            try
            {
                (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

                if (type == KeyValueResponseType.Get && entry?.Value is not null)
                    return Encoding.UTF8.GetString(entry.Value);
            }
            catch (KahunaServerException)
            {
                // A forward to a node that stopped in the meantime; the next attempt re-routes.
            }

            await Task.Delay(50, ct);
        }
    }

    /// <summary>
    /// Waits until the node's own replica holds <paramref name="expected"/> for <paramref name="key"/>:
    /// the value reached the node through replication, not through a forward to the leader.
    /// </summary>
    private static async Task WaitForLocalReplicaAsync(EmbeddedKahunaNode node, string key, string expected, CancellationToken ct)
    {
        while (true)
        {
            await node.Kahuna.FlushPersistenceAsync();

            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.TryGetValue(
                HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent);

            if (type == KeyValueResponseType.Get && entry?.Value is not null && Encoding.UTF8.GetString(entry.Value) == expected)
                return;

            await Task.Delay(50, ct);
        }
    }
}

/// <summary>
/// Counts Kommander's process-wide election counter, so it must not share the process with other
/// clusters: the collection disables parallelisation and runs on its own.
/// </summary>
[CollectionDefinition("ExclusiveElectionMeasurement", DisableParallelization = true)]
public sealed class ExclusiveElectionMeasurementCollection { }

/// <summary>
/// Three nodes on one thread share every heartbeat and election timer. A heartbeat that runs late
/// behind the other nodes' work looks like a dead leader and starts a needless election. With the
/// embedded default timings, leadership must stay stable for a minute under a light write load.
/// </summary>
[Collection("ExclusiveElectionMeasurement")]
public sealed class TestEmbeddedKahunaClusterStability
{
    private static readonly TimeSpan StableWindow = TimeSpan.FromSeconds(60);

    private readonly ITestOutputHelper output;

    private readonly ILoggerFactory loggerFactory;

    public TestEmbeddedKahunaClusterStability(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    [Fact]
    public async Task Leadership_StaysStable_ForAMinute_OnOneThread()
    {
        await SingleThreadedContext.RunAsync(async () =>
        {
            using CancellationTokenSource cts = new(StableWindow + TimeSpan.FromMinutes(2));

            await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(
                3, TestEmbeddedKahunaCluster.ClusterOptions(), loggerFactory, cts.Token);

            long elections = 0;
            double maxHeartbeatDelayMs = 0;

            using MeterListener listener = new();
            listener.InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == "Kommander" && instrument.Name is "raft.elections_started_total" or "raft.heartbeat_delay_ms")
                    meterListener.EnableMeasurementEvents(instrument);
            };
            listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref elections, value));
            listener.SetMeasurementEventCallback<double>((_, value, _, _) =>
            {
                lock (listener)
                    maxHeartbeatDelayMs = Math.Max(maxHeartbeatDelayMs, value);
            });
            listener.Start();

            DateTime end = DateTime.UtcNow + StableWindow;
            int writes = 0;

            while (DateTime.UtcNow < end)
            {
                EmbeddedKahunaNode node = cluster.GetNode(writes % cluster.NodeCount);
                await TestEmbeddedKahunaCluster.CommitWriteAsync(node, $"stable/key-{writes % 16}", $"v{writes}", cts.Token);
                writes++;
                await Task.Delay(250, cts.Token);
            }

            output.WriteLine($"writes={writes} elections={Interlocked.Read(ref elections)} maxHeartbeatDelayMs={maxHeartbeatDelayMs:F0}");

            Assert.Equal(0, Interlocked.Read(ref elections));
        }, StableWindow + TimeSpan.FromMinutes(3));
    }
}

using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Text;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Routing;
using Kommander;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// An embedded node given a snapshot staging directory stages a whole-partition snapshot it receives on disk,
/// rather than on the managed heap, and still installs it. The snapshot is exported from the partition leader and
/// handed to a follower's receive path in chunks, the way a leader's transfer delivers it.
/// </summary>
public sealed class TestEmbeddedSnapshotStaging : IDisposable
{
    private const int Partition = 1;

    private static readonly TimeSpan RunDeadline = TimeSpan.FromMinutes(2);

    private readonly ILoggerFactory loggerFactory;

    private readonly string stagingRoot = Path.Combine(Path.GetTempPath(), "kahuna-staging-" + Guid.NewGuid().ToString("N"));

    public TestEmbeddedSnapshotStaging(ITestOutputHelper output)
    {
        loggerFactory = TestLogFactory.Create(output, quietKommander: true);
    }

    public void Dispose()
    {
        try { Directory.Delete(stagingRoot, recursive: true); }
        catch (DirectoryNotFoundException) { }
    }


    [Fact]
    public Task AFollowerStagesAReceivedSnapshotOnDisk_AndInstallsIt() =>
        SingleThreadedContext.RunAsync(async () =>
        {
            using CancellationTokenSource cts = new(RunDeadline);
            await StageAndInstallAsync(cts.Token);
        }, RunDeadline + TimeSpan.FromSeconds(30));

    private async Task StageAndInstallAsync(CancellationToken ct)
    {
        EmbeddedKahunaOptions options = TestEmbeddedKahunaCluster.ClusterOptions();
        options.NodeName = "staging";
        options.Port = 7300;
        options.RaftSnapshotStagingDirectory = stagingRoot;
        // Every staged byte goes to disk from the first chunk.
        options.RaftSnapshotStagingMemoryBytes = 0;

        await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(3, options, loggerFactory, ct);

        int dataPartitions = cluster.PartitionCount - 1;
        // Keys are placed by their keyspace, so pick one keyspace that lands on the partition.
        string keySpace = "staging0";
        for (int i = 1; 1 + HashPlacement.BucketOfKeySpace(keySpace, dataPartitions) != Partition; i++)
            keySpace = $"staging{i}";

        List<string> keys = new(40);
        for (int i = 0; i < 40; i++)
            keys.Add($"{keySpace}/key-{i}");

        int leader = await cluster.GetLeaderIndexAsync(Partition, ct);
        int installee = (leader + 1) % cluster.NodeCount;
        EmbeddedKahunaNode leaderNode = cluster.GetNode(leader);
        EmbeddedKahunaNode installeeNode = cluster.GetNode(installee);

        foreach (string key in keys)
            await TestEmbeddedKahunaCluster.CommitWriteAsync(leaderNode, key, "staged-" + key, ct);

        // Nothing reaches the follower's consumer until the install, so the install is what seeds the rows.
        Assert.Equal(RaftOperationStatus.Success, await installeeNode.Raft.HoldConsumerAppliesForTesting(Partition, ct));

        long snapshotIndex;
        byte[] payload;
        try
        {
            snapshotIndex = await WaitForStableCommitIndex(leaderNode.Raft, ct);

            await using (Stream export = await ((KahunaManager)leaderNode.Kahuna).KeyValues.PartitionStateTransfer.ExportPartitionState(Partition, snapshotIndex, ct))
            {
                using MemoryStream buffer = new();
                await export.CopyToAsync(buffer, ct);
                payload = buffer.ToArray();
            }

            long term = leaderNode.Raft.GetPartitionTerm(Partition);
            string sessionId = Guid.NewGuid().ToString("N");
            string checksum = Convert.ToHexString(SHA256.HashData(payload));
            RaftManager receiver = (RaftManager)installeeNode.Raft;
            string installeeStaging = Path.Combine(stagingRoot, $"staging-{installee + 1}");

            using SpillCounter spills = new(Partition);

            const int Chunks = 3;
            int chunkSize = (payload.Length + Chunks - 1) / Chunks;
            SnapshotResponse? response = null;

            for (int chunk = 0; chunk < Chunks; chunk++)
            {
                int offset = chunk * chunkSize;
                bool last = chunk == Chunks - 1;

                response = await receiver.ReceiveInstallSnapshot(new SnapshotRequest
                {
                    SessionId = sessionId,
                    PartitionId = Partition,
                    SnapshotIndex = snapshotIndex,
                    LeaderTerm = term,
                    LastIncludedTerm = term,
                    LeaderEndpoint = cluster.GetEndpoint(leader),
                    FollowerEndpoint = cluster.GetEndpoint(installee),
                    ChunkIndex = chunk,
                    IsLast = last,
                    Data = payload.AsMemory(offset, last ? payload.Length - offset : chunkSize),
                    Kind = SnapshotKind.PartitionState,
                    SnapshotChecksum = checksum
                }, ct);

                if (!last)
                {
                    Assert.True(response.Success, $"chunk {chunk} was refused");

                    // The staged bytes are in a spill file inside this member's own staging directory.
                    Assert.NotEmpty(Directory.GetFiles(installeeStaging, "*.snapshot-staging"));
                }
            }

            Assert.True(response!.Success, "the follower refused the snapshot install");
            Assert.Equal(1, spills.Count);

            // The spill file goes with its session.
            Assert.Empty(Directory.GetFiles(installeeStaging, "*.snapshot-staging"));

            // Each member has a staging directory of its own under the one the cluster was given.
            for (int i = 0; i < cluster.NodeCount; i++)
                Assert.True(Directory.Exists(Path.Combine(stagingRoot, $"staging-{i + 1}")), $"member {i} has no staging directory of its own");
        }
        finally
        {
            await installeeNode.Raft.ResumeConsumerAppliesForTesting(Partition, ct);
        }

        // The install seeded the rows on the follower's own replica.
        KahunaManager installed = (KahunaManager)installeeNode.Kahuna;
        foreach (string key in keys)
        {
            byte[] expected = Encoding.UTF8.GetBytes("staged-" + key);
            await WaitUntilAsync(
                () => installed.PersistenceBackend.GetKeyValue(key) is { Value: { } value } && value.AsSpan().SequenceEqual(expected),
                ct);
        }
    }

    private static async Task<long> WaitForStableCommitIndex(IRaft raft, CancellationToken ct)
    {
        long previous = -1;
        while (true)
        {
            long current = raft.GetCommitIndex(Partition);
            if (current == previous)
                return current;

            previous = current;
            await Task.Delay(200, ct);
        }
    }

    private static async Task WaitUntilAsync(Func<bool> condition, CancellationToken ct)
    {
        while (!condition())
            await Task.Delay(50, ct);
    }

    /// <summary>Counts Kommander's spill events for one partition while it is alive.</summary>
    private sealed class SpillCounter : IDisposable
    {
        private readonly MeterListener listener = new();

        private int count;

        public SpillCounter(int partitionId)
        {
            listener.InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == KommanderMetrics.MeterName && instrument.Name == "raft.snapshot.receive_sessions_spilled_total")
                    meterListener.EnableMeasurementEvents(instrument);
            };

            listener.SetMeasurementEventCallback<long>((_, measurement, tags, _) =>
            {
                foreach (KeyValuePair<string, object?> tag in tags)
                {
                    if (tag.Key == "partition_id" && tag.Value is int id && id == partitionId)
                        Interlocked.Add(ref count, (int)measurement);
                }
            });

            listener.Start();
        }

        public int Count => Volatile.Read(ref count);

        public void Dispose() => listener.Dispose();
    }
}

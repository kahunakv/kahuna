using System.Text;

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Routing;

using Kommander.Time;

using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Three nodes on the in-memory transports. The leader of a partition is killed: its links to the other nodes are
/// blocked, and in one variant the node is also stopped. A survivor still routes to the killed node until the
/// partition elects a new leader. In that window each forwarded call gets the answer that the gRPC transport gives
/// for an unreachable peer, so a caller that retries on <see cref="KeyValueResponseType.MustRetry"/> gets through,
/// and no call throws.
/// </summary>
public sealed class TestKilledLeaderOnTheMemoryTransport
{
    private static readonly TimeSpan RunDeadline = TimeSpan.FromMinutes(3);

    private static readonly TimeSpan RetryPause = TimeSpan.FromMilliseconds(50);

    private readonly ILoggerFactory loggerFactory;

    private readonly ITestOutputHelper output;

    public TestKilledLeaderOnTheMemoryTransport(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    /// <summary>
    /// A transaction start through a survivor, for a coordinator partition that the killed node led, answers
    /// <see cref="KeyValueResponseType.MustRetry"/> while the survivor still routes to the killed node, then starts
    /// the session once the partition has a new leader.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StartThroughASurvivor_ReturnsMustRetryUntilTheNewLeader_ThenStarts(bool stopTheNode)
    {
        using CancellationTokenSource cts = new(RunDeadline);
        CancellationToken ct = cts.Token;

        await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(3, TestEmbeddedKahunaCluster.ClusterOptions(), loggerFactory, ct);

        const int coordinatorPartition = 1;
        int killed = await cluster.GetLeaderIndexAsync(coordinatorPartition, ct);
        int survivor = (killed + 1) % cluster.NodeCount;

        await KillAsync(cluster, killed, stopTheNode);

        List<string> answers = [];
        KeyValueResponseType started;
        TransactionHandle handle;

        while (true)
        {
            try
            {
                (started, handle) = await cluster.GetNode(survivor).Kahuna.LocateAndStartTransaction(
                    new KeyValueTransactionOptions { CoordinatorPartitionId = coordinatorPartition, Timeout = 30_000 }, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Assert.Fail($"The start threw after {answers.Count} answers ({Summarize(answers)}): {ex}");
                throw;
            }

            answers.Add(started.ToString());

            if (started != KeyValueResponseType.MustRetry)
                break;

            await Task.Delay(RetryPause, ct);
        }

        output.WriteLine($"Killed node {killed}; answers through node {survivor}: {Summarize(answers)}");

        Assert.Equal(KeyValueResponseType.Set, started);
        Assert.False(handle.IsEmpty);

        // The first attempt was made before the survivor could see a new leader, so it was routed to the killed node.
        Assert.Equal(nameof(KeyValueResponseType.MustRetry), answers[0]);

        Assert.Equal(KeyValueResponseType.RolledBack, await cluster.GetNode(survivor).Kahuna.LocateAndRollbackTransaction(handle, ct));
    }

    /// <summary>
    /// A commit is answered, then its coordinator is killed. A retry of the commit through a survivor, while the
    /// survivor still routes to the killed coordinator, answers <see cref="KeyValueResponseType.MustRetry"/> with
    /// <see cref="CommitRefusalClass.Unresolved"/> and the caller's record anchor. A retry after the coordinator
    /// partition elects a new leader reads the transaction record and answers
    /// <see cref="KeyValueResponseType.Committed"/> with the original commit timestamp.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CommitRetriedAfterItsCoordinatorWasKilled_IsUnresolvedThenCommittedWithTheOriginalTimestamp(bool stopTheNode)
    {
        using CancellationTokenSource cts = new(RunDeadline);
        CancellationToken ct = cts.Token;

        await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(3, TestEmbeddedKahunaCluster.ClusterOptions(), loggerFactory, ct);

        const int coordinatorPartition = 1;
        int coordinator = await cluster.GetLeaderIndexAsync(coordinatorPartition, ct);
        int survivor = (coordinator + 1) % cluster.NodeCount;
        IKahuna kahuna = cluster.GetNode(survivor).Kahuna;

        // The record lives on the partition of the key. A partition that a survivor leads keeps the record readable
        // while the coordinator partition elects.
        string key = await PickKeyNotLedByAsync(cluster, coordinator, ct);

        (KeyValueResponseType started, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorPartitionId = coordinatorPartition,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 30_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, started);

        (KeyValueResponseType set, _, _) = await kahuna.LocateAndTrySetKeyValue(
            handle.TransactionId, key, Encoding.UTF8.GetBytes("committed"), null, -1, KeyValueFlags.Set, 0,
            KeyValueDurability.Persistent, ct, 0, handle.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.Equal(KeyValueResponseType.Set, set);

        TransactionCommitAnswer first = await kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, first.Type);
        Assert.NotNull(first.RecordAnchorKey);
        Assert.NotEqual(HLCTimestamp.Zero, first.CommitTimestamp);

        TransactionHandle retried = handle with { RecordAnchorKey = first.RecordAnchorKey };

        await KillAsync(cluster, coordinator, stopTheNode);

        List<TransactionCommitAnswer> answers = [];
        TransactionCommitAnswer answer;

        while (true)
        {
            try
            {
                answer = await kahuna.LocateAndCommitTransaction(retried, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                Assert.Fail($"The commit retry threw after {answers.Count} answers ({Summarize(answers)}): {ex}");
                throw;
            }

            answers.Add(answer);

            if (answer.Type != KeyValueResponseType.MustRetry)
                break;

            await Task.Delay(RetryPause, ct);
        }

        output.WriteLine($"Killed coordinator {coordinator}; answers through node {survivor}: {Summarize(answers)}");

        // The first retry was made before the survivor could see a new leader, so it was routed to the killed node.
        TransactionCommitAnswer unanswered = answers[0];
        Assert.Equal(KeyValueResponseType.MustRetry, unanswered.Type);
        Assert.Equal(CommitRefusalClass.Unresolved, unanswered.Refusal);
        Assert.Equal(first.RecordAnchorKey, unanswered.RecordAnchorKey);

        Assert.Equal(KeyValueResponseType.Committed, answer.Type);
        Assert.Equal(first.CommitTimestamp, answer.CommitTimestamp);
        Assert.Equal(first.RecordAnchorKey, answer.RecordAnchorKey);

        Assert.Equal("committed", await TestEmbeddedKahunaCluster.ReadAsync(cluster.GetNode(survivor), key, ct));
    }

    /// <summary>Each distinct answer in order of its first appearance, with the number of times it was given.</summary>
    private static string Summarize<T>(List<T> answers) =>
        string.Join("; ", answers.GroupBy(static a => a).Select(static g => $"{g.Count()} x {g.Key}"));

    /// <summary>
    /// Kills the member at <paramref name="index"/>: its links to every other member are blocked, so code that still
    /// runs on it cannot reach them. With <paramref name="stopTheNode"/> the member is also stopped, and the Kahuna
    /// transport no longer knows it.
    /// </summary>
    private static async Task KillAsync(EmbeddedKahunaCluster cluster, int index, bool stopTheNode)
    {
        cluster.IsolateNode(index);

        if (stopTheNode)
            await cluster.StopNodeAsync(index);
    }

    /// <summary>A key whose data partition a member other than <paramref name="excluded"/> leads, when one exists.</summary>
    private static async Task<string> PickKeyNotLedByAsync(EmbeddedKahunaCluster cluster, int excluded, CancellationToken ct)
    {
        int partitions = TestEmbeddedKahunaCluster.ClusterOptions().InitialPartitions;

        for (int i = 0; i < 64; i++)
        {
            string candidate = $"killed-leader/key-{i}";

            // Data partitions start at 1, after the meta partition.
            int partitionId = 1 + HashPlacement.BucketOfKey(candidate, partitions);

            if (await cluster.GetLeaderIndexAsync(partitionId, ct) != excluded)
                return candidate;
        }

        return "killed-leader/key-0";
    }
}

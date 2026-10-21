
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A member that restarts while its cluster has no leader must keep waiting for one, not fail its
/// start: Kommander bounds each election round by 10 s, and a node that gave up on that round exited
/// its process, taking with it the vote the surviving majority needed. Persistent members, so a
/// restarted one replays its own log and rejoins with its partition map, the way a restarted process
/// does; an empty member could not even learn the map without a leader.
/// </summary>
public sealed class TestEmbeddedKahunaClusterRestartWithoutLeader : IDisposable
{
    private static readonly TimeSpan RunDeadline = TimeSpan.FromMinutes(3);

    /// <summary>Longer than Kommander's 10 s election budget, so one undecided round is certain to pass.</summary>
    private static readonly TimeSpan PastOneElectionBudget = TimeSpan.FromSeconds(12);

    private readonly RecordingLogProvider recorder = new();

    private readonly ILoggerFactory loggerFactory;

    private readonly ITestOutputHelper output;

    private readonly string tempRoot =
        Path.Combine(Path.GetTempPath(), "kahuna_leaderless_" + Guid.NewGuid().ToString("N"));

    public TestEmbeddedKahunaClusterRestartWithoutLeader(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true, recorder: recorder);
    }

    public void Dispose()
    {
        loggerFactory.Dispose();

        if (Directory.Exists(tempRoot))
            Directory.Delete(tempRoot, recursive: true);
    }

    private void Phase(string name) => output.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] {name}");

    private EmbeddedKahunaOptions PersistentOptions() => new()
    {
        NodeName = "leaderless",
        Storage = "sqlite",
        StoragePath = Path.Combine(tempRoot, "store"),
        WalStorage = "sqlite",
        WalPath = Path.Combine(tempRoot, "wal"),
        InitialPartitions = 2,
        ReadIOThreads = 1,
        WriteIOThreads = 1,
        PartitionExecutorPoolSize = 1
    };

    /// <summary>
    /// Every peer is down when the member restarts: it has no quorum to elect with, waits past the
    /// election budget, logging the wait, and finishes its start once a peer returns.
    /// </summary>
    [Fact]
    public async Task RestartedWithEveryPeerDown_WaitsForALeader_AndFinishesWhenAPeerReturns()
    {
        using CancellationTokenSource cts = new(RunDeadline);
        CancellationToken ct = cts.Token;

        Phase("create cluster");
        await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(3, PersistentOptions(), loggerFactory, ct);
        await TestEmbeddedKahunaCluster.CommitWriteAsync(cluster.GetNode(0), "leaderless/before", "before", ct);

        Phase("stop every member");
        for (int i = 0; i < cluster.NodeCount; i++)
            await cluster.StopNodeAsync(i);

        Phase("restart node 0 alone");
        Task restartAlone = cluster.RestartNodeAsync(0, ct);

        await WaitForUndecidedRoundAsync(ct);
        Assert.False(restartAlone.IsCompleted, "the start must keep waiting for a leader, not finish or fail");

        Phase("restart node 1: a quorum is possible again");
        Task restartPeer = cluster.RestartNodeAsync(1, ct);

        await Task.WhenAll(restartAlone, restartPeer);

        Phase("the restarted members serve reads and writes");
        Assert.Equal("before", await TestEmbeddedKahunaCluster.ReadAsync(cluster.GetNode(0), "leaderless/before", ct));
        await TestEmbeddedKahunaCluster.CommitWriteAsync(cluster.GetNode(0), "leaderless/after", "after", ct);
        Assert.Equal("after", await TestEmbeddedKahunaCluster.ReadAsync(cluster.GetNode(1), "leaderless/after", ct));
        Phase("done");
    }

    /// <summary>
    /// The shape of the outage: one peer is down and the restarting member cannot reach the other,
    /// so no partition can elect while it starts. It waits past the election budget; once the link
    /// heals the remaining majority (the member and its reachable peer) elects, and the start finishes.
    /// </summary>
    [Fact]
    public async Task RestartedWhileNoPartitionCanElect_FinishesOnceTheMajorityElects()
    {
        using CancellationTokenSource cts = new(RunDeadline);
        CancellationToken ct = cts.Token;

        Phase("create cluster");
        await using EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(3, PersistentOptions(), loggerFactory, ct);
        await TestEmbeddedKahunaCluster.CommitWriteAsync(cluster.GetNode(0), "leaderless/before", "before", ct);

        Phase("stop node 1, stop node 0, cut node 0 from node 2");
        await cluster.StopNodeAsync(1);
        await cluster.StopNodeAsync(0);

        // Link blocks outlive a stop and a restart, so the member comes back unable to reach its only
        // running peer: with node 1 down nobody has a quorum.
        cluster.BlockLinkBothWays(0, 2);

        Phase("restart node 0 while nothing can elect");
        Task restart = cluster.RestartNodeAsync(0, ct);

        await WaitForUndecidedRoundAsync(ct);
        Assert.False(restart.IsCompleted, "the start must keep waiting for a leader, not finish or fail");

        Phase("heal the link: nodes 0 and 2 are a majority");
        cluster.UnblockLinkBothWays(0, 2);

        await restart;

        Phase("the restarted member serves reads and writes");
        Assert.Equal("before", await TestEmbeddedKahunaCluster.ReadAsync(cluster.GetNode(0), "leaderless/before", ct));
        await TestEmbeddedKahunaCluster.CommitWriteAsync(cluster.GetNode(0), "leaderless/after", "after", ct);
        Assert.Equal("after", await TestEmbeddedKahunaCluster.ReadAsync(cluster.GetNode(2), "leaderless/after", ct));
        Phase("done");
    }

    /// <summary>Disposing a member whose start is still waiting for a leader ends that start as cancelled.</summary>
    [Fact]
    public async Task DisposingAMemberWhoseStartIsWaiting_EndsTheStart()
    {
        using CancellationTokenSource cts = new(RunDeadline);
        CancellationToken ct = cts.Token;

        Phase("create cluster");
        EmbeddedKahunaCluster cluster = await EmbeddedKahunaCluster.CreateInMemoryAsync(3, PersistentOptions(), loggerFactory, ct);

        Phase("stop every member, restart node 0 alone");
        for (int i = 0; i < cluster.NodeCount; i++)
            await cluster.StopNodeAsync(i);

        Task restartAlone = cluster.RestartNodeAsync(0, ct);
        await WaitForUndecidedRoundAsync(ct);
        Assert.False(restartAlone.IsCompleted);

        Phase("dispose the cluster while the start waits");
        await cluster.DisposeAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => restartAlone);
        Phase("done");
    }

    /// <summary>
    /// Waits until the restarting member has logged an undecided election round (which Kommander reports
    /// after its 10 s budget), so the assertions that follow stand on the wait having actually been retried.
    /// </summary>
    private async Task WaitForUndecidedRoundAsync(CancellationToken ct)
    {
        using CancellationTokenSource bound = CancellationTokenSource.CreateLinkedTokenSource(ct);
        bound.CancelAfter(PastOneElectionBudget * 3);

        while (recorder.Containing("still waiting for a leader of partition").Count == 0)
            await Task.Delay(100, bound.Token);

        Phase("an undecided round was logged");
    }
}

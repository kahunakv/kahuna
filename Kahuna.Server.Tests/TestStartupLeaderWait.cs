
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The start of an embedded node keeps waiting for a partition leader across Kommander's bounded election
/// rounds. Each outcome of a round is scripted here; the cluster shapes are in
/// <see cref="TestEmbeddedKahunaClusterRestartWithoutLeader"/>.
/// </summary>
public sealed class TestStartupLeaderWait : IDisposable
{
    private readonly RecordingLogProvider recorder = new();

    private readonly ILoggerFactory loggerFactory;

    public TestStartupLeaderWait(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, recorder: recorder);
    }

    public void Dispose() => loggerFactory.Dispose();

    private ILogger Logger => loggerFactory.CreateLogger<IKahuna>();

    private static readonly RaftException Undecided = new("Leader couldn't be found or is not decided");

    [Fact]
    public async Task UndecidedRounds_AreRetried_UntilALeaderAppears()
    {
        int round = 0;

        string leader = await StartupLeaderWait.WaitAsync(
            _ =>
            {
                round++;
                return round switch
                {
                    1 => throw new RaftNodeNotReadyException("Cannot resolve leader for partition 1: node has not completed cluster initialization"),
                    2 => throw Undecided,
                    3 => throw Undecided,
                    _ => ValueTask.FromResult("localhost:7002")
                };
            },
            partitionId: 1,
            Timeout.InfiniteTimeSpan,
            Logger,
            TestContext.Current.CancellationToken);

        Assert.Equal("localhost:7002", leader);
        Assert.Equal(4, round);

        // One warning per undecided round, each naming the partition, then the late leader.
        Assert.Equal(3, recorder.Containing("still waiting for a leader of partition 1").Count);
        Assert.Single(recorder.Containing("not completed cluster initialization"));
    }

    [Fact]
    public async Task AFaultedRestore_IsNotRetried()
    {
        int round = 0;
        RaftException restoreFailed = new("Partition 1 restore failed", new IOException("disk gone"));

        RaftException thrown = await Assert.ThrowsAsync<RaftException>(async () =>
            await StartupLeaderWait.WaitAsync(
                _ => { round++; throw restoreFailed; },
                partitionId: 1,
                Timeout.InfiniteTimeSpan,
                Logger,
                TestContext.Current.CancellationToken));

        Assert.Same(restoreFailed, thrown);
        Assert.Equal(1, round);
        Assert.Empty(recorder.Containing("still waiting"));
    }

    [Fact]
    public async Task APartitionNoLongerHosted_IsNotRetried()
    {
        int round = 0;

        await Assert.ThrowsAsync<PartitionNotHostedException>(async () =>
            await StartupLeaderWait.WaitAsync(
                _ => { round++; throw new PartitionNotHostedException(2); },
                partitionId: 2,
                Timeout.InfiniteTimeSpan,
                Logger,
                TestContext.Current.CancellationToken));

        Assert.Equal(1, round);
    }

    [Fact]
    public async Task Cancellation_EndsTheWait()
    {
        using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        int round = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await StartupLeaderWait.WaitAsync(
                _ =>
                {
                    if (++round == 2)
                        cts.Cancel();

                    throw Undecided;
                },
                partitionId: 1,
                Timeout.InfiniteTimeSpan,
                Logger,
                cts.Token));

        Assert.Equal(2, round);
    }

    [Fact]
    public async Task AFiniteBudget_FailsTheStart_WithTheLastUndecidedRoundAsCause()
    {
        RaftException thrown = await Assert.ThrowsAsync<RaftException>(async () =>
            await StartupLeaderWait.WaitAsync(
                async token =>
                {
                    await Task.Delay(20, token);
                    throw Undecided;
                },
                partitionId: 3,
                TimeSpan.FromMilliseconds(1),
                Logger,
                TestContext.Current.CancellationToken));

        Assert.Same(Undecided, thrown.InnerException);
        Assert.Contains("partition 3", thrown.Message);
        Assert.Contains("budget", thrown.Message);
    }

    [Fact]
    public void OnlyTheUndecidedOutcomes_AreRetryable()
    {
        Assert.True(StartupLeaderWait.IsUndecided(Undecided));
        Assert.True(StartupLeaderWait.IsUndecided(new RaftNodeNotReadyException("assembling")));
        Assert.False(StartupLeaderWait.IsUndecided(new RaftException("Partition 1 restore failed", new IOException())));
        Assert.False(StartupLeaderWait.IsUndecided(new PartitionNotHostedException(1)));
    }
}


using System.Diagnostics;
using Kahuna.Server.KeyValues.Transactions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The replica fence's per-replica lag breaker (<see cref="ReplicaFenceLagTracker"/>): a replica that keeps
/// failing to attest within the apply wait is asked without the wait until a periodic full-budget probe sees it
/// attest again. The clock is driven explicitly in stopwatch ticks.
/// </summary>
public sealed class TestReplicaFenceLagTracker
{
    private const int FullWaitMs = 400;

    private const int FullBudgetMs = 1500;

    private const int LaggingBudgetMs = 100;

    private static readonly TimeSpan Probe = TimeSpan.FromSeconds(1);

    private static long Ticks(double seconds) => (long)(seconds * Stopwatch.Frequency);

    private static ReplicaFenceLagTracker Build() => new(FullWaitMs, FullBudgetMs, LaggingBudgetMs, Probe);

    [Fact]
    public void HealthyReplica_IsAskedWithTheFullWait_AndScored()
    {
        ReplicaFenceLagTracker tracker = Build();

        ReplicaFenceLagTracker.AskPlan plan = tracker.Plan("n2", Ticks(0));

        Assert.Equal(FullWaitMs, plan.WaitMs);
        Assert.Equal(FullBudgetMs, plan.CallBudgetMs);
        Assert.True(plan.Score);
        Assert.False(plan.IsProbe);
        Assert.False(plan.Lagging);
        Assert.False(tracker.IsLagging("n2"));
        Assert.Equal(0, tracker.LaggingCount);
    }

    [Fact]
    public void ConsecutiveNonAttestingRounds_MarkTheReplicaLagging_AttestationResetsTheStreak()
    {
        ReplicaFenceLagTracker tracker = Build();

        // Two strikes, then an attestation: the streak resets and the replica stays healthy.
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(0), out _));
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(0.1), out _));
        Assert.Null(tracker.Observe("n2", attested: true, Ticks(0.2), out _));
        Assert.False(tracker.IsLagging("n2"));

        // The threshold of consecutive strikes trips the breaker exactly once.
        for (int i = 1; i < ReplicaFenceLagTracker.LaggingThreshold; i++)
            Assert.Null(tracker.Observe("n2", attested: false, Ticks(1 + i * 0.1), out _));

        Assert.True(tracker.Observe("n2", attested: false, Ticks(2), out _));
        Assert.True(tracker.IsLagging("n2"));
        Assert.Equal(1, tracker.LaggingCount);

        // Further strikes while lagging change nothing (and do not double-count the gauge).
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(2.5), out _));
        Assert.Equal(1, tracker.LaggingCount);
    }

    [Fact]
    public void LaggingReplica_IsAskedWithoutTheWait_AndProbedOncePerInterval()
    {
        ReplicaFenceLagTracker tracker = Build();

        for (int i = 0; i < ReplicaFenceLagTracker.LaggingThreshold; i++)
            tracker.Observe("n2", attested: false, Ticks(i * 0.1), out _);

        Assert.True(tracker.IsLagging("n2"));

        // Inside the probe interval every ask is a zero-wait, short-budget, unscored ask.
        ReplicaFenceLagTracker.AskPlan noWait = tracker.Plan("n2", Ticks(0.5));
        Assert.Equal(0, noWait.WaitMs);
        Assert.Equal(LaggingBudgetMs, noWait.CallBudgetMs);
        Assert.False(noWait.Score);
        Assert.False(noWait.IsProbe);
        Assert.True(noWait.Lagging);

        // Once the interval elapses exactly one ask is the full-budget probe; the next one is zero-wait again.
        ReplicaFenceLagTracker.AskPlan probe = tracker.Plan("n2", Ticks(1.5));
        Assert.True(probe.IsProbe);
        Assert.True(probe.Score);
        Assert.Equal(FullWaitMs, probe.WaitMs);
        Assert.Equal(FullBudgetMs, probe.CallBudgetMs);

        ReplicaFenceLagTracker.AskPlan afterProbe = tracker.Plan("n2", Ticks(1.5));
        Assert.False(afterProbe.IsProbe);
        Assert.Equal(0, afterProbe.WaitMs);

        // A probe that still fails keeps the replica lagging; the next probe is another interval away.
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(1.6), out _));
        Assert.False(tracker.Plan("n2", Ticks(2.0)).IsProbe);
        Assert.True(tracker.Plan("n2", Ticks(2.6)).IsProbe);
    }

    [Fact]
    public void ProbeThatAttests_RestoresTheReplica_AndReportsTheEpisodeLength()
    {
        ReplicaFenceLagTracker tracker = Build();

        for (int i = 0; i < ReplicaFenceLagTracker.LaggingThreshold; i++)
            tracker.Observe("n2", attested: false, Ticks(1), out _);

        Assert.True(tracker.IsLagging("n2"));

        Assert.True(tracker.Plan("n2", Ticks(2.5)).IsProbe);
        Assert.False(tracker.Observe("n2", attested: true, Ticks(31), out double laggingMs));

        Assert.False(tracker.IsLagging("n2"));
        Assert.Equal(0, tracker.LaggingCount);
        Assert.InRange(laggingMs, 29_000, 31_000);

        // Healthy again: full-wait asks, and a fresh streak is needed to trip the breaker.
        Assert.Equal(FullWaitMs, tracker.Plan("n2", Ticks(32)).WaitMs);
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(33), out _));
        Assert.False(tracker.IsLagging("n2"));
    }

    [Fact]
    public void ReplicasAreTrackedIndependently()
    {
        ReplicaFenceLagTracker tracker = Build();

        for (int i = 0; i < ReplicaFenceLagTracker.LaggingThreshold; i++)
            tracker.Observe("n2", attested: false, Ticks(1), out _);

        Assert.True(tracker.IsLagging("n2"));
        Assert.False(tracker.IsLagging("n3"));
        Assert.Equal(FullWaitMs, tracker.Plan("n3", Ticks(1.5)).WaitMs);
        Assert.Equal(0, tracker.Plan("n2", Ticks(1.5)).WaitMs);
        Assert.Equal(1, tracker.LaggingCount);
    }
}

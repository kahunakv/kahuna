using System.Diagnostics;
using Kahuna.Server.KeyValues.Transactions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The replica fence's per-replica lag breaker (<see cref="ReplicaFenceLagTracker"/>): a replica that keeps
/// failing to attest within the apply wait is asked without the wait until enough consecutive full-budget
/// probes see it attest again. The clock is driven explicitly in stopwatch ticks.
/// </summary>
public sealed class TestReplicaFenceLagTracker
{
    private const int FullWaitMs = 400;

    private const int FullBudgetMs = 1500;

    private const int LaggingBudgetMs = 100;

    private static readonly TimeSpan Probe = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan Relapse = TimeSpan.FromSeconds(30);

    private static long Ticks(double seconds) => (long)(seconds * Stopwatch.Frequency);

    private static ReplicaFenceLagTracker Build() => new(FullWaitMs, FullBudgetMs, LaggingBudgetMs, Probe, Relapse);

    /// <summary>Trips the breaker for <paramref name="endpoint"/> at <paramref name="atSeconds"/>.</summary>
    private static void Trip(ReplicaFenceLagTracker tracker, string endpoint, double atSeconds)
    {
        for (int i = 1; i < ReplicaFenceLagTracker.LaggingThreshold; i++)
            Assert.Null(tracker.Observe(endpoint, attested: false, Ticks(atSeconds), isProbe: false, out _));

        Assert.True(tracker.Observe(endpoint, attested: false, Ticks(atSeconds), isProbe: false, out _));
        Assert.True(tracker.IsLagging(endpoint));
    }

    /// <summary>Plans and answers one attesting probe per interval starting at <paramref name="fromSeconds"/>,
    /// returning the recovery transition of the last one (null while still lagging).</summary>
    private static bool? AttestProbes(ReplicaFenceLagTracker tracker, string endpoint, double fromSeconds, int count, out double laggingMs)
    {
        laggingMs = 0;
        bool? last = null;

        for (int i = 0; i < count; i++)
        {
            double at = fromSeconds + i * Probe.TotalSeconds;
            ReplicaFenceLagTracker.AskPlan plan = tracker.Plan(endpoint, Ticks(at));
            Assert.True(plan.IsProbe, $"ask {i} at {at}s should have been a probe");
            last = tracker.Observe(endpoint, attested: true, Ticks(at), isProbe: true, out laggingMs);
        }

        return last;
    }

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
        Assert.Equal(ReplicaFenceLagTracker.RecoveryThreshold, tracker.RequiredRecoveryStreak("n2"));
    }

    [Fact]
    public void ConsecutiveNonAttestingRounds_MarkTheReplicaLagging_AttestationResetsTheStreak()
    {
        ReplicaFenceLagTracker tracker = Build();

        // Two strikes, then an attestation: the streak resets and the replica stays healthy.
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(0), isProbe: false, out _));
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(0.1), isProbe: false, out _));
        Assert.Null(tracker.Observe("n2", attested: true, Ticks(0.2), isProbe: false, out _));
        Assert.False(tracker.IsLagging("n2"));

        // The threshold of consecutive strikes trips the breaker exactly once.
        for (int i = 1; i < ReplicaFenceLagTracker.LaggingThreshold; i++)
            Assert.Null(tracker.Observe("n2", attested: false, Ticks(1 + i * 0.1), isProbe: false, out _));

        Assert.True(tracker.Observe("n2", attested: false, Ticks(2), isProbe: false, out _));
        Assert.True(tracker.IsLagging("n2"));
        Assert.Equal(1, tracker.LaggingCount);

        // Further strikes while lagging change nothing (and do not double-count the gauge).
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(2.5), isProbe: false, out _));
        Assert.Equal(1, tracker.LaggingCount);
    }

    [Fact]
    public void LaggingReplica_IsAskedWithoutTheWait_AndProbedOncePerInterval()
    {
        ReplicaFenceLagTracker tracker = Build();
        Trip(tracker, "n2", 0);

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
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(1.6), isProbe: true, out _));
        Assert.False(tracker.Plan("n2", Ticks(2.0)).IsProbe);
        Assert.True(tracker.Plan("n2", Ticks(2.6)).IsProbe);
    }

    [Fact]
    public void Recovery_NeedsConsecutiveAttestingProbes_AndReportsTheEpisodeLength()
    {
        ReplicaFenceLagTracker tracker = Build();
        Trip(tracker, "n2", 1);

        // One probe short of the threshold: still lagging.
        Assert.Null(AttestProbes(tracker, "n2", 2.5, ReplicaFenceLagTracker.RecoveryThreshold - 1, out _));
        Assert.True(tracker.IsLagging("n2"));
        Assert.Equal(1, tracker.LaggingCount);

        // The last consecutive probe restores the replica; the episode spans from the trip to this probe.
        double lastProbeAt = 2.5 + (ReplicaFenceLagTracker.RecoveryThreshold - 1) * Probe.TotalSeconds;
        ReplicaFenceLagTracker.AskPlan plan = tracker.Plan("n2", Ticks(lastProbeAt));
        Assert.True(plan.IsProbe);
        Assert.False(tracker.Observe("n2", attested: true, Ticks(lastProbeAt), isProbe: true, out double laggingMs));

        Assert.False(tracker.IsLagging("n2"));
        Assert.Equal(0, tracker.LaggingCount);
        Assert.InRange(laggingMs, (lastProbeAt - 1) * 1000 - 1, (lastProbeAt - 1) * 1000 + 1);

        // Healthy again: full-wait asks, and a fresh streak is needed to trip the breaker.
        Assert.Equal(FullWaitMs, tracker.Plan("n2", Ticks(lastProbeAt + 1)).WaitMs);
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(lastProbeAt + 2), isProbe: false, out _));
        Assert.False(tracker.IsLagging("n2"));
    }

    [Fact]
    public void WhileLagging_OnlyProbesAreEvidence_AFullWaitAskPlannedBeforeTheTripCannotRestore()
    {
        ReplicaFenceLagTracker tracker = Build();

        // Concurrent finalizes: several full-wait asks were planned while the replica was still healthy.
        for (int i = 0; i < 5; i++)
            Assert.False(tracker.Plan("n2", Ticks(0)).Lagging);

        Trip(tracker, "n2", 0.4);

        // Their late answers land after the trip. Attesting or not, they are not recovery evidence — the first
        // version restored the endpoint on the first of these and flapped once per second for minutes.
        Assert.Null(tracker.Observe("n2", attested: true, Ticks(0.41), isProbe: false, out _));
        Assert.Null(tracker.Observe("n2", attested: true, Ticks(0.42), isProbe: false, out _));
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(0.43), isProbe: false, out _));
        Assert.True(tracker.IsLagging("n2"));
        Assert.Equal(1, tracker.LaggingCount);

        // Nor do they advance the probe streak: the full threshold of probes is still required.
        Assert.Null(AttestProbes(tracker, "n2", 1.5, ReplicaFenceLagTracker.RecoveryThreshold - 1, out _));
        Assert.True(tracker.IsLagging("n2"));
    }

    [Fact]
    public void AFailedProbe_ResetsTheRecoveryStreak()
    {
        ReplicaFenceLagTracker tracker = Build();
        Trip(tracker, "n2", 0);

        Assert.Null(AttestProbes(tracker, "n2", 1.5, ReplicaFenceLagTracker.RecoveryThreshold - 1, out _));

        // The next probe fails: the replica attests only intermittently, so the streak starts over.
        double failedAt = 1.5 + (ReplicaFenceLagTracker.RecoveryThreshold - 1) * Probe.TotalSeconds;
        Assert.True(tracker.Plan("n2", Ticks(failedAt)).IsProbe);
        Assert.Null(tracker.Observe("n2", attested: false, Ticks(failedAt), isProbe: true, out _));

        Assert.Null(AttestProbes(tracker, "n2", failedAt + 1, ReplicaFenceLagTracker.RecoveryThreshold - 1, out _));
        Assert.True(tracker.IsLagging("n2"));

        double restoredAt = failedAt + 1 + (ReplicaFenceLagTracker.RecoveryThreshold - 1) * Probe.TotalSeconds;
        Assert.True(tracker.Plan("n2", Ticks(restoredAt)).IsProbe);
        Assert.False(tracker.Observe("n2", attested: true, Ticks(restoredAt), isProbe: true, out _));
        Assert.False(tracker.IsLagging("n2"));
    }

    [Fact]
    public void RelapseInsideTheWindow_DoublesTheRequirement_UpToTheCap_AndAQuietStretchResetsIt()
    {
        ReplicaFenceLagTracker tracker = Build();

        double now = 0;
        int expected = ReplicaFenceLagTracker.RecoveryThreshold;

        // Trip, recover, relapse right away: each relapse inside the window doubles the probes needed.
        for (int episode = 0; episode < 6; episode++)
        {
            Trip(tracker, "n2", now);
            Assert.Equal(expected, tracker.RequiredRecoveryStreak("n2"));

            Assert.Null(AttestProbes(tracker, "n2", now + 1.5, expected - 1, out _));
            Assert.True(tracker.IsLagging("n2"));

            double restoredAt = now + 1.5 + (expected - 1) * Probe.TotalSeconds;
            Assert.True(tracker.Plan("n2", Ticks(restoredAt)).IsProbe);
            Assert.False(tracker.Observe("n2", attested: true, Ticks(restoredAt), isProbe: true, out _));
            Assert.False(tracker.IsLagging("n2"));

            now = restoredAt + 0.5;
            expected = Math.Min(expected * 2, ReplicaFenceLagTracker.MaxRecoveryThreshold);
        }

        Assert.Equal(ReplicaFenceLagTracker.MaxRecoveryThreshold, expected);

        // A relapse after a healthy stretch longer than the window starts again at the base threshold.
        now += Relapse.TotalSeconds + 1;
        Trip(tracker, "n2", now);
        Assert.Equal(ReplicaFenceLagTracker.RecoveryThreshold, tracker.RequiredRecoveryStreak("n2"));
    }

    [Fact]
    public void ReplicasAreTrackedIndependently()
    {
        ReplicaFenceLagTracker tracker = Build();
        Trip(tracker, "n2", 1);

        Assert.True(tracker.IsLagging("n2"));
        Assert.False(tracker.IsLagging("n3"));
        Assert.Equal(FullWaitMs, tracker.Plan("n3", Ticks(1.5)).WaitMs);
        Assert.Equal(0, tracker.Plan("n2", Ticks(1.5)).WaitMs);
        Assert.Equal(1, tracker.LaggingCount);
    }
}

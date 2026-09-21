using System.Diagnostics;
using Kahuna.Server.KeyValues.Transactions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The replica fence's per-replica lag breaker (<see cref="ReplicaFenceLagTracker"/>): a replica that keeps
/// failing to attest within the apply wait, or that the leader's acknowledgement snapshot shows cannot attest
/// (durable frontier past the bound, or a durable-write stall), is asked without the wait until enough
/// consecutive full-budget probes — made while its frontier is within the bound — see it attest again. The
/// clock is driven explicitly in stopwatch ticks.
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

    private static ReplicaFrontier Behind(long entries) => new(Known: true, entries, WalStalled: false);

    private static ReplicaFrontier CaughtUp => new(Known: true, EntriesBehind: 12, WalStalled: false);

    private static ReplicaFrontier Stalled => new(Known: true, EntriesBehind: 40, WalStalled: true);

    /// <summary>
    /// A healthy replica whose durable frontier is more than the bound behind the
    /// leader's commit index cannot have applied a prepare the leader just committed, so the fence must stop
    /// waiting on it at once — no three strikes — and say why. Within the bound, nothing changes.
    /// </summary>
    [Fact]
    public void AHealthyReplica_FarBehindOnItsDurableFrontier_IsTrippedOnThePlan_NotAfterStrikes()
    {
        ReplicaFenceLagTracker tracker = Build();

        // At the bound: still a healthy full-wait ask.
        ReplicaFenceLagTracker.AskPlan atBound = tracker.Plan("n2", Ticks(0), Behind(ReplicaFenceLagTracker.MaxEntriesBehind));
        Assert.False(atBound.Lagging);
        Assert.Equal(FullWaitMs, atBound.WaitMs);
        Assert.Null(atBound.Tripped);

        // One past it: tripped by the plan itself, asked without the wait, with the reason for the log line.
        ReplicaFenceLagTracker.AskPlan tripped = tracker.Plan("n2", Ticks(0.1), Behind(ReplicaFenceLagTracker.MaxEntriesBehind + 1));
        Assert.True(tripped.Lagging);
        Assert.Equal(0, tripped.WaitMs);
        Assert.Equal(LaggingBudgetMs, tripped.CallBudgetMs);
        Assert.False(tripped.Score);
        Assert.Equal(ReplicaFenceLagReason.Frontier, tripped.Tripped);
        Assert.True(tracker.IsLagging("n2"));
        Assert.Equal(ReplicaFenceLagReason.Frontier, tracker.LaggingReason("n2"));
        Assert.Equal(1, tracker.LaggingCount);

        // Tripping is reported once: the next plan is an ordinary lagging ask.
        Assert.Null(tracker.Plan("n2", Ticks(0.2), Behind(75_000)).Tripped);
        Assert.Equal(1, tracker.LaggingCount);
    }

    [Fact]
    public void AHealthyReplica_ReportingADurableWriteStall_IsTrippedWithTheStallReason()
    {
        ReplicaFenceLagTracker tracker = Build();

        ReplicaFenceLagTracker.AskPlan tripped = tracker.Plan("n2", Ticks(0), Stalled);

        Assert.True(tripped.Lagging);
        Assert.Equal(ReplicaFenceLagReason.Stall, tripped.Tripped);
        Assert.Equal(ReplicaFenceLagReason.Stall, tracker.LaggingReason("n2"));
    }

    [Fact]
    public void UnknownFrontierEvidence_LeavesTheProbeRulesInCharge()
    {
        ReplicaFenceLagTracker tracker = Build();

        Assert.False(tracker.IsBehind(ReplicaFrontier.Unknown));
        Assert.False(tracker.Plan("n2", Ticks(0), ReplicaFrontier.Unknown).Lagging);

        // An unknown frontier with a huge lag figure is still unknown: only Known evidence counts.
        Assert.False(tracker.IsBehind(new ReplicaFrontier(Known: false, EntriesBehind: 1_000_000, WalStalled: true)));
    }

    /// <summary>
    /// The lagging-attester shape: a lagging replica attests fast probes while tens of thousands of entries behind. Such a
    /// probe must not count, and while the evidence says it is behind no probe is even sent — the due probe
    /// is withheld and the streak restarts — so it is restored only by consecutive probes made and answered
    /// with its frontier within the bound.
    /// </summary>
    [Fact]
    public void WhileLagging_ProbesAreWithheldAndTheStreakRestarts_WhileTheFrontierIsBehind()
    {
        ReplicaFenceLagTracker tracker = Build();
        Trip(tracker, "n2", 0);

        // Two attesting probes with the frontier caught up: one short of recovery.
        Assert.Null(AttestProbes(tracker, "n2", 1.5, ReplicaFenceLagTracker.RecoveryThreshold - 1, out _));

        // The replica falls behind (a snapshot install, a stall): the due probe is withheld, not sent.
        double heldAt = 1.5 + (ReplicaFenceLagTracker.RecoveryThreshold - 1) * Probe.TotalSeconds;
        ReplicaFenceLagTracker.AskPlan held = tracker.Plan("n2", Ticks(heldAt), Behind(75_000));
        Assert.False(held.IsProbe);
        Assert.True(held.HeldByFrontier);
        Assert.True(held.Lagging);
        Assert.Equal(0, held.WaitMs);
        Assert.False(held.Score);
        Assert.Equal(ReplicaFenceLagReason.Frontier, tracker.LaggingReason("n2"));

        // Caught up again: the probe goes out at once (no interval wait), but the streak starts over — the
        // two earlier attestations were made against a replica that has since been behind.
        ReplicaFenceLagTracker.AskPlan probe = tracker.Plan("n2", Ticks(heldAt + 0.1), CaughtUp);
        Assert.True(probe.IsProbe);
        Assert.Null(tracker.Observe("n2", attested: true, Ticks(heldAt + 0.1), isProbe: true, CaughtUp, out _));
        Assert.True(tracker.IsLagging("n2"));

        Assert.Null(AttestProbes(tracker, "n2", heldAt + 1.1, ReplicaFenceLagTracker.RecoveryThreshold - 2, out _));
        Assert.True(tracker.IsLagging("n2"));

        double restoredAt = heldAt + 1.1 + (ReplicaFenceLagTracker.RecoveryThreshold - 2) * Probe.TotalSeconds;
        Assert.True(tracker.Plan("n2", Ticks(restoredAt), CaughtUp).IsProbe);
        Assert.False(tracker.Observe("n2", attested: true, Ticks(restoredAt), isProbe: true, CaughtUp, out _));
        Assert.False(tracker.IsLagging("n2"));
    }

    [Fact]
    public void AnAttestingProbe_AnsweredWhileTheFrontierIsBehind_IsNotRecoveryEvidence()
    {
        ReplicaFenceLagTracker tracker = Build();
        Trip(tracker, "n2", 0);

        Assert.Null(AttestProbes(tracker, "n2", 1.5, ReplicaFenceLagTracker.RecoveryThreshold - 1, out _));

        // The probe was planned with the frontier caught up; by the time it answers the leader has learned the
        // replica is stalling. Its attestation came from state it has not caught up to: streak restarts.
        double at = 1.5 + (ReplicaFenceLagTracker.RecoveryThreshold - 1) * Probe.TotalSeconds;
        Assert.True(tracker.Plan("n2", Ticks(at), CaughtUp).IsProbe);
        Assert.Null(tracker.Observe("n2", attested: true, Ticks(at), isProbe: true, Stalled, out _));
        Assert.True(tracker.IsLagging("n2"));
        Assert.Equal(ReplicaFenceLagReason.Stall, tracker.LaggingReason("n2"));

        // The full threshold is needed again.
        Assert.Null(AttestProbes(tracker, "n2", at + 1, ReplicaFenceLagTracker.RecoveryThreshold - 1, out _));
        Assert.True(tracker.IsLagging("n2"));
    }

    [Fact]
    public void AHealthyReplicasAnswer_ScoredAfterTheLeaderLearnedItIsBehind_TripsRegardlessOfAttestation()
    {
        ReplicaFenceLagTracker tracker = Build();

        Assert.False(tracker.Plan("n2", Ticks(0), CaughtUp).Lagging);

        // The ask attested, but the acknowledgement folded during the wait shows a stall: waiting again is
        // known to be wasted, so the breaker trips here rather than after three more strikes.
        Assert.True(tracker.Observe("n2", attested: true, Ticks(0.3), isProbe: false, Stalled, out _));
        Assert.True(tracker.IsLagging("n2"));
        Assert.Equal(ReplicaFenceLagReason.Stall, tracker.LaggingReason("n2"));
        Assert.Equal(1, tracker.LaggingCount);
    }

    [Fact]
    public void AFrontierTrip_EscalatesLikeAnyRelapse()
    {
        ReplicaFenceLagTracker tracker = Build();

        Trip(tracker, "n2", 0);
        double restoredAt = 1.5 + (ReplicaFenceLagTracker.RecoveryThreshold - 1) * Probe.TotalSeconds;
        Assert.False(AttestProbes(tracker, "n2", 1.5, ReplicaFenceLagTracker.RecoveryThreshold, out _));

        // Relapse on frontier evidence inside the window: the next recovery needs twice the probes.
        Assert.Equal(ReplicaFenceLagReason.Frontier, tracker.Plan("n2", Ticks(restoredAt + 1), Behind(5_000)).Tripped);
        Assert.Equal(ReplicaFenceLagTracker.RecoveryThreshold * 2, tracker.RequiredRecoveryStreak("n2"));
    }

    [Fact]
    public void TheBoundIsConfigurable()
    {
        ReplicaFenceLagTracker tracker = new(FullWaitMs, FullBudgetMs, LaggingBudgetMs, Probe, Relapse, maxEntriesBehind: 10);

        Assert.False(tracker.IsBehind(Behind(10)));
        Assert.True(tracker.IsBehind(Behind(11)));
        Assert.True(tracker.IsBehind(new ReplicaFrontier(Known: true, EntriesBehind: 0, WalStalled: true)));
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

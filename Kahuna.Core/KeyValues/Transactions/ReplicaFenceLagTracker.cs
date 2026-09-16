using System.Collections.Concurrent;
using System.Diagnostics;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// What the partition leader knows about a replica's position relative to its own commit index, read
/// from Kommander's per-follower acknowledgement snapshot (<c>IRaft.GetFollowerProgress</c>) at the moment
/// the fence plans an ask. <see cref="Known"/> is false when this node does not lead the partition being
/// asked about or the current leadership has not heard from the replica yet; the fence then falls back
/// to probe evidence alone. <see cref="EntriesBehind"/> is how many committed entries the replica's
/// DURABLE frontier trails the leader's commit index by — the frontier its disk has written, not the one
/// it advances for entries merely queued — and <see cref="WalStalled"/> whether the replica's last
/// acknowledgement put its oldest pending WAL write past the stall threshold.
/// </summary>
internal readonly record struct ReplicaFrontier(bool Known, long EntriesBehind, bool WalStalled)
{
    /// <summary>No leader-side evidence: probe behavior only.</summary>
    internal static readonly ReplicaFrontier Unknown;
}

/// <summary>Why a replica is (or just became) lagging in the fence's eyes.</summary>
internal enum ReplicaFenceLagReason
{
    /// <summary>It failed to attest <see cref="ReplicaFenceLagTracker.LaggingThreshold"/> consecutive full-wait asks.</summary>
    Attestation,

    /// <summary>Its durable frontier is more than <see cref="ReplicaFenceLagTracker.MaxEntriesBehind"/> committed entries behind the leader's commit index.</summary>
    Frontier,

    /// <summary>It reports a durable-write stall: its disk is not answering, so nothing new can apply there.</summary>
    Stall,
}

/// <summary>
/// Per-replica circuit breaker for the pre-decision replica fence (<see cref="DurableMaintenanceService.ConfirmReplicaFenceForCommitAsync"/>).
///
/// <para><b>Why it exists.</b> The fence asks every replica of every participant partition for its
/// staged-base verdict and waits for all of them before the decision. A replica whose apply has stalled
/// (its disk paused, its WAL saturated, a snapshot install in progress) cannot attest: its handler waits the
/// full server-side apply budget and answers <c>NotApplied</c> for every key, or does not answer at all. The
/// answer carries no information the commit can use, yet every commit on the leader paid the full wait for
/// it. In the CamusDB slow-disk runs (Vorpal 3c7f6b99) that turned one follower's 30-second device pause into
/// a 70% throughput loss on a leader whose Raft quorum was intact, and the paused replica's catch-up kept the
/// cluster below half speed for minutes after the pause ended. Waiting on a replica that has proven it cannot
/// attest is pure cost: a replica that cannot answer contributes nothing to the fence by design (a down node
/// cannot veto either), so the fence's safety does not depend on the wait.</para>
///
/// <para><b>What it does.</b> Each replica endpoint is either <i>healthy</i> or <i>lagging</i>. A healthy replica
/// is asked with the full apply wait and call budget. Once <see cref="LaggingThreshold"/> consecutive full-wait
/// asks come back without an attestation (a timeout, a transport fault, an unserviced reply, or a serviced
/// reply whose verdicts are all <c>NotApplied</c>), the endpoint becomes lagging: it is still asked on every
/// commit, but with a zero apply wait and a short call budget, so its instant memory-based verdict still
/// counts (a <c>StaleBase</c> the replica can prove without waiting is still honoured) while the commit no
/// longer pays for an apply it is not going to see. Once per <see cref="ProbeInterval"/> a single ask to a
/// lagging replica is sent with the full budget as a probe. Non-probe asks to a lagging replica are not
/// scored, since their <c>NotApplied</c> is expected.</para>
///
/// <para><b>The leader's frontier evidence gates both directions.</b> Probe latency is the wrong evidence
/// for whether a replica can attest: a replica tens of thousands of entries behind answers a probe
/// instantly and truthfully from state that old, and a replica whose disk is paused answers from memory
/// until the entry it is asked about is the one it cannot write. In the CamusDB leader-kill run lk8
/// (Vorpal 029dad72, ask 2) a restarted replica attested three fast probes while 75,000 entries behind
/// the leader's commit index and stalling on its shared NVMe; the fence restored it, every commit then
/// waited the full apply wait for a verdict it could not give, and throughput sat at 60% for three
/// minutes. The leader already holds the facts that settle this — the replica's durable frontier and
/// its pending-write age, refreshed by every acknowledgement — so the fence reads them
/// (<see cref="ReplicaFrontier"/>) on every ask it plans: a healthy replica whose durable frontier is more
/// than <see cref="MaxEntriesBehind"/> entries behind, or that reports a durable-write stall, is tripped at
/// once without waiting for three strikes (it cannot have applied a prepare the leader just committed, so
/// the wait is known to be wasted); a lagging replica in that state is not even probed, and its recovery
/// streak restarts, so it is restored only by consecutive attesting probes made while its frontier is
/// within the bound and its disk is answering. Where the evidence is unknown — this node does not lead the
/// partition asked about, as for a participant partition led elsewhere — the probe rules alone apply.</para>
///
/// <para><b>Recovery is hysteretic.</b> A lagging replica is restored only by <see cref="RecoveryThreshold"/>
/// CONSECUTIVE attesting probes, and only probes count while it is lagging. The first version restored the
/// endpoint on the first attesting answer of any full-wait ask, and that flapped: finalizes run concurrently,
/// so when a replica that attests for some keys and not for others (a restarted node whose install left it
/// unable to apply a subset of keys — Vorpal 029dad72) tripped the breaker, dozens of full-wait asks planned
/// before the trip were still in flight, the first of them to attest restored the endpoint within
/// milliseconds, every concurrent commit paid the full wait again, three of those timed out and tripped it
/// again — about once per second for the rest of the run, at half throughput. Requiring several consecutive
/// probes spaced by the probe interval turns "attests sometimes" into a sustained lagging state whose whole
/// cost is one full-wait probe per interval. A replica that relapses within <see cref="RelapseWindow"/> of a
/// recovery needs twice as many consecutive probes the next time (capped at <see cref="MaxRecoveryThreshold"/>),
/// so a replica hovering at the edge of the apply wait is asked without the wait for longer each time until
/// it is genuinely stable; a replica that stays healthy past the window starts over at the base
/// threshold.</para>
///
/// <para><b>Concurrency.</b> Finalizes run concurrently across the node; the per-endpoint state is guarded by
/// its own lock. Time is passed in as <see cref="Stopwatch"/> ticks so tests can drive the clock.</para>
/// </summary>
internal sealed class ReplicaFenceLagTracker
{
    /// <summary>Consecutive non-attesting full-wait rounds before a replica is treated as lagging. Three rounds
    /// at the 400 ms apply wait is ~1.2 s of a replica not applying, well past a healthy commit-broadcast hop
    /// (single-digit milliseconds) and short against the 30 s device pause the breaker exists for.</summary>
    internal const int LaggingThreshold = 3;

    /// <summary>Consecutive attesting probes that restore a lagging replica. At one probe per second this is a
    /// few seconds of sustained attestation — long enough that a replica attesting only intermittently cannot
    /// get back in, short enough that a genuinely healed replica rejoins the fence promptly.</summary>
    internal const int RecoveryThreshold = 3;

    /// <summary>Ceiling for the escalated recovery requirement of a replica that keeps relapsing: about half a
    /// minute of consecutive attesting probes.</summary>
    internal const int MaxRecoveryThreshold = 24;

    /// <summary>
    /// How far a replica's durable frontier may trail the leader's commit index, in committed entries, before
    /// the fence stops waiting on it. A healthy follower trails by the entries in flight over one
    /// commit-broadcast hop plus its fsync — tens of entries at the ~7,000 ops/s of the CamusDB bank runs —
    /// so a thousand is well clear of normal jitter and a small fraction of the 400 ms apply wait's worth of
    /// entries at that rate, while a replica catching up from a restart or a snapshot install (tens of
    /// thousands behind) or ridden by a disk pause is far past it. Sized in entries rather than time because
    /// the question it answers is positional: can the replica have applied the prepare the leader just
    /// committed? It cannot until its durable frontier reaches that entry.
    /// </summary>
    internal const long MaxEntriesBehind = 1_000;

    /// <summary>A relapse within this long after a recovery doubles the probes the next recovery needs; a
    /// relapse after it starts again at <see cref="RecoveryThreshold"/>.</summary>
    internal static readonly TimeSpan RelapseWindow = TimeSpan.FromSeconds(30);

    /// <summary>Spacing of full-budget probes to a lagging replica: one commit per second pays the full wait
    /// to notice recovery, so a healed replica is back in the fence within a few seconds of applying again.</summary>
    internal static readonly TimeSpan ProbeInterval = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The plan for one ask: the server-side apply wait and caller-side budget to use, whether the outcome
    /// should be scored, whether this ask is a recovery probe of a lagging replica, and — when planning the
    /// ask itself tripped the breaker on frontier evidence — why (<see cref="Tripped"/>, for the caller's log
    /// line). <see cref="HeldByFrontier"/> marks a due probe the frontier evidence withheld.
    /// </summary>
    internal readonly record struct AskPlan(
        int WaitMs, int CallBudgetMs, bool Score, bool IsProbe, bool Lagging,
        ReplicaFenceLagReason? Tripped = null, bool HeldByFrontier = false);

    private sealed class EndpointState
    {
        public int Strikes;
        public bool Lagging;
        public ReplicaFenceLagReason Reason;
        public long NextProbeTicks;
        public long LaggingSinceTicks;

        // Recovery hysteresis: attesting probes in a row since the trip (a failed probe, or one made while the
        // frontier evidence says the replica is behind, resets it), how many this episode needs, and when the
        // endpoint last recovered (for relapse escalation).
        public int RecoveryStreak;
        public int RequiredRecoveryStreak = RecoveryThreshold;
        public bool HasRecovered;
        public long LastRecoveredTicks;
    }

    private readonly ConcurrentDictionary<string, EndpointState> endpoints = new(StringComparer.Ordinal);

    private readonly int fullWaitMs;

    private readonly int fullCallBudgetMs;

    private readonly int laggingCallBudgetMs;

    private readonly long probeIntervalTicks;

    private readonly long relapseWindowTicks;

    private readonly long maxEntriesBehind;

    private int laggingCount;

    /// <summary>Endpoints currently treated as lagging (a gauge for the metrics scrape).</summary>
    internal int LaggingCount => Volatile.Read(ref laggingCount);

    internal ReplicaFenceLagTracker(
        int fullWaitMs, int fullCallBudgetMs, int laggingCallBudgetMs,
        TimeSpan? probeInterval = null, TimeSpan? relapseWindow = null, long? maxEntriesBehind = null)
    {
        this.fullWaitMs = fullWaitMs;
        this.fullCallBudgetMs = fullCallBudgetMs;
        this.laggingCallBudgetMs = laggingCallBudgetMs;
        probeIntervalTicks = (long)((probeInterval ?? ProbeInterval).TotalSeconds * Stopwatch.Frequency);
        relapseWindowTicks = (long)((relapseWindow ?? RelapseWindow).TotalSeconds * Stopwatch.Frequency);
        this.maxEntriesBehind = maxEntriesBehind ?? MaxEntriesBehind;
    }

    /// <summary>Whether <paramref name="endpoint"/> is currently treated as lagging.</summary>
    internal bool IsLagging(string endpoint) =>
        endpoints.TryGetValue(endpoint, out EndpointState? state) && Volatile.Read(ref state.Lagging);

    /// <summary>Why <paramref name="endpoint"/> is lagging (meaningful only while <see cref="IsLagging"/>).</summary>
    internal ReplicaFenceLagReason LaggingReason(string endpoint) =>
        endpoints.TryGetValue(endpoint, out EndpointState? state) ? state.Reason : ReplicaFenceLagReason.Attestation;

    /// <summary>How many consecutive attesting probes <paramref name="endpoint"/>'s current (or next) lagging
    /// episode needs before it is restored. Observability and tests.</summary>
    internal int RequiredRecoveryStreak(string endpoint) =>
        endpoints.TryGetValue(endpoint, out EndpointState? state) ? Volatile.Read(ref state.RequiredRecoveryStreak) : RecoveryThreshold;

    /// <summary>Whether the leader's evidence says the replica cannot attest right now: its disk is stalled or
    /// its durable frontier is past the bound. Unknown evidence never says so.</summary>
    internal bool IsBehind(ReplicaFrontier frontier) =>
        frontier.Known && (frontier.WalStalled || frontier.EntriesBehind > maxEntriesBehind);

    private static ReplicaFenceLagReason FrontierReason(ReplicaFrontier frontier) =>
        frontier.WalStalled ? ReplicaFenceLagReason.Stall : ReplicaFenceLagReason.Frontier;

    /// <summary>Decides how to ask <paramref name="endpoint"/> at <paramref name="nowTicks"/> with no frontier evidence.</summary>
    internal AskPlan Plan(string endpoint, long nowTicks) => Plan(endpoint, nowTicks, ReplicaFrontier.Unknown);

    /// <summary>
    /// Decides how to ask <paramref name="endpoint"/> at <paramref name="nowTicks"/> given the leader's
    /// <paramref name="frontier"/> evidence about it. A healthy replica the evidence says is behind is tripped
    /// here and asked without the wait; a lagging one is probed only while the evidence does not say so.
    /// </summary>
    internal AskPlan Plan(string endpoint, long nowTicks, ReplicaFrontier frontier)
    {
        EndpointState state = endpoints.GetOrAdd(endpoint, static _ => new EndpointState());
        bool behind = IsBehind(frontier);

        lock (state)
        {
            if (!state.Lagging)
            {
                if (!behind)
                    return new AskPlan(fullWaitMs, fullCallBudgetMs, Score: true, IsProbe: false, Lagging: false);

                // The replica cannot have applied a prepare the leader just committed: waiting is known to be
                // wasted, so no strikes are needed to prove it.
                ReplicaFenceLagReason reason = FrontierReason(frontier);
                Trip(state, nowTicks, reason);
                return new AskPlan(0, laggingCallBudgetMs, Score: false, IsProbe: false, Lagging: true, Tripped: reason);
            }

            if (behind)
            {
                // A probe now would either fail (wasting the full wait) or attest from state the replica has
                // not caught up to (restoring it wrongly). Withhold it, and restart the streak: whatever it
                // attested before, it is behind again now.
                state.RecoveryStreak = 0;
                state.Reason = FrontierReason(frontier);
                return new AskPlan(0, laggingCallBudgetMs, Score: false, IsProbe: false, Lagging: true, HeldByFrontier: true);
            }

            if (nowTicks - state.NextProbeTicks >= 0)
            {
                state.NextProbeTicks = nowTicks + probeIntervalTicks;
                return new AskPlan(fullWaitMs, fullCallBudgetMs, Score: true, IsProbe: true, Lagging: true);
            }

            return new AskPlan(0, laggingCallBudgetMs, Score: false, IsProbe: false, Lagging: true);
        }
    }

    /// <summary>Scores an ask made with no frontier evidence; see the other overload.</summary>
    internal bool? Observe(string endpoint, bool attested, long nowTicks, bool isProbe, out double laggingMs) =>
        Observe(endpoint, attested, nowTicks, isProbe, ReplicaFrontier.Unknown, out laggingMs);

    /// <summary>
    /// Scores the outcome of a scored ask (<see cref="AskPlan.Score"/>); <paramref name="isProbe"/> is the plan's
    /// <see cref="AskPlan.IsProbe"/> and <paramref name="frontier"/> the leader's evidence re-read as the answer
    /// arrived. Returns the transition it caused: <c>true</c> when the endpoint just became lagging,
    /// <c>false</c> when it just recovered (with <paramref name="laggingMs"/> the length of the episode),
    /// <c>null</c> when nothing changed. While the endpoint is lagging only probes are evidence: a full-wait
    /// ask planned while it was still healthy and answered after the trip says nothing about whether it has
    /// recovered, and letting it restore the endpoint is exactly the flapping described on the type. An
    /// attesting probe answered while the evidence says the replica is behind is not recovery evidence
    /// either — it attested from state it has not caught up to — and restarts the streak.
    /// </summary>
    internal bool? Observe(string endpoint, bool attested, long nowTicks, bool isProbe, ReplicaFrontier frontier, out double laggingMs)
    {
        laggingMs = 0;
        EndpointState state = endpoints.GetOrAdd(endpoint, static _ => new EndpointState());
        bool behind = IsBehind(frontier);

        lock (state)
        {
            if (state.Lagging)
            {
                if (!isProbe)
                    return null;

                if (!attested || behind)
                {
                    state.RecoveryStreak = 0;
                    if (behind)
                        state.Reason = FrontierReason(frontier);
                    return null;
                }

                if (++state.RecoveryStreak < state.RequiredRecoveryStreak)
                    return null;

                laggingMs = (nowTicks - state.LaggingSinceTicks) * 1000.0 / Stopwatch.Frequency;
                state.Lagging = false;
                state.Strikes = 0;
                state.RecoveryStreak = 0;
                state.LaggingSinceTicks = 0;
                state.HasRecovered = true;
                state.LastRecoveredTicks = nowTicks;
                Interlocked.Decrement(ref laggingCount);
                return false;
            }

            if (behind)
            {
                // The answer arrived after the leader learned the replica is behind (or stalled): whether it
                // attested or not, the next wait would be wasted.
                Trip(state, nowTicks, FrontierReason(frontier));
                return true;
            }

            if (attested)
            {
                state.Strikes = 0;
                return null;
            }

            if (++state.Strikes < LaggingThreshold)
                return null;

            Trip(state, nowTicks, ReplicaFenceLagReason.Attestation);
            return true;
        }
    }

    /// <summary>Moves a healthy endpoint to lagging. Caller holds the state lock.</summary>
    private void Trip(EndpointState state, long nowTicks, ReplicaFenceLagReason reason)
    {
        // A relapse soon after a recovery escalates what the next recovery needs; a long healthy stretch
        // resets the requirement.
        state.RequiredRecoveryStreak = state.HasRecovered && nowTicks - state.LastRecoveredTicks < relapseWindowTicks
            ? Math.Min(state.RequiredRecoveryStreak * 2, MaxRecoveryThreshold)
            : RecoveryThreshold;

        state.Lagging = true;
        state.Reason = reason;
        state.Strikes = 0;
        state.RecoveryStreak = 0;
        state.LaggingSinceTicks = nowTicks;
        state.NextProbeTicks = nowTicks + probeIntervalTicks;
        Interlocked.Increment(ref laggingCount);
    }
}

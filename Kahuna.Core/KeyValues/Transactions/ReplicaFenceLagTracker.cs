
using System.Collections.Concurrent;
using System.Diagnostics;

namespace Kahuna.Server.KeyValues.Transactions;

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
/// lagging replica is sent with the full budget as a probe; the first attesting answer restores the endpoint
/// to healthy. Non-probe asks to a lagging replica are not scored, since their <c>NotApplied</c> is expected.</para>
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

    /// <summary>Spacing of full-budget probes to a lagging replica: one commit per second pays the full wait
    /// to notice recovery, so a healed replica is back in the fence within about a second of applying again.</summary>
    internal static readonly TimeSpan ProbeInterval = TimeSpan.FromSeconds(1);

    /// <summary>The plan for one ask: the server-side apply wait and caller-side budget to use, whether the
    /// outcome should be scored, and whether this ask is a recovery probe of a lagging replica.</summary>
    internal readonly record struct AskPlan(int WaitMs, int CallBudgetMs, bool Score, bool IsProbe, bool Lagging);

    private sealed class EndpointState
    {
        public int Strikes;
        public bool Lagging;
        public long NextProbeTicks;
        public long LaggingSinceTicks;
    }

    private readonly ConcurrentDictionary<string, EndpointState> endpoints = new(StringComparer.Ordinal);

    private readonly int fullWaitMs;

    private readonly int fullCallBudgetMs;

    private readonly int laggingCallBudgetMs;

    private readonly long probeIntervalTicks;

    private int laggingCount;

    /// <summary>Endpoints currently treated as lagging (a gauge for the metrics scrape).</summary>
    internal int LaggingCount => Volatile.Read(ref laggingCount);

    internal ReplicaFenceLagTracker(int fullWaitMs, int fullCallBudgetMs, int laggingCallBudgetMs, TimeSpan? probeInterval = null)
    {
        this.fullWaitMs = fullWaitMs;
        this.fullCallBudgetMs = fullCallBudgetMs;
        this.laggingCallBudgetMs = laggingCallBudgetMs;
        probeIntervalTicks = (long)((probeInterval ?? ProbeInterval).TotalSeconds * Stopwatch.Frequency);
    }

    /// <summary>Whether <paramref name="endpoint"/> is currently treated as lagging.</summary>
    internal bool IsLagging(string endpoint) =>
        endpoints.TryGetValue(endpoint, out EndpointState? state) && Volatile.Read(ref state.Lagging);

    /// <summary>Decides how to ask <paramref name="endpoint"/> at <paramref name="nowTicks"/>.</summary>
    internal AskPlan Plan(string endpoint, long nowTicks)
    {
        EndpointState state = endpoints.GetOrAdd(endpoint, static _ => new EndpointState());

        lock (state)
        {
            if (!state.Lagging)
                return new AskPlan(fullWaitMs, fullCallBudgetMs, Score: true, IsProbe: false, Lagging: false);

            if (nowTicks - state.NextProbeTicks >= 0)
            {
                state.NextProbeTicks = nowTicks + probeIntervalTicks;
                return new AskPlan(fullWaitMs, fullCallBudgetMs, Score: true, IsProbe: true, Lagging: true);
            }

            return new AskPlan(0, laggingCallBudgetMs, Score: false, IsProbe: false, Lagging: true);
        }
    }

    /// <summary>
    /// Scores the outcome of a full-wait ask. Returns the transition it caused: <c>true</c> when the endpoint
    /// just became lagging, <c>false</c> when it just recovered (with <paramref name="laggingMs"/> the length
    /// of the episode), <c>null</c> when nothing changed.
    /// </summary>
    internal bool? Observe(string endpoint, bool attested, long nowTicks, out double laggingMs)
    {
        laggingMs = 0;
        EndpointState state = endpoints.GetOrAdd(endpoint, static _ => new EndpointState());

        lock (state)
        {
            if (attested)
            {
                state.Strikes = 0;

                if (!state.Lagging)
                    return null;

                laggingMs = (nowTicks - state.LaggingSinceTicks) * 1000.0 / Stopwatch.Frequency;
                state.Lagging = false;
                state.LaggingSinceTicks = 0;
                Interlocked.Decrement(ref laggingCount);
                return false;
            }

            if (state.Lagging)
                return null;

            if (++state.Strikes < LaggingThreshold)
                return null;

            state.Lagging = true;
            state.LaggingSinceTicks = nowTicks;
            state.NextProbeTicks = nowTicks + probeIntervalTicks;
            Interlocked.Increment(ref laggingCount);
            return true;
        }
    }

}

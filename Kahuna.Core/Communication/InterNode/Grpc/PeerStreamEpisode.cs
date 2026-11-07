using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Communication.Internode.Grpc;

/// <summary>
/// Per-peer record of the shared streams' failures: the backoff that spaces re-opens to a peer that
/// refuses them, and the rate limit on the log lines those failures produce.
///
/// <para><b>Why.</b> The registry of shared streams is rebuilt by the first drain that finds it empty,
/// and a drain runs for every burst of forwarded requests. Against a dead peer every rebuild fails at
/// once, so a leader with reads to confirm and locks to forward re-opened the streams 70–100 times a
/// second for the whole outage and logged four Warning lines per attempt — 94,000 lines on one leader
/// in a two-hour fault soak, each attempt two duplex calls and two read loops torn down the moment they
/// started, and the lines that mattered buried under them. One episode per peer keeps the fault
/// attributable from the log without making the log the fault, and the backoff caps the opens at one
/// per <see cref="ReopenBackoffMax"/> once the peer has refused a few in a row.</para>
///
/// <para><b>Backoff.</b> An eviction — one stream set torn down, whichever of its write or read paths
/// failed first — schedules the next open at now plus the current delay and doubles the delay up to
/// the cap. A drain that finds the registry empty while the delay holds refuses its requests without
/// an open; they fail with the same retryable status a failed open gives them, so every caller sees
/// the operation's typed MustRetry as before, only sooner. <see cref="TryAllowOpen"/> claims the open
/// it allows, so a second drain that arrives before the outcome of the first is known does not open
/// a second set. The first write that reaches the peer closes the episode and resets the delay:
/// recovery is bounded by the channel's own reconnect, not by this backoff.</para>
///
/// <para><b>Log shape.</b> The eviction that opens an episode logs at Warning with the failure that
/// caused it; while the episode lasts, one Warning summary per <see cref="FailureSummaryInterval"/>
/// carries the counts of re-opens, evictions and refused requests; the write that recovers logs one
/// Information line with the totals; every other failure logs at Debug. Only an eviction opens an
/// episode: the write and read paths that fail each report their own signal, but every one of them
/// is followed by the eviction of its set, so a signal arriving late — the close of a stream disposed
/// before the recovery — cannot start an episode on its own.</para>
///
/// <para>Thread-safe: the dispatch loop, the concurrent per-stream writers and both read loops of a
/// peer all report here. State changes take a private lock and log after leaving it; the checks on
/// the write path are a volatile read of one flag, free while no episode is open. The clock is
/// injectable so the delays and the summary interval are testable without waiting them out.</para>
/// </summary>
internal sealed class PeerStreamEpisode
{
    /// <summary>
    /// Delay before the first re-open after an eviction; it doubles per eviction up to
    /// <see cref="ReopenBackoffMax"/>. Mutable only so tests can shrink it.
    /// </summary>
    internal static TimeSpan ReopenBackoffInitial = TimeSpan.FromMilliseconds(100);

    /// <summary>Longest delay between two re-opens of a peer's streams. Mutable only so tests can shrink it.</summary>
    internal static TimeSpan ReopenBackoffMax = TimeSpan.FromSeconds(2);

    /// <summary>Spacing of the Warning summaries while an episode lasts. Mutable only so tests can shrink it.</summary>
    internal static TimeSpan FailureSummaryInterval = TimeSpan.FromSeconds(10);

    private readonly object gate = new();

    private readonly ILogger logger;

    private readonly string peer;

    private readonly Func<long> clockMs;

    private readonly KeyValuePair<string, object?> peerTag;

    // An explicit flag, not a sentinel clock value: the clock is whatever the host gives
    // (Environment.TickCount64 can be anything, a test clock starts at 0).
    private bool open;

    /// <summary>Clock reading when the open episode started.</summary>
    private long episodeStartMs;

    private long lastSummaryMs;

    /// <summary>Clock reading from which the next open is allowed; meaningful only while <see cref="open"/>.</summary>
    private long nextOpenAllowedAtMs;

    /// <summary>Delay the next eviction schedules before the open that follows it.</summary>
    private long backoffMs;

    private long opensInEpisode, opensSinceSummary;

    private long evictionsInEpisode, evictionsSinceSummary;

    private long refusedInEpisode, refusedSinceSummary;

    private enum Line
    {
        Detail,
        Opened,
        Summary
    }

    public PeerStreamEpisode(ILogger logger, string peer, Func<long>? clockMs = null)
    {
        this.logger = logger;
        this.peer = peer;
        this.clockMs = clockMs ?? (static () => Environment.TickCount64);
        peerTag = new("peer", peer);
    }

    /// <summary>True while failures were seen with no write reaching the peer since.</summary>
    public bool IsOpen => Volatile.Read(ref open);

    /// <summary>Delay the next eviction will schedule; 0 when no episode is open. Exposed for tests.</summary>
    internal long BackoffMs
    {
        get
        {
            lock (gate)
                return backoffMs;
        }
    }

    /// <summary>Stream sets opened since the episode started; 0 when none is open. Exposed for tests.</summary>
    internal long OpensInEpisode
    {
        get
        {
            lock (gate)
                return opensInEpisode;
        }
    }

    /// <summary>Stream sets torn down since the episode started; 0 when none is open. Exposed for tests.</summary>
    internal long EvictionsInEpisode
    {
        get
        {
            lock (gate)
                return evictionsInEpisode;
        }
    }

    /// <summary>Requests refused without an open since the episode started; 0 when none is open. Exposed for tests.</summary>
    internal long RefusedInEpisode
    {
        get
        {
            lock (gate)
                return refusedInEpisode;
        }
    }

    /// <summary>
    /// Whether the peer's streams may be rebuilt now. False while the delay of an open episode holds.
    /// True claims the open: the next allowed time moves to now plus the current delay, so a second
    /// drain that arrives before this open's outcome is known is refused instead of opening again.
    /// Free while no episode is open.
    /// </summary>
    public bool TryAllowOpen()
    {
        if (!Volatile.Read(ref open))
            return true;

        long now = clockMs();

        lock (gate)
        {
            if (!open)
                return true;

            if (now - nextOpenAllowedAtMs < 0)
                return false;

            nextOpenAllowedAtMs = now + backoffMs;
            return true;
        }
    }

    /// <summary>Counts one rebuilt stream set.</summary>
    public void NoteOpened()
    {
        InterNodeStreamMetrics.StreamOpens.Add(1, peerTag);

        if (!Volatile.Read(ref open))
            return;

        lock (gate)
        {
            if (!open)
                return;

            opensInEpisode++;
            opensSinceSummary++;
        }
    }

    /// <summary>Counts <paramref name="requests"/> refused without an open because the delay held.</summary>
    public void NoteRefused(int requests)
    {
        if (requests <= 0)
            return;

        InterNodeStreamMetrics.RequestsRefusedByBackoff.Add(requests, peerTag);

        lock (gate)
        {
            if (!open)
                return;

            refusedInEpisode += requests;
            refusedSinceSummary += requests;
        }
    }

    /// <summary>
    /// Records one failure signal that did not itself tear a stream set down: a write that failed, a
    /// response stream that closed, a stream the peer refused. Inside an open episode it is a Debug
    /// line or, when the interval has passed, the summary. Outside one it is a Debug line and nothing
    /// more: every such signal is followed by the eviction of its set, and the eviction is what opens
    /// an episode, so the late close of a stream disposed before a recovery cannot start a new one.
    /// <paramref name="stage"/> names the path that failed, as a constant string.
    /// </summary>
    public void NoteFailure(string stage, Exception cause) => Record(stage, cause, evicted: false);

    /// <summary>
    /// Records the eviction of one stream set — the moment the registry entry was removed, which
    /// happens once per set however many of its paths fail — with the failure that caused it. Steps
    /// the backoff: the next open waits the current delay, and the delay doubles up to the cap.
    /// </summary>
    public void NoteEviction(string reason, Exception cause) => Record(reason, cause, evicted: true);

    /// <summary>
    /// A write reached the peer: closes the open episode, if any, with its totals and resets the
    /// delay, so the next failure starts a new episode at the initial delay. Free when none is open.
    /// </summary>
    public void NoteSuccess()
    {
        if (!Volatile.Read(ref open))
            return;

        long now = clockMs();
        long opens, evictions, refused, elapsedMs;

        lock (gate)
        {
            if (!open)
                return;

            opens = opensInEpisode;
            evictions = evictionsInEpisode;
            refused = refusedInEpisode;
            elapsedMs = now - episodeStartMs;

            Volatile.Write(ref open, false);
            episodeStartMs = 0;
            lastSummaryMs = 0;
            nextOpenAllowedAtMs = 0;
            backoffMs = 0;
            opensInEpisode = opensSinceSummary = 0;
            evictionsInEpisode = evictionsSinceSummary = 0;
            refusedInEpisode = refusedSinceSummary = 0;
        }

        if (logger.IsEnabled(LogLevel.Information))
            logger.LogInformation(
                "GrpcServerBatcher streams to {Peer} recovered: a write reached the peer after {Opens} re-opens, {Evictions} torn down and {Refused} requests refused while backed off, over {Seconds:F0} s",
                peer, opens, evictions, refused, elapsedMs / 1000.0);
    }

    private void Record(string stage, Exception cause, bool evicted)
    {
        if (evicted)
            InterNodeStreamMetrics.StreamFailures.Add(1, peerTag);

        long now = clockMs();
        long intervalMs = (long)FailureSummaryInterval.TotalMilliseconds;

        Line line;
        long recentOpens = 0, recentEvictions = 0, recentRefused = 0, recentMs = 0;
        long totalOpens = 0, totalEvictions = 0, totalRefused = 0, episodeMs = 0;
        long initialMs = 0, maxMs = 0;

        lock (gate)
        {
            if (!open && !evicted)
                line = Line.Detail;
            else if (!open)
            {
                Volatile.Write(ref open, true);
                episodeStartMs = now;
                lastSummaryMs = now;
                backoffMs = (long)ReopenBackoffInitial.TotalMilliseconds;
                opensInEpisode = opensSinceSummary = 0;
                refusedInEpisode = refusedSinceSummary = 0;

                // The opening eviction is on the Warning line itself, so it is not "since the summary".
                evictionsInEpisode = 1;
                evictionsSinceSummary = 0;
                ScheduleNextOpen(now);

                initialMs = (long)ReopenBackoffInitial.TotalMilliseconds;
                maxMs = (long)ReopenBackoffMax.TotalMilliseconds;
                line = Line.Opened;
            }
            else
            {
                if (evicted)
                {
                    evictionsInEpisode++;
                    evictionsSinceSummary++;
                    ScheduleNextOpen(now);
                }

                if (now - lastSummaryMs >= intervalMs)
                {
                    recentOpens = opensSinceSummary;
                    recentEvictions = evictionsSinceSummary;
                    recentRefused = refusedSinceSummary;
                    recentMs = now - lastSummaryMs;
                    totalOpens = opensInEpisode;
                    totalEvictions = evictionsInEpisode;
                    totalRefused = refusedInEpisode;
                    episodeMs = now - episodeStartMs;

                    lastSummaryMs = now;
                    opensSinceSummary = evictionsSinceSummary = refusedSinceSummary = 0;
                    line = Line.Summary;
                }
                else
                    line = Line.Detail;
            }
        }

        switch (line)
        {
            case Line.Opened:
                logger.LogWarning(
                    "GrpcServerBatcher streams to {Peer} failed ({Stage}): {ExType}: {Message}; re-opens back off from {InitialMs} ms to {MaxMs} ms, further failures to this peer are summarised every {IntervalSeconds} s and logged in full at Debug",
                    peer, stage, cause.GetType().Name, cause.Message, initialMs, maxMs, intervalMs / 1000.0);
                break;

            case Line.Summary:
                logger.LogWarning(
                    "GrpcServerBatcher streams to {Peer} still failing: {RecentOpens} re-opens, {RecentEvictions} torn down and {RecentRefused} requests refused while backed off in the last {RecentSeconds:F0} s ({TotalOpens}, {TotalEvictions} and {TotalRefused} since the episode opened {EpisodeSeconds:F0} s ago); last ({Stage}): {ExType}: {Message}",
                    peer, recentOpens, recentEvictions, recentRefused, recentMs / 1000.0, totalOpens, totalEvictions, totalRefused, episodeMs / 1000.0, stage, cause.GetType().Name, cause.Message);
                break;

            default:
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug(
                        "GrpcServerBatcher streams to {Peer}: {Stage}: {ExType}: {Message}",
                        peer, stage, cause.GetType().Name, cause.Message);
                break;
        }
    }

    /// <summary>Caller holds <see cref="gate"/>.</summary>
    private void ScheduleNextOpen(long now)
    {
        nextOpenAllowedAtMs = now + backoffMs;
        backoffMs = Math.Min(backoffMs * 2, (long)ReopenBackoffMax.TotalMilliseconds);
    }
}

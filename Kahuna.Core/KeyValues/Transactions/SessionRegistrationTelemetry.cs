using System.Diagnostics;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The three calls a node makes to the node that owns a transaction's session: the idempotent registration
/// that opens an operation, the completion that folds its effect, and the working-set query. The tag of
/// <see cref="DurableTransactionMetrics.SessionRegistrationForwards"/>.
/// </summary>
internal enum SessionRegistrationOp
{
    Begin,
    Complete,
    WorkingSet
}

/// <summary>
/// Counts session-registration calls for one node, beside the process-wide meter counters. An in-process test
/// cluster shares the static meter across every node, so a per-node figure — how many calls ONE node made for
/// ONE transaction — is only readable here. Every method also feeds the meter, so there is one counting
/// implementation and the two views cannot drift.
///
/// <para>All counters are cumulative and thread-safe. A forwarded call is counted whether it answered or threw,
/// because both spent a network round trip; <see cref="SessionRegistrationCounts.Threw"/> is the subset that
/// threw. An attempt that found no session leader never left the node and is counted only under
/// <see cref="SessionRegistrationCounts.Unrouted"/>, so the forward counts stay an exact hop count.</para>
/// </summary>
internal sealed class SessionRegistrationTelemetry
{
    private long beginLocal;
    private long beginForwarded;
    private long completeLocal;
    private long completeForwarded;
    private long workingSetLocal;
    private long workingSetForwarded;
    private long refused;
    private long threw;
    private long unrouted;
    private long beginForwardSamples;
    private long beginForwardTicks;
    private long completeForwardSamples;
    private long completeForwardTicks;
    private long workingSetForwardSamples;
    private long workingSetForwardTicks;

    /// <summary>A registration served on this node because it holds the session.</summary>
    internal void Local(SessionRegistrationOp op, bool ok)
    {
        switch (op)
        {
            case SessionRegistrationOp.Begin:
                Interlocked.Increment(ref beginLocal);
                break;

            case SessionRegistrationOp.Complete:
                Interlocked.Increment(ref completeLocal);
                break;

            default:
                Interlocked.Increment(ref workingSetLocal);
                break;
        }

        if (!ok)
            Interlocked.Increment(ref refused);

        DurableTransactionMetrics.SessionRegistrationLocal(op, ok);
    }

    /// <summary>A registration this node sent to the session owner, and the answer it came back with.</summary>
    internal void Forwarded(SessionRegistrationOp op, bool ok)
    {
        CountForward(op);

        if (!ok)
            Interlocked.Increment(ref refused);

        DurableTransactionMetrics.SessionRegistrationForwarded(op, ok);
    }

    /// <summary>A registration this node sent to the session owner whose transport threw.</summary>
    internal void ForwardThrew(SessionRegistrationOp op)
    {
        CountForward(op);
        Interlocked.Increment(ref threw);

        DurableTransactionMetrics.SessionRegistrationForwardThrew(op);
    }

    /// <summary>A registration that had to be forwarded but found no session leader; no call left the node.</summary>
    internal void Unrouted(SessionRegistrationOp op)
    {
        Interlocked.Increment(ref unrouted);

        DurableTransactionMetrics.SessionRegistrationUnrouted(op);
    }

    /// <summary>
    /// Opens a forwarded session-registration call and reads the clock for it. The returned value carries the
    /// start timestamp to the call's single exit, so the count and the duration are reported together and the
    /// caller cannot report one without the other.
    ///
    /// <para>The clock is read only when a listener asked for
    /// <see cref="DurableTransactionMetrics.SessionRegistrationForwardMs"/>. With no listener the returned
    /// value carries no timestamp, the exit records no sample, and the whole instrument costs one boolean
    /// read on the hot path.</para>
    /// </summary>
    internal SessionRegistrationForward BeginForward(SessionRegistrationOp op) =>
        new(this, op, DurableTransactionMetrics.SessionRegistrationForwardMs.Enabled ? Stopwatch.GetTimestamp() : 0);

    /// <summary>
    /// Records how long one forwarded call took, when <paramref name="startTicks"/> carries a timestamp. The
    /// completion path of <see cref="SessionRegistrationForward"/>; call it through that type rather than
    /// directly, so no exit can record a duration without also counting the hop.
    /// </summary>
    internal void RecordForwardDuration(SessionRegistrationOp op, long startTicks)
    {
        if (startTicks == 0)
            return;

        long elapsedTicks = Stopwatch.GetTimestamp() - startTicks;

        // A monotonic clock cannot go backwards, but a zero-length sample is meaningful and a negative one is
        // not, so a clock that surprises us is clamped rather than allowed to skew the distribution.
        if (elapsedTicks < 0)
            elapsedTicks = 0;

        switch (op)
        {
            case SessionRegistrationOp.Begin:
                Interlocked.Increment(ref beginForwardSamples);
                Interlocked.Add(ref beginForwardTicks, elapsedTicks);
                break;

            case SessionRegistrationOp.Complete:
                Interlocked.Increment(ref completeForwardSamples);
                Interlocked.Add(ref completeForwardTicks, elapsedTicks);
                break;

            default:
                Interlocked.Increment(ref workingSetForwardSamples);
                Interlocked.Add(ref workingSetForwardTicks, elapsedTicks);
                break;
        }

        DurableTransactionMetrics.SessionRegistrationForwardTimed(op, TicksToMs(elapsedTicks));
    }

    private static double TicksToMs(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;

    private void CountForward(SessionRegistrationOp op)
    {
        switch (op)
        {
            case SessionRegistrationOp.Begin:
                Interlocked.Increment(ref beginForwarded);
                break;

            case SessionRegistrationOp.Complete:
                Interlocked.Increment(ref completeForwarded);
                break;

            default:
                Interlocked.Increment(ref workingSetForwarded);
                break;
        }
    }

    /// <summary>The session-registration calls this node has made so far.</summary>
    internal SessionRegistrationCounts Snapshot => new(
        Interlocked.Read(ref beginLocal),
        Interlocked.Read(ref beginForwarded),
        Interlocked.Read(ref completeLocal),
        Interlocked.Read(ref completeForwarded),
        Interlocked.Read(ref workingSetLocal),
        Interlocked.Read(ref workingSetForwarded),
        Interlocked.Read(ref refused),
        Interlocked.Read(ref threw),
        Interlocked.Read(ref unrouted));

    /// <summary>
    /// The wall time this node's forwarded session-registration calls have taken so far. Empty while no
    /// listener asked for the duration histogram, because the clock is read only for a listened instrument.
    /// </summary>
    internal SessionRegistrationForwardDurations DurationSnapshot => new(
        Interlocked.Read(ref beginForwardSamples),
        TicksToMs(Interlocked.Read(ref beginForwardTicks)),
        Interlocked.Read(ref completeForwardSamples),
        TicksToMs(Interlocked.Read(ref completeForwardTicks)),
        Interlocked.Read(ref workingSetForwardSamples),
        TicksToMs(Interlocked.Read(ref workingSetForwardTicks)));
}

/// <summary>
/// One forwarded session-registration call in flight: which call it is, and the timestamp it started at — or
/// zero when no listener asked for the duration, in which case no clock was read and the exit records no
/// sample. A value type, so an in-flight call costs no allocation even on a timed path.
///
/// <para>Obtain it from <see cref="SessionRegistrationTelemetry.BeginForward"/> immediately before the
/// transport call, and end it with <see cref="Answered"/> or <see cref="Threw"/> on every path out. Both exits
/// count the hop and record its duration, so a forward can never appear in the counter without appearing in
/// the histogram.</para>
/// </summary>
internal readonly struct SessionRegistrationForward
{
    private readonly SessionRegistrationTelemetry telemetry;

    private readonly SessionRegistrationOp op;

    private readonly long startTicks;

    internal SessionRegistrationForward(SessionRegistrationTelemetry telemetry, SessionRegistrationOp op, long startTicks)
    {
        this.telemetry = telemetry;
        this.op = op;
        this.startTicks = startTicks;
    }

    /// <summary>The forwarded call returned an answer; <paramref name="ok"/> is false for a rejection or a
    /// not-delivered reply, which cost the same round trip as an acceptance.</summary>
    internal void Answered(bool ok)
    {
        telemetry.RecordForwardDuration(op, startTicks);
        telemetry.Forwarded(op, ok);
    }

    /// <summary>The forwarded call's transport threw. The time it consumed is recorded, because the
    /// transaction waited for it whether or not it produced an answer.</summary>
    internal void Threw()
    {
        telemetry.RecordForwardDuration(op, startTicks);
        telemetry.ForwardThrew(op);
    }
}

/// <summary>
/// The wall time one node's forwarded session-registration calls have taken, beside the process-wide
/// histogram. An in-process test cluster shares the static meter across every node and the test project runs
/// classes in parallel, so a deterministic per-node figure is only readable here.
///
/// <para>Every field is zero unless a listener asked for
/// <see cref="DurableTransactionMetrics.SessionRegistrationForwardMs"/> while the calls ran: the clock is
/// read only for a listened instrument, so an unlistened path records nothing here either.</para>
/// </summary>
/// <param name="BeginSamples">Forwarded registrations timed, answered and thrown together.</param>
/// <param name="BeginTotalMs">Total wall time those registrations took.</param>
/// <param name="CompleteSamples">Forwarded completions timed.</param>
/// <param name="CompleteTotalMs">Total wall time those completions took.</param>
/// <param name="WorkingSetSamples">Forwarded working-set queries timed.</param>
/// <param name="WorkingSetTotalMs">Total wall time those queries took.</param>
internal readonly record struct SessionRegistrationForwardDurations(
    long BeginSamples,
    double BeginTotalMs,
    long CompleteSamples,
    double CompleteTotalMs,
    long WorkingSetSamples,
    double WorkingSetTotalMs);

/// <summary>
/// Session-registration calls one node made, split the way the cost model counts them: served locally because
/// this node holds the session, or forwarded to the node that does. An operation executed away from its
/// session node costs one forwarded begin and one forwarded complete; the local counts are the denominator
/// that turns those forwards into the remote share of registrations.
/// </summary>
/// <param name="BeginLocal">Registrations opened on this node's own session table.</param>
/// <param name="BeginForwarded">Registrations sent to the session owner.</param>
/// <param name="CompleteLocal">Completions folded into this node's own session table.</param>
/// <param name="CompleteForwarded">Completions sent to the session owner, including the ones a receiver
/// redirected after finding it had lost the coordinator partition.</param>
/// <param name="WorkingSetLocal">Working-set queries this node routed and then answered from its own session
/// table. A query another node forwarded here lands on the unrouted local entry point and is not counted, so
/// this field is the queries this node started, not every query it answered.</param>
/// <param name="WorkingSetForwarded">Working-set queries sent to the session owner.</param>
/// <param name="Refused">Calls, local or forwarded, whose answer was a rejection or a not-delivered reply.</param>
/// <param name="Threw">Forwarded calls whose transport threw; a subset of the forwarded counts.</param>
/// <param name="Unrouted">Calls that had to be forwarded and found no session leader, so no call left the
/// node; deliberately outside the forwarded counts, which stay an exact hop count.</param>
internal readonly record struct SessionRegistrationCounts(
    long BeginLocal,
    long BeginForwarded,
    long CompleteLocal,
    long CompleteForwarded,
    long WorkingSetLocal,
    long WorkingSetForwarded,
    long Refused,
    long Threw,
    long Unrouted);

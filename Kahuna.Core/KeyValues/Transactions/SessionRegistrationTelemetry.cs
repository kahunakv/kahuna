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
}

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

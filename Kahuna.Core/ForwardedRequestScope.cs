namespace Kahuna.Server;

/// <summary>
/// Ambient marker for "the request being served arrived from another Kahuna node", set by the
/// inter-node receive paths (the server-side gRPC batcher, and the in-memory transport's
/// locator-re-entering calls) and consulted by the routing in
/// <see cref="PartitionLeaderResolver"/>.
///
/// <para>
/// Its one job is loop safety. Every node resolves a forward target from its own local belief, and
/// local beliefs disagree during an election window or a placement-map transition: node A believes
/// B leads the partition while B believes A leads it. Unbudgeted, each node forwards to the other
/// for as long as the disagreement lasts. On the gRPC transport that is an unbounded request loop;
/// on the in-memory transport the forward is a direct in-process call, so the loop is mutual
/// recursion and the process dies of stack exhaustion. The budget in
/// <see cref="MaxForwardHops"/> caps the chain: past it a receiver answers <c>MustRetry</c>
/// instead of forwarding onward.
/// </para>
///
/// <para>
/// <c>MustRetry</c> is the honest answer in that window: the caller retries against a settled
/// view instead of riding a resolution neither node can make.
/// </para>
///
/// <para>
/// Carried on an <see cref="AsyncLocal{T}"/> so it flows through the whole async serving path of
/// one request without threading a parameter through every locator signature or a marker field
/// through every wire message. Entering returns a scope that must be disposed by the same async
/// flow that entered it.
/// </para>
/// </summary>
internal static class ForwardedRequestScope
{
    /// <summary>
    /// How many times one operation may be forwarded before a receiver must answer <c>MustRetry</c>
    /// instead of forwarding it onward.
    ///
    /// <para>Two, so that the one useful second hop survives. A node that does not host the
    /// partition forwards to a replica it guessed from the committed map; that receiver hosts the
    /// range, so its own leader resolution is strictly better than the guess and is worth one more
    /// hop. A third hop buys nothing: every node past the first resolves from the same kind of
    /// local belief, so a longer chain only lets two disagreeing nodes bounce the operation. Two
    /// also bounds the in-process recursion on the in-memory transport to a fixed two frames.</para>
    /// </summary>
    public const int MaxForwardHops = 2;

    /// <summary>
    /// Hard ceiling on how deep forwards may nest inside one serving flow, enforced by
    /// <see cref="Enter"/>. <see cref="MaxForwardHops"/> keeps one chain at 2, and
    /// <see cref="Suppress"/> starts a new chain for the sub-operations of a transaction, so a
    /// legitimate flow nests a handful of levels at most. A flow that reaches this depth has found
    /// a loop the hop budget does not cover; the bound turns it into a thrown, diagnosable failure
    /// of that one operation instead of a stack overflow that kills the process.
    /// </summary>
    private const int MaxNestedForwards = 16;

    private static readonly AsyncLocal<ForwardState> state = new();

    /// <summary>Whether the current async flow is serving a request forwarded by another node.</summary>
    public static bool IsActive => state.Value.ChainedHops > 0;

    /// <summary>
    /// How many times the operation now being served was already forwarded. Zero on the node a
    /// client reached; <see cref="MaxForwardHops"/> or more means it may not be forwarded again.
    /// </summary>
    public static int ChainedHops => state.Value.ChainedHops;

    /// <summary>Whether the operation now being served may still be forwarded to another node.</summary>
    public static bool CanForward => state.Value.ChainedHops < MaxForwardHops;

    /// <summary>
    /// Marks the current async flow (and everything it awaits) as serving a forwarded request.
    /// </summary>
    /// <exception cref="ForwardLoopException">
    /// The flow already nests <see cref="MaxNestedForwards"/> forwards. See that constant.
    /// </exception>
    public static Scope Enter()
    {
        ForwardState previous = state.Value;

        if (previous.NestedHops >= MaxNestedForwards)
            throw new ForwardLoopException(previous.NestedHops);

        state.Value = new ForwardState(previous.ChainedHops + 1, previous.NestedHops + 1);

        return new Scope(previous);
    }

    /// <summary>
    /// Marks the current async flow as serving a request that the sender says it had already
    /// forwarded <paramref name="arrivedAtHops"/> times, plus this hop. Used by the inter-node
    /// receive paths that carry the count on the wire: the budget in
    /// <see cref="MaxForwardHops"/> has to span the whole chain, and an
    /// <see cref="AsyncLocal{T}"/> stops at the process boundary. A count below the one this hop
    /// implies is raised to it, so an old or absent field on the wire still marks the request
    /// forwarded.
    /// </summary>
    /// <exception cref="ForwardLoopException">
    /// The flow already nests <see cref="MaxNestedForwards"/> forwards. See that constant.
    /// </exception>
    public static Scope EnterAt(int arrivedAtHops)
    {
        ForwardState previous = state.Value;

        if (previous.NestedHops >= MaxNestedForwards)
            throw new ForwardLoopException(previous.NestedHops);

        int chained = Math.Max(previous.ChainedHops + 1, arrivedAtHops);

        state.Value = new ForwardState(chained, previous.NestedHops + 1);

        return new Scope(previous);
    }

    /// <summary>Restores the marker captured when <see cref="Enter"/> was entered; the value
    /// captured by tasks spawned inside the scope is unaffected.</summary>
    public readonly struct Scope(ForwardState previous) : IDisposable
    {
        public void Dispose() => state.Value = previous;
    }

    /// <summary>
    /// Clears the marker for the current async flow so that work the serving node initiates on its
    /// own behalf routes normally. The loop-safety rule exists to stop the <em>same</em> operation
    /// from being forwarded onward by a receiver; once an operation has reached the component that
    /// serves it (a transaction coordinator running 2PC, a leader resolving a foreign intent's
    /// anchor record), the sub-operations it spawns are new operations with their own targets —
    /// refusing to route them would wedge every cross-partition transaction whose commit arrived
    /// forwarded. Ping-pong stays impossible: each sub-operation starts a fresh chain, and that
    /// chain is budgeted like any other.
    /// </summary>
    public static SuppressScope Suppress()
    {
        ForwardState previous = state.Value;

        // The nesting depth is deliberately carried across the suppression: it bounds how deep one
        // serving flow may recurse in-process, which a new sub-operation adds to just as a chained
        // forward does.
        state.Value = previous with { ChainedHops = 0 };

        return new SuppressScope(previous);
    }

    /// <summary>Restores the marker captured when <see cref="Suppress"/> was entered.</summary>
    public readonly struct SuppressScope(ForwardState previous) : IDisposable
    {
        public void Dispose() => state.Value = previous;
    }

    /// <summary>
    /// <paramref name="ChainedHops"/> counts how many times the operation now being served was
    /// forwarded without an intervening <see cref="Suppress"/>; it is what
    /// <see cref="MaxForwardHops"/> budgets.
    /// <paramref name="NestedHops"/> counts every forward the serving flow nests, suppressions
    /// included, and is what <see cref="MaxNestedForwards"/> bounds.
    /// </summary>
    public readonly record struct ForwardState(int ChainedHops, int NestedHops);
}

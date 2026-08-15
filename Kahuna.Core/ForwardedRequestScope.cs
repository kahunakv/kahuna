
namespace Kahuna.Server;

/// <summary>
/// Ambient marker for "the request being served arrived from another Kahuna node", set by the
/// inter-node receive paths (the server-side gRPC batcher, and the in-memory transport's
/// locator-re-entering calls) and consulted by the placement routing in
/// <see cref="PartitionLeaderResolver"/>.
///
/// <para>
/// Its one job is loop safety for replica-placement forwarding: a node that does <b>not</b> host a
/// partition may forward an operation to a replica only when it is the <em>originator</em>. A
/// receiver that finds itself non-hosting (the sender routed on a stale placement view) must
/// answer <c>MustRetry</c> instead of forwarding onward — two nodes with mutually stale views
/// would otherwise bounce the operation between them until a timeout. A receiver that hosts the
/// partition may still redirect once to the partition's leader, exactly as it does today; that
/// resolution is accurate because the partition is materialized locally.
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
    private static readonly AsyncLocal<bool> active = new();

    /// <summary>Whether the current async flow is serving a request forwarded by another node.</summary>
    public static bool IsActive => active.Value;

    /// <summary>Marks the current async flow (and everything it awaits) as serving a forwarded request.</summary>
    public static Scope Enter()
    {
        active.Value = true;
        return default;
    }

    /// <summary>Clears the marker for code following the scope in the entering flow; the value
    /// captured by tasks spawned inside the scope is unaffected.</summary>
    public readonly struct Scope : IDisposable
    {
        public void Dispose() => active.Value = false;
    }

    /// <summary>
    /// Clears the marker for the current async flow so that work the serving node initiates on its
    /// own behalf routes normally. The loop-safety rule exists to stop the <em>same</em> operation
    /// from being forwarded onward by a non-hosting receiver; once an operation has reached the
    /// component that serves it (a transaction coordinator running 2PC, a leader resolving a foreign
    /// intent's anchor record), the sub-operations it spawns are new operations with their own
    /// targets — refusing to route them would wedge every cross-partition transaction whose commit
    /// arrived forwarded. Ping-pong stays impossible: each sub-operation's receiver still serves
    /// locally or answers MustRetry, never forwards onward.
    /// </summary>
    public static SuppressScope Suppress()
    {
        bool wasActive = active.Value;
        active.Value = false;
        return new SuppressScope(wasActive);
    }

    /// <summary>Restores the marker captured when <see cref="Suppress"/> was entered.</summary>
    public readonly struct SuppressScope(bool wasActive) : IDisposable
    {
        public void Dispose() => active.Value = wasActive;
    }
}

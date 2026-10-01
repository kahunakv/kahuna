
namespace Kahuna.Server.KeyValues;

/// <summary>
/// The leadership a lock was granted under: the partition whose leader holds the lock in its memory, the Raft
/// term that leader was confirmed in when it granted it, and a key that routes to the partition.
///
/// <para>A point lock, a prefix lock and a range lock are all in-memory state of the partition leader. A leader
/// change drops them without telling their holder, and the next leader grants the same keys to another
/// transaction. The term is what makes that loss detectable: a term has exactly one leader, and a leader that
/// stops leading can only lead again in a later term. So when the confirmed leader of the partition still
/// reports this term at commit, it is the node that granted the lock and it has led without interruption since,
/// which means the lock is still in its memory. Any other answer means the lock may be gone.</para>
///
/// <para><see cref="RoutingKey"/> lets the holder ask that question through a key-routed probe: for a point
/// lock it is the locked key, for a prefix or range lock a key of the locked key space that the partition
/// serves.</para>
/// </summary>
internal readonly record struct LockGrantTerm(int PartitionId, long Term, string RoutingKey);

/// <summary>
/// The grants collected while one lock request ran. Written by the locators on the node that granted, possibly
/// from the parallel per-node fan-out of a many-key acquire, so the list is guarded.
/// </summary>
internal sealed class LockGrantCapture
{
    private readonly Lock sync = new();

    private List<LockGrantTerm>? grants;

    /// <summary>Records one grant. A partition is kept once per term: a second grant under the same term adds
    /// nothing a commit-time check needs, and one under another term is the evidence of a leader change that
    /// the holder must see.</summary>
    public void Record(LockGrantTerm grant)
    {
        lock (sync)
        {
            grants ??= [];

            foreach (LockGrantTerm existing in grants)
                if (existing.PartitionId == grant.PartitionId && existing.Term == grant.Term)
                    return;

            grants.Add(grant);
        }
    }

    /// <summary>The grants recorded so far, or null when there are none. The returned list is a copy.</summary>
    public List<LockGrantTerm>? Take()
    {
        lock (sync)
            return grants is { Count: > 0 } ? [.. grants] : null;
    }
}

/// <summary>
/// Ambient collection point for the leadership terms the locks of an in-flight request are granted under.
///
/// <para>The term is known where the lock is granted: deep in a locator, on the node that leads the partition,
/// after it confirmed its leadership. The party that needs it is the transaction's coordinator, several layers
/// above and possibly on another node. Carrying it back explicitly would widen the return type of every lock
/// method of every locator, manager, transport and <c>IKahuna</c> member on the way. An
/// <see cref="AsyncLocal{T}"/> carries it instead, as <see cref="Routing.RouteCaptureScope"/> does for route
/// hints: the caller opens a capture, the locator that grants writes into it, and a transport that crosses a
/// process boundary copies the remote node's capture into its response and replays it into the caller's.</para>
///
/// <para>A lock request served while no capture is open records nothing and costs one null check.</para>
/// </summary>
internal static class LockGrantScope
{
    private static readonly AsyncLocal<LockGrantCapture?> current = new();

    /// <summary>The capture the current request writes into, or null when none is open.</summary>
    public static LockGrantCapture? Current => current.Value;

    /// <summary>Opens a capture for the current async flow and everything it awaits. The returned scope must be
    /// disposed by the same flow.</summary>
    public static Scope Begin(out LockGrantCapture capture)
    {
        LockGrantCapture? previous = current.Value;

        capture = new LockGrantCapture();
        current.Value = capture;

        return new Scope(previous);
    }

    /// <summary>Records a grant into the open capture, if one is open.</summary>
    public static void Record(int partitionId, long term, string routingKey) =>
        current.Value?.Record(new LockGrantTerm(partitionId, term, routingKey));

    /// <summary>Restores the capture that was open when this scope was entered.</summary>
    public readonly struct Scope(LockGrantCapture? previous) : IDisposable
    {
        public void Dispose() => current.Value = previous;
    }
}

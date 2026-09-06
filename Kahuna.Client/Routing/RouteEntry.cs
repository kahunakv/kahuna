using Kahuna.Shared.Routing;

namespace Kahuna.Client.Routing;

/// <summary>
/// One learned route. Immutable: a change replaces the whole instance, so a reader either sees the
/// entry it looked up or a complete replacement, never a half-updated one, and a writer can
/// compare-and-swap on the instance it observed.
/// </summary>
internal sealed class RouteEntry
{
    /// <summary>
    /// The client-reachable URL to send the resource's operations to, already resolved through the
    /// endpoint policy and, wherever possible, interned to the configured URL instance so the
    /// transport's per-URL connection pool hits its existing entry.
    /// </summary>
    public readonly string Endpoint;

    /// <summary>The partition the server said the resource belongs to. Diagnostic; never a gate.</summary>
    public readonly int PartitionId;

    /// <summary>
    /// The range descriptor generation the server routed through, or 0 for a hash-routed space.
    /// Diagnostic: the server re-applies its own live fence on every request, so the client never
    /// decides anything from this.
    /// </summary>
    public readonly long Generation;

    public readonly KahunaRouteProvenance Provenance;

    /// <summary>
    /// Monotonic local deadline (<see cref="Environment.TickCount64"/>) past which the entry is
    /// ignored. Monotonic because it measures elapsed local time; wall-clock time would move under
    /// a clock adjustment, and ordering distributed events by it is never correct.
    /// </summary>
    public readonly long ExpiresAtTicks;

    public RouteEntry(string endpoint, int partitionId, long generation, KahunaRouteProvenance provenance, long expiresAtTicks)
    {
        Endpoint = endpoint;
        PartitionId = partitionId;
        Generation = generation;
        Provenance = provenance;
        ExpiresAtTicks = expiresAtTicks;
    }

    public bool IsValidAt(long nowTicks) => nowTicks < ExpiresAtTicks;
}

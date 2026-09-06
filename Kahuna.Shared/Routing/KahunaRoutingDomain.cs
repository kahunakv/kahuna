namespace Kahuna.Shared.Routing;

/// <summary>
/// The namespace a routed resource name belongs to.
///
/// <para>
/// Kahuna routes locks, key-value keys and sequences through separate subsystems. The three name
/// spaces overlap — a lock and a key may both be called <c>"orders/1"</c> — and a sequence does
/// not even route by its own name but by the partition of its storage key. Every cached route and
/// every wire hint therefore carries the domain alongside the resource, so one subsystem's route
/// can never be served to another.
/// </para>
/// </summary>
public enum KahunaRoutingDomain : byte
{
    /// <summary>A key-value key, routed by hash of its key space or by a range descriptor.</summary>
    KeyValue = 0,

    /// <summary>A distributed lock resource, routed by hash of its key space.</summary>
    Lock = 1,

    /// <summary>A named sequence, routed by the partition of its server-side storage key.</summary>
    Sequence = 2
}

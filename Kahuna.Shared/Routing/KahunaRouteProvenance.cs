namespace Kahuna.Shared.Routing;

/// <summary>
/// Where the endpoint carried by a routing hint came from. A client may use both usable values to
/// pick a destination; the distinction says how much the hint proves, not whether it is usable.
/// </summary>
public enum KahunaRouteProvenance : byte
{
    /// <summary>No route was resolved. The rest of the hint carries nothing.</summary>
    Unknown = 0,

    /// <summary>
    /// The answering node resolved itself as the owner and ran the operation. The endpoint is the
    /// final serving destination.
    /// </summary>
    Executed = 1,

    /// <summary>
    /// The answering node forwarded the operation to this endpoint. The receiver re-resolves
    /// ownership on arrival, so the endpoint is a best-effort suggestion, not a proven executor.
    /// </summary>
    Forwarded = 2
}

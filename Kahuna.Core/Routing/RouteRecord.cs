using Kahuna.Shared.Routing;

namespace Kahuna.Server.Routing;

/// <summary>
/// One resolved route, as the node that resolved it saw it: which partition the resource belongs
/// to, which client-reachable endpoint serves that partition, and how much the answer proves.
/// </summary>
/// <param name="PartitionId">The partition the resource resolved to.</param>
/// <param name="Endpoint">
/// The advertised client endpoint of the resolved node. Empty when that node advertises none, in
/// which case no hint is emitted.
/// </param>
/// <param name="Provenance">
/// <see cref="KahunaRouteProvenance.Executed"/> when this node owned and ran the operation;
/// <see cref="KahunaRouteProvenance.Forwarded"/> when it sent the operation to
/// <paramref name="Endpoint"/>, which re-resolves ownership on arrival.
/// </param>
/// <param name="Generation">
/// Committed generation of the range descriptor that admitted the resource, or 0 when no
/// descriptor was consulted (a hash-routed key space).
/// </param>
internal readonly record struct RouteRecord(
    int PartitionId,
    string Endpoint,
    KahunaRouteProvenance Provenance,
    long Generation
);

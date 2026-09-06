using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.Routing;

namespace Kahuna.Server.Routing;

/// <summary>
/// Turns the routes an in-flight request recorded into the advisory hints its response carries.
///
/// <para>
/// A hint is emitted only for a resource whose owner was actually resolved. Nothing is invented:
/// an operation refused before routing, or served by a node that advertises no client endpoint,
/// answers with no hint at all, which leaves the caller on its existing endpoint selection rather
/// than sending it somewhere a guess named.
/// </para>
/// </summary>
internal static class RouteHintWriter
{
    /// <summary>The gRPC hint for <paramref name="resource"/>, or null when none was resolved.</summary>
    public static GrpcRouteHint? Grpc(RouteCapture? capture, KahunaRoutingDomain domain, string resource)
    {
        if (capture is null || !capture.TryGet(domain, resource, out RouteRecord record))
            return null;

        return new GrpcRouteHint
        {
            PartitionId = record.PartitionId,
            Endpoint = record.Endpoint,
            Provenance = (GrpcRouteProvenance)record.Provenance,
            Generation = record.Generation
        };
    }

    /// <summary>The REST hint for <paramref name="resource"/>, or null when none was resolved.</summary>
    public static KahunaRouteHint? Rest(RouteCapture? capture, KahunaRoutingDomain domain, string resource)
    {
        if (capture is null || !capture.TryGet(domain, resource, out RouteRecord record))
            return null;

        return new KahunaRouteHint
        {
            PartitionId = record.PartitionId,
            Endpoint = record.Endpoint,
            Provenance = record.Provenance,
            Generation = record.Generation
        };
    }

    /// <summary>
    /// Accumulates the deduplicated hint table a batched response carries, and hands out the 1-based
    /// index each item points at. One endpoint string is written once however many items resolved to
    /// it, so a thousand-key batch does not repeat it a thousand times.
    /// </summary>
    public sealed class Table
    {
        private readonly RouteCapture? capture;

        private Dictionary<RouteRecord, int>? indexes;

        private List<RouteRecord>? records;

        public Table(RouteCapture? capture) => this.capture = capture is null || capture.IsEmpty ? null : capture;

        /// <summary>Whether any item resolved a route, so an empty table is never attached.</summary>
        public bool HasRoutes => records is { Count: > 0 };

        /// <summary>
        /// The 1-based table index for <paramref name="resource"/>, or 0 when no route was resolved
        /// for it.
        /// </summary>
        public int IndexOf(KahunaRoutingDomain domain, string resource)
        {
            if (capture is null || resource.Length == 0 || !capture.TryGet(domain, resource, out RouteRecord record))
                return 0;

            indexes ??= new();

            if (indexes.TryGetValue(record, out int existing))
                return existing;

            records ??= new();
            records.Add(record);

            int index = records.Count;
            indexes[record] = index;
            return index;
        }

        /// <summary>The accumulated table as gRPC hints, in index order.</summary>
        public IEnumerable<GrpcRouteHint> ToGrpc()
        {
            if (records is null)
                yield break;

            foreach (RouteRecord record in records)
                yield return new GrpcRouteHint
                {
                    PartitionId = record.PartitionId,
                    Endpoint = record.Endpoint,
                    Provenance = (GrpcRouteProvenance)record.Provenance,
                    Generation = record.Generation
                };
        }

        /// <summary>The accumulated table as REST hints, in index order.</summary>
        public List<KahunaRouteHint>? ToRest()
        {
            if (records is null)
                return null;

            List<KahunaRouteHint> hints = new(records.Count);

            foreach (RouteRecord record in records)
                hints.Add(new KahunaRouteHint
                {
                    PartitionId = record.PartitionId,
                    Endpoint = record.Endpoint,
                    Provenance = record.Provenance,
                    Generation = record.Generation
                });

            return hints;
        }
    }
}

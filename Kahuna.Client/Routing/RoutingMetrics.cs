using System.Diagnostics.Metrics;

namespace Kahuna.Client.Routing;

/// <summary>
/// Counters for what the routing cache is doing. Every dimension is a fixed, small set — an
/// operation family, a mode, a reason — so the series count stays bounded. Resource names, owner
/// tokens, transaction ids and endpoint URLs are never used as labels: each of those is unbounded
/// in a real workload and would turn one counter into millions of series.
/// </summary>
internal static class RoutingMetrics
{
    internal const string MeterName = "Kahuna.Client.Routing";

    private static readonly Meter Meter = new(MeterName, "1.0.0");

    /// <summary>An operation that found a live cached route and used it.</summary>
    public static readonly Counter<long> CacheHits =
        Meter.CreateCounter<long>("kahuna.client.routing.cache_hits", "operations");

    /// <summary>An operation that found no live cached route and fell back to endpoint rotation.</summary>
    public static readonly Counter<long> CacheMisses =
        Meter.CreateCounter<long>("kahuna.client.routing.cache_misses", "operations");

    /// <summary>An operation routed from metadata rather than from a learned route.</summary>
    public static readonly Counter<long> MetadataHits =
        Meter.CreateCounter<long>("kahuna.client.routing.metadata_hits", "operations");

    /// <summary>A response hint that was accepted and stored.</summary>
    public static readonly Counter<long> HintsLearned =
        Meter.CreateCounter<long>("kahuna.client.routing.hints_learned", "hints");

    /// <summary>
    /// A response hint that was not stored. The <c>reason</c> dimension takes a fixed set of values:
    /// <c>endpoint_rejected</c>, <c>superseded</c>, <c>unknown_provenance</c>.
    /// </summary>
    public static readonly Counter<long> HintsRejected =
        Meter.CreateCounter<long>("kahuna.client.routing.hints_rejected", "hints");

    /// <summary>An endpoint put into its failure cooldown after a transport failure.</summary>
    public static readonly Counter<long> EndpointsSuppressed =
        Meter.CreateCounter<long>("kahuna.client.routing.endpoints_suppressed", "endpoints");

    /// <summary>A cached route skipped because its endpoint was in its failure cooldown.</summary>
    public static readonly Counter<long> SuppressedRoutesSkipped =
        Meter.CreateCounter<long>("kahuna.client.routing.suppressed_routes_skipped", "operations");

    /// <summary>A routing-metadata read that was issued.</summary>
    public static readonly Counter<long> MetadataRefreshes =
        Meter.CreateCounter<long>("kahuna.client.routing.metadata_refreshes", "reads");

    /// <summary>A routing-metadata read that another caller's in-flight read served instead.</summary>
    public static readonly Counter<long> MetadataRefreshesCoalesced =
        Meter.CreateCounter<long>("kahuna.client.routing.metadata_refreshes_coalesced", "reads");

    /// <summary>
    /// A routing-metadata read that produced no usable map. The <c>reason</c> dimension takes a
    /// fixed set of values: <c>unavailable</c>, <c>incoherent</c>, <c>unsupported_schema</c>,
    /// <c>unsupported_hash</c>, <c>not_initialized</c>.
    /// </summary>
    public static readonly Counter<long> MetadataRefreshFailures =
        Meter.CreateCounter<long>("kahuna.client.routing.metadata_refresh_failures", "reads");
}

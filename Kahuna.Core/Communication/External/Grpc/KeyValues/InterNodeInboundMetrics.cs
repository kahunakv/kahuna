using System.Diagnostics.Metrics;

namespace Kahuna.Communication.External.Grpc.KeyValues;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the forwarded durable requests a node serves on its
/// inbound inter-node streams. Those requests start as soon as the stream delivers them, so how many a node holds
/// at once is set by its senders' offered load and by how long each takes to answer, not by the stream. The write
/// aggregator's queued-items gauge covers only the part of that time a request spends admitted to the aggregator:
/// a request that is still resolving the partition's leadership, is redirecting to another leader, waits for the
/// ordered apply of its committed batch, applies a resolution on the key-value actors (which never enter the
/// aggregator), or waits for its response write is invisible there. The tag set is fixed: one value per durable
/// request type.
/// </summary>
internal static class InterNodeInboundMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Forwarded durable requests this node has started serving and not yet answered, tagged by <c>request</c>.
    /// A request is counted from the moment its handler starts until its response is written or its handler
    /// fails, whichever ends it, so the gauge returns to zero when the node's inbound durable work drains.
    /// </summary>
    internal static readonly UpDownCounter<long> DurableInFlight =
        Meter.CreateUpDownCounter<long>(
            "kahuna.internode.inbound_durable_in_flight",
            unit: "{request}",
            description: "Forwarded durable requests this node is serving for remote senders and has not answered yet, tagged by request type.");

    internal static readonly KeyValuePair<string, object?> DurableOperation = new("request", "durable_operation");

    internal static readonly KeyValuePair<string, object?> DurableBundle = new("request", "durable_bundle");

    internal static readonly KeyValuePair<string, object?> DurableOnePhase = new("request", "durable_one_phase");

    internal static readonly KeyValuePair<string, object?> DurableDecision = new("request", "durable_decision");

    internal static readonly KeyValuePair<string, object?> LookupRecord = new("request", "lookup_record");

    /// <summary>Counts one forwarded durable request of the tagged type as started.</summary>
    internal static void DurableStarted(KeyValuePair<string, object?> request) => DurableInFlight.Add(1, request);

    /// <summary>Counts one forwarded durable request of the tagged type as answered or failed.</summary>
    internal static void DurableEnded(KeyValuePair<string, object?> request) => DurableInFlight.Add(-1, request);
}

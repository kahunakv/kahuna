using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the partition write aggregator, recorded at the
/// lane's single-threaded ownership point. All tags are low-cardinality (a bounded reason/outcome string) —
/// never a key, partition id, request id, or transaction id. The primary effectiveness signal is
/// <c>dispatched entries / dispatched batches</c>: under a coalescing burst it approaches the batch cap, not one.
/// </summary>
internal static class PartitionWriteAggregatorMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    internal static readonly Counter<long> AdmittedItems =
        Meter.CreateCounter<long>("kahuna.kv.write.admitted", description: "Direct writes admitted to the aggregator.");

    internal static readonly Counter<long> Rejections =
        Meter.CreateCounter<long>("kahuna.kv.write.rejections", description: "Admissions/dispatches rejected or released, tagged by reason.");

    internal static readonly Counter<long> DispatchedBatches =
        Meter.CreateCounter<long>("kahuna.kv.write.batches", description: "Raft batches dispatched by the aggregator.");

    internal static readonly Counter<long> DispatchedEntries =
        Meter.CreateCounter<long>("kahuna.kv.write.entries", description: "Log entries dispatched across all aggregator batches.");

    internal static readonly Counter<long> BatchOutcomes =
        Meter.CreateCounter<long>("kahuna.kv.write.outcomes", description: "Batch outcomes tagged success/transient/permanent.");

    internal static readonly Histogram<int> BatchItemCount =
        Meter.CreateHistogram<int>("kahuna.kv.write.batch_items", unit: "{entries}", description: "Entries per dispatched batch.");

    internal static readonly Histogram<long> BatchBytes =
        Meter.CreateHistogram<long>("kahuna.kv.write.batch_bytes", unit: "By", description: "Serialized bytes per dispatched batch.");

    internal static readonly Histogram<long> QueueAgeMs =
        Meter.CreateHistogram<long>("kahuna.kv.write.queue_age", unit: "ms", description: "Age of the oldest item in a dispatched batch.");

    internal static readonly Histogram<double> RaftDurationMs =
        Meter.CreateHistogram<double>("kahuna.kv.write.raft_duration", unit: "ms", description: "Aggregator Raft-call duration.");

    private static readonly KeyValuePair<string, object?> ReasonQueueFull = new("reason", "queue_full");
    private static readonly KeyValuePair<string, object?> ReasonInboxFull = new("reason", "inbox_full");
    private static readonly KeyValuePair<string, object?> ReasonStopping = new("reason", "stopping");
    private static readonly KeyValuePair<string, object?> ReasonFenceStale = new("reason", "fence_stale");
    private static readonly KeyValuePair<string, object?> ReasonQueueExpired = new("reason", "queue_expired");
    private static readonly KeyValuePair<string, object?> OutcomeSuccess = new("outcome", "success");
    private static readonly KeyValuePair<string, object?> OutcomeTransient = new("outcome", "transient");
    private static readonly KeyValuePair<string, object?> OutcomePermanent = new("outcome", "permanent");

    internal static void RejectedQueueFull() => Rejections.Add(1, ReasonQueueFull);
    internal static void RejectedInboxFull() => Rejections.Add(1, ReasonInboxFull);
    internal static void RejectedStopping() => Rejections.Add(1, ReasonStopping);
    internal static void ReleasedFenceStale() => Rejections.Add(1, ReasonFenceStale);
    internal static void ReleasedQueueExpired() => Rejections.Add(1, ReasonQueueExpired);

    internal static void BatchDispatched(int entries, long bytes, long oldestAgeMs)
    {
        DispatchedBatches.Add(1);
        DispatchedEntries.Add(entries);
        BatchItemCount.Record(entries);
        BatchBytes.Record(bytes);
        QueueAgeMs.Record(oldestAgeMs);
    }

    internal static void BatchSettled(bool success, bool transient, double durationMs)
    {
        RaftDurationMs.Record(durationMs);
        BatchOutcomes.Add(1, success ? OutcomeSuccess : transient ? OutcomeTransient : OutcomePermanent);
    }

    /// <summary>
    /// Creates the observable gauges for a facade's admission state on a fresh, <b>instance-owned</b>
    /// <see cref="Meter"/> and returns it. The caller must dispose the returned meter when the aggregator
    /// stops — the gauge callbacks capture <paramref name="admission"/>, so a static registration would keep
    /// every disposed node's registry reachable and accumulate duplicate instruments across construction. This
    /// mirrors the instance-meter ownership of <c>SnapshotFloorStore</c>. Counters/histograms above stay on the
    /// shared static meter: they record by tag and capture no per-instance state, so they do not leak.
    /// </summary>
    internal static Meter RegisterGauges(PartitionAdmissionRegistry admission)
    {
        Meter gaugeMeter = new("Kahuna", "1.0");
        gaugeMeter.CreateObservableGauge("kahuna.kv.write.queued_items", admission.TotalReservedItems, description: "Items admitted but not yet completed, across partitions.");
        gaugeMeter.CreateObservableGauge("kahuna.kv.write.queued_bytes", admission.TotalReservedBytes, unit: "By", description: "Serialized bytes admitted but not yet completed.");
        gaugeMeter.CreateObservableGauge("kahuna.kv.write.in_flight_partitions", () => (long)admission.InFlightPartitions, description: "Partitions with a batch awaiting its Raft result.");
        return gaugeMeter;
    }
}

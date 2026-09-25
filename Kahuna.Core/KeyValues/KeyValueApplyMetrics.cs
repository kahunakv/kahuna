
using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Metrics for the per-partition apply fingerprint (<see cref="Data.KeyValueApplyFingerprint"/>): the
/// applied kv log id per partition as a gauge, and the count of divergences the promotion-time and
/// split-time comparisons detected. The committed-head count per partition is already exported as
/// <c>kahuna.durable_tx.committed_head_ledger_entries</c>; together with the applied id gauge two
/// replicas disagreeing is visible from a metrics scrape instead of a post-hoc diff of node logs.
/// </summary>
internal static class KeyValueApplyMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Replicas found holding a different committed-head count than their leader at the same applied
    /// kv log id, by the comparison a new leader runs at promotion and a split runs before its copy.
    /// Any non-zero value is a replica whose apply stream diverged from the log.
    /// </summary>
    internal static readonly Counter<long> DivergenceDetected =
        Meter.CreateCounter<long>(
            "kahuna.keyvalues.apply_divergence_detected",
            description: "Replicas whose committed-head or live-intent count differed from the leader's at the same applied kv log id.");

    /// <summary>
    /// Containment actions taken on this node for a partition whose local projection was proven
    /// incomplete, tagged by <c>action</c>: <c>gated</c> (serving refused until a whole-partition
    /// install), <c>transferred</c> (leadership handed to the fuller peer), <c>stepped_down</c>
    /// (leadership dropped when the transfer was refused), <c>relinquish_failed</c>.
    /// </summary>
    internal static readonly Counter<long> DivergenceContained =
        Meter.CreateCounter<long>(
            "kahuna.keyvalues.apply_divergence_contained",
            description: "Containment actions taken for a partition whose local apply projection was proven incomplete, by action.");

    /// <summary>Gated partitions whose projection a whole-partition install has since replaced.</summary>
    internal static readonly Counter<long> DivergenceRepaired =
        Meter.CreateCounter<long>(
            "kahuna.keyvalues.apply_divergence_repaired",
            description: "Gated partitions whose local projection a whole-partition snapshot install replaced.");

    /// <summary>
    /// Leader-change fingerprint comparisons that ended their retry window without comparing every peer
    /// (unreachable, or never observed at the leader's applied log id). Not a divergence, but not a pass.
    /// </summary>
    internal static readonly Counter<long> InconclusiveComparisons =
        Meter.CreateCounter<long>(
            "kahuna.keyvalues.apply_fingerprint_inconclusive",
            description: "Leader-change fingerprint comparisons that could not compare every peer inside the retry window.");

    /// <summary>
    /// Registers the applied-log-id gauge on an instance-owned meter. The caller disposes the returned
    /// meter on teardown so a disposed node's dispatcher is not kept reachable by the callback.
    /// </summary>
    internal static Meter RegisterGauges(Func<IReadOnlyList<(int PartitionId, long AppliedLogId)>> appliedLogIds)
    {
        Meter gaugeMeter = new("Kahuna", "1.0");

        // Tagged by partition: cardinality is the node's hosted partition count, small and bounded.
        gaugeMeter.CreateObservableGauge("kahuna.keyvalues.applied_log_id",
            () => AppliedMeasurements(appliedLogIds()),
            description: "Highest kv log id the key-value subsystem applied, per partition.");

        return gaugeMeter;
    }

    private static IEnumerable<Measurement<long>> AppliedMeasurements(IReadOnlyList<(int PartitionId, long AppliedLogId)> applied)
    {
        List<Measurement<long>> measurements = new(applied.Count);
        foreach ((int partitionId, long appliedLogId) in applied)
            measurements.Add(new Measurement<long>(appliedLogId, new KeyValuePair<string, object?>("partition", partitionId)));
        return measurements;
    }
}

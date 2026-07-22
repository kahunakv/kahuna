using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the durable-intent 2PC finalize path. The decision
/// deadline is safety-critical: it decides whether a slow-but-alive coordinator's commit is honored or presumed
/// aborted by recovery. A deadline set too low spuriously aborts healthy transactions under load; one set too
/// high delays recovery of genuinely dead coordinators. These instruments make a mis-tuned deadline visible
/// rather than letting it silently convert live commits into aborts.
///
/// <para>Instrument naming follows the OpenTelemetry semantic conventions (dot-separated lowercase); Prometheus
/// exporters typically translate dots to underscores automatically. Counters are cumulative and thread-safe.</para>
/// </summary>
internal static class DurableTransactionMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Commits rejected because the attempt's HLC passed the transaction's frozen decision deadline, so the
    /// canonical record stayed <c>Undecided</c> and the transaction yields to presumed-abort recovery. A rising
    /// rate is the signal that the deadline is too tight for the current finalize latency — healthy transactions
    /// are being converted into aborts. Alert on any sustained non-zero rate.
    /// </summary>
    internal static readonly Counter<long> LateCommitRejections =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.late_commit_rejections",
            description: "Durable commits rejected because the attempt passed the frozen decision deadline.");

    /// <summary>
    /// Recovery aborts attributed to decision-deadline expiry: a canonical record still <c>Undecided</c> past its
    /// deadline that recovery drove to a presumed abort. Distinguishes deadline-expiry aborts from orphan-prepare
    /// aborts (a remote prepare that outlived a failed anchor initialization). A rising rate corroborates
    /// <see cref="LateCommitRejections"/>: the deadline is expiring before healthy coordinators can decide.
    /// </summary>
    internal static readonly Counter<long> DeadlineExpiryAborts =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.deadline_expiry_aborts",
            description: "Recovery presumed-aborts of records left Undecided past their decision deadline.");

    /// <summary>
    /// The decision-deadline margin (ms past the commit timestamp) chosen for each finalize. Recorded at freeze
    /// so dashboards can correlate the derived margin with the observed finalize latency and the late-commit /
    /// deadline-expiry rates when tuning the floor, ceiling, and multiplier.
    /// </summary>
    internal static readonly Histogram<long> DecisionDeadlineMarginMs =
        Meter.CreateHistogram<long>(
            "kahuna.durable_tx.decision_deadline_margin_ms",
            unit: "ms",
            description: "Decision-deadline margin (ms past commit timestamp) frozen for each durable finalize.");

    /// <summary>
    /// Terminal transaction records reclaimed by the retention GC sweep (removed after their retention window
    /// elapsed and their participants' receipts were released). Its rate against admitted durable transactions
    /// shows whether reclamation keeps pace with inflow; a persistently lagging value is the early signal of the
    /// metadata growth this GC exists to bound, visible long before a heap dump.
    /// </summary>
    internal static readonly Counter<long> GcRecordsReclaimed =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.gc_records_reclaimed",
            description: "Terminal transaction records removed by the retention GC sweep.");

    /// <summary>
    /// Participant completion receipts released by the retention GC sweep. Receipts are otherwise never evicted,
    /// so this is the counter that proves the completion-receipt store returns to a steady-state floor rather
    /// than growing for the node's lifetime.
    /// </summary>
    internal static readonly Counter<long> GcReceiptsReleased =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.gc_receipts_released",
            description: "Participant completion receipts released by the retention GC sweep.");

    /// <summary>
    /// Durable transactions refused admission because the node was at <c>DurableDecisionOutstandingMax</c>
    /// outstanding durable finalizes. Each is a retryable <c>MustRetry</c> that prepared nothing. A sustained
    /// non-zero rate means inflow exceeds the admission bound — the backpressure that keeps prepared state, and
    /// the write scheduler's terminal-class reserve, within their budgets.
    /// </summary>
    internal static readonly Counter<long> AdmissionRejections =
        Meter.CreateCounter<long>(
            "kahuna.durable_tx.admission_rejections",
            description: "Durable transactions refused admission at the outstanding-decision cap.");

    internal static void RecordsReclaimed(int count) => GcRecordsReclaimed.Add(count);

    internal static void ReceiptsReleased(int count) => GcReceiptsReleased.Add(count);

    /// <summary>
    /// Registers resident-state observable gauges — canonical record count, completion-receipt count, resident
    /// prepared-intent count and bytes, and outstanding durable transactions — on a fresh, <b>instance-owned</b>
    /// <see cref="Meter"/> returned to the caller. The callbacks capture the stores/coordinator, so the caller
    /// must dispose the returned meter on teardown or a disposed node's state stays reachable (mirrors the write
    /// aggregator's instance-meter ownership). These gauges make the retained metadata this GC bounds visible
    /// continuously, long before a heap dump; the counters above stay on the shared static meter.
    /// </summary>
    internal static Meter RegisterGauges(
        Func<long> recordCount,
        Func<long> receiptCount,
        Func<long> preparedIntentCount,
        Func<long> preparedIntentBytes,
        Func<long> outstandingDurable)
    {
        Meter gaugeMeter = new("Kahuna", "1.0");
        gaugeMeter.CreateObservableGauge("kahuna.durable_tx.resident_records", recordCount,
            description: "Canonical transaction records resident on this node (awaiting retention GC).");
        gaugeMeter.CreateObservableGauge("kahuna.durable_tx.resident_receipts", receiptCount,
            description: "Completion receipts resident on this node (released by GC after retention).");
        gaugeMeter.CreateObservableGauge("kahuna.durable_tx.resident_prepared_intents", preparedIntentCount,
            description: "Prepared intents resident on this node (bounded by durable admission).");
        gaugeMeter.CreateObservableGauge("kahuna.durable_tx.resident_prepared_intent_bytes", preparedIntentBytes,
            unit: "By", description: "Resident prepared-intent value bytes on this node (bounded by durable admission).");
        gaugeMeter.CreateObservableGauge("kahuna.durable_tx.outstanding", outstandingDurable,
            description: "Durable transactions currently being driven through finalize (admission-gated).");
        return gaugeMeter;
    }
}

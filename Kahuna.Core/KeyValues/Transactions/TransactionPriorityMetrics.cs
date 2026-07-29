using System.Diagnostics.Metrics;

using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the transaction admission gates.
///
/// <para>The question these exist to answer is whether the gate is actually doing anything. Below its
/// ceiling the orderer admits everyone immediately and priority is inert; the moment queue depth leaves
/// zero, transactions are being deferred and the configured priorities start deciding who waits. Without
/// that signal an operator cannot tell a well-sized ceiling from one that never engages, or from one so
/// tight that ordinary work is queueing constantly.</para>
///
/// <para>Every instrument is tagged by <c>gate</c> (script vs session) and by <c>priority</c>, because the
/// interesting failure is asymmetric: high-priority work flowing freely while background work queues is
/// the gate working as designed, whereas high-priority work queueing means the ceiling is too low for the
/// load. Instrument naming follows the OpenTelemetry conventions used elsewhere in this project.</para>
/// </summary>
internal static class TransactionPriorityMetrics
{
    private static readonly TransactionPriority[] Priorities = Enum.GetValues<TransactionPriority>();

    /// <summary>
    /// Registers observable gauges for both gates on a caller-owned meter. The meter is returned so a
    /// disposed node's orderers are not kept reachable by gauge callbacks for the lifetime of the process.
    /// </summary>
    internal static Meter RegisterGauges(TransactionPriorityOrderer scriptOrderer, TransactionPriorityOrderer sessionOrderer)
    {
        Meter meter = new("Kahuna", "1.0");

        meter.CreateObservableGauge(
            "kahuna.tx_admission.in_flight",
            () => Observe(scriptOrderer, sessionOrderer, static (orderer, priority) => orderer.InFlightAt(priority)),
            description: "Transactions currently holding an admission slot, by gate and priority.");

        meter.CreateObservableGauge(
            "kahuna.tx_admission.queued",
            () => Observe(scriptOrderer, sessionOrderer, static (orderer, priority) => orderer.QueuedAt(priority)),
            description: "Transactions currently waiting for an admission slot, by gate and priority. Non-zero means the gate is deferring work.");

        meter.CreateObservableGauge(
            "kahuna.tx_admission.max_queue_depth",
            () => Observe(scriptOrderer, sessionOrderer, static (orderer, priority) => orderer.MaxQueueDepthAt(priority)),
            description: "High-water mark of waiters seen at once, by gate and priority.");

        meter.CreateObservableGauge(
            "kahuna.tx_admission.admitted",
            () => Observe(scriptOrderer, sessionOrderer, static (orderer, priority) => orderer.AdmittedAt(priority)),
            description: "Transactions admitted since start, by gate and priority.");

        meter.CreateObservableGauge(
            "kahuna.tx_admission.aged_promotions",
            () => Observe(scriptOrderer, sessionOrderer, static (orderer, priority) => orderer.PromotedAt(priority)),
            description: "Waiters that aging promoted at least one level while queued, by gate and priority.");

        meter.CreateObservableGauge(
            "kahuna.tx_admission.abandoned_while_waiting",
            () => Observe(scriptOrderer, sessionOrderer, static (orderer, priority) => orderer.AbandonedWhileWaitingAt(priority)),
            description: "Waiters whose own timeout fired before they were ever started, by gate and priority.");

        return meter;
    }

    private static IEnumerable<Measurement<long>> Observe(
        TransactionPriorityOrderer scriptOrderer,
        TransactionPriorityOrderer sessionOrderer,
        Func<TransactionPriorityOrderer, TransactionPriority, long> read)
    {
        List<Measurement<long>> measurements = new(Priorities.Length * 2);

        foreach (TransactionPriority priority in Priorities)
        {
            measurements.Add(new(read(scriptOrderer, priority),
                new KeyValuePair<string, object?>("gate", "script"),
                new KeyValuePair<string, object?>("priority", priority.ToString())));

            measurements.Add(new(read(sessionOrderer, priority),
                new KeyValuePair<string, object?>("gate", "session"),
                new KeyValuePair<string, object?>("priority", priority.ToString())));
        }

        return measurements;
    }
}

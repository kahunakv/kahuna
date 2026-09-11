using System.Diagnostics.Metrics;

namespace Kahuna.Server.Persistence;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the background persistence path: the
/// targeted revision prune that shares the writer with the flush, and the unflushed backlog it must
/// never be allowed to starve. Counters live on the shared static meter (they capture no instance
/// state); the per-node backlog gauges are registered by <see cref="PersistenceBacklogMonitor"/>.
/// </summary>
internal static class PersistenceMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>Keys the targeted prune walked (a revision-block scan). Compare with
    /// <see cref="PruneKeysSkipped"/>: on a hot key-set with nothing prunable yet, skips should
    /// dominate; walks that dominate mean the memo is not covering the workload.</summary>
    internal static readonly Counter<long> PruneKeysWalked =
        Meter.CreateCounter<long>("kahuna.persistence.revision_prune.keys_walked_total",
            description: "Keys whose revision block the targeted prune walked.");

    /// <summary>Keys the targeted prune answered from the backend's memo without a walk.</summary>
    internal static readonly Counter<long> PruneKeysSkipped =
        Meter.CreateCounter<long>("kahuna.persistence.revision_prune.keys_skipped_total",
            description: "Keys the targeted prune skipped because the backend memo proved nothing was deletable.");

    /// <summary>Revision rows the targeted prune deleted.</summary>
    internal static readonly Counter<long> PruneRevisionsDeleted =
        Meter.CreateCounter<long>("kahuna.persistence.revision_prune.revisions_deleted_total",
            description: "Historical revision rows deleted by the targeted prune.");

    /// <summary>Flush cycles whose targeted prune stopped on its time budget with keys still queued. A
    /// sustained rate means retention is lagging the write rate; the flush itself is unaffected.</summary>
    internal static readonly Counter<long> PruneBudgetExhausted =
        Meter.CreateCounter<long>("kahuna.persistence.revision_prune.budget_exhausted_total",
            description: "Flush cycles whose targeted prune hit its time budget before visiting every queued key.");

    /// <summary>Wall-clock time the targeted prune took per flush cycle.</summary>
    internal static readonly Histogram<double> PruneCycleMs =
        Meter.CreateHistogram<double>("kahuna.persistence.revision_prune.cycle_duration", unit: "ms",
            description: "Time the targeted revision prune spent in one flush cycle.");
}

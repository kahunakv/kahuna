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

    /// <summary>Keys the prune (targeted and sweep) walked (a revision-block scan). Compare with
    /// <see cref="PruneKeysSkipped"/>: on a hot key-set with nothing prunable yet, skips should
    /// dominate; walks that dominate mean the memo is not covering the workload.</summary>
    internal static readonly Counter<long> PruneKeysWalked =
        Meter.CreateCounter<long>("kahuna.persistence.revision_prune.keys_walked_total",
            description: "Keys whose revision block the revision prune (targeted or sweep) walked.");

    /// <summary>Keys the prune answered from the backend's memo without a walk.</summary>
    internal static readonly Counter<long> PruneKeysSkipped =
        Meter.CreateCounter<long>("kahuna.persistence.revision_prune.keys_skipped_total",
            description: "Keys the revision prune (targeted or sweep) skipped because the backend memo proved nothing was deletable.");

    /// <summary>Of the skipped keys, those whose deletable rows the snapshot floor (a hold registry entry)
    /// protects. Skips with this at zero are retention waiting on the clock or the row count — i.e. the
    /// configured policy, not a hold, is what keeps history on disk.</summary>
    internal static readonly Counter<long> PruneKeysFloorBlocked =
        Meter.CreateCounter<long>("kahuna.persistence.revision_prune.keys_floor_blocked_total",
            description: "Keys the revision prune skipped because the snapshot floor protects every row it could otherwise delete (a subset of keys_skipped_total).");

    /// <summary>Revision rows the prune (targeted and sweep) deleted.</summary>
    internal static readonly Counter<long> PruneRevisionsDeleted =
        Meter.CreateCounter<long>("kahuna.persistence.revision_prune.revisions_deleted_total",
            description: "Historical revision rows deleted by the revision prune (targeted or sweep).");

    /// <summary>Backend-wide sweep passes that paused on their share of the cycle's time budget. The
    /// sweep resumes from its cursor next cycle, so a steady rate only means the store is large; a
    /// pass that keeps pausing without the cursor wrapping means the sweep never completes.</summary>
    internal static readonly Counter<long> SweepBudgetExhausted =
        Meter.CreateCounter<long>("kahuna.persistence.revision_prune.sweep_budget_exhausted_total",
            description: "Backend-wide revision sweep passes that paused on the cycle's time budget.");

    /// <summary>Wall-clock time one backend-wide sweep pass took; bounded by its time budget plus one key.</summary>
    internal static readonly Histogram<double> SweepPassMs =
        Meter.CreateHistogram<double>("kahuna.persistence.revision_prune.sweep_pass_duration", unit: "ms",
            description: "Time one backend-wide revision sweep pass spent on the writer.");

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

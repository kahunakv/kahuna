
using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Instruments of the whole-partition snapshot install (<see cref="PartitionStateTransfer.ImportPartitionState"/>).
/// The install runs on the partition's Raft executor and holds it for its whole duration, so its wall time is
/// what decides whether the follower falls past the leader's log retention while it installs; the per-phase
/// histogram attributes that time to the phase that spent it — the verify pass over the staged stream, the
/// writer round-trip, the backend purge, the row and store applies, the store snapshots, the hooks — so a slow
/// install on a storage engine under load is told apart from a slow decode without a profiler attached.
/// </summary>
internal static class PartitionInstallMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>Wall time of one phase of one install, tagged by <c>phase</c>; the <c>total</c> phase is the whole install.</summary>
    internal static readonly Histogram<double> PhaseMs =
        Meter.CreateHistogram<double>("kahuna.partition_install.phase_duration", unit: "ms",
            description: "Wall time one phase of a whole-partition snapshot install spent, tagged by phase (verify, gate, drain, purge_rows, purge_stores, rows, stores, snapshots, floor, hooks, total).");

    /// <summary>Installs that took longer than the slow-install threshold, the ones logged at Warning with their phase breakdown.</summary>
    internal static readonly Counter<long> SlowInstalls =
        Meter.CreateCounter<long>("kahuna.partition_install.slow_total",
            description: "Whole-partition snapshot installs that exceeded the slow-install threshold and were logged with their phase breakdown.");

    internal static readonly KeyValuePair<string, object?> PhaseVerify = new("phase", "verify");
    internal static readonly KeyValuePair<string, object?> PhaseGate = new("phase", "gate");
    internal static readonly KeyValuePair<string, object?> PhaseDrain = new("phase", "drain");
    internal static readonly KeyValuePair<string, object?> PhasePurgeRows = new("phase", "purge_rows");
    internal static readonly KeyValuePair<string, object?> PhasePurgeStores = new("phase", "purge_stores");
    internal static readonly KeyValuePair<string, object?> PhaseRows = new("phase", "rows");
    internal static readonly KeyValuePair<string, object?> PhaseStores = new("phase", "stores");
    internal static readonly KeyValuePair<string, object?> PhaseSnapshots = new("phase", "snapshots");
    internal static readonly KeyValuePair<string, object?> PhaseFloor = new("phase", "floor");
    internal static readonly KeyValuePair<string, object?> PhaseHooks = new("phase", "hooks");
    internal static readonly KeyValuePair<string, object?> PhaseTotal = new("phase", "total");
}

using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the MVCC snapshot-floor subsystem.
///
/// <para>
/// <c>kahuna.snapshot_floor.missing_protected_version_total</c> is the fault-signal counter:
/// it increments at reclamation sites (in-memory trim and persistent prune) when a
/// floor-protected boundary revision is scheduled for trimming — a signal that boundary
/// computation regressed and floor protection has a gap. This counter must remain 0 under
/// normal operation; a non-zero value indicates a bug at one of the reclamation sites.
/// </para>
///
/// <para>
/// Observable gauges for live hold count and effective floor physical timestamp are registered
/// on a per-instance <see cref="Meter"/> owned by <see cref="SnapshotFloorStore"/> (named with the
/// same <c>"Kahuna"</c> scope as this static meter, since multiple meters may share a name).
/// Disposing a store disposes its meter, which removes the gauge callbacks and prevents GC
/// retention of the dead store — which is why the gauges are not on this static, process-lived
/// <see cref="Meter"/>. The scope name is identical, so a consumer subscribing to the <c>"Kahuna"</c>
/// scope collects both the gauges and this counter; only lifetime/disposal differs.
/// </para>
/// </summary>
internal static class SnapshotFloorMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Increments at reclamation sites when a floor-protected boundary revision is scheduled
    /// for trimming — a boundary-computation regression. Fires at two sites:
    /// <list type="bullet">
    ///   <item><description>
    ///     In-memory trim (<c>RemoveExpiredRevisions</c> in <c>BaseHandler</c>): fires when
    ///     the floor-boundary revision appears in the computed removal set, which correct trim
    ///     logic never produces.
    ///   </description></item>
    ///   <item><description>
    ///     Persistent prune (<c>BackgroundWriterActor</c>): fires when
    ///     <c>PruneKeyValueRevisions</c> reports a non-zero <c>FloorViolations</c> count,
    ///     indicating the backend deleted a floor-needed revision despite a non-zero floor being
    ///     passed.
    ///   </description></item>
    /// </list>
    /// Should always be 0 in production and in the test suite.
    /// </summary>
    internal static readonly Counter<long> MissingProtectedVersion =
        Meter.CreateCounter<long>(
            "kahuna.snapshot_floor.missing_protected_version_total",
            description: "Floor-protected boundary revisions scheduled for trimming at a reclamation site (must stay 0).");
}

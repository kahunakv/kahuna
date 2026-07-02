using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the MVCC snapshot-floor subsystem.
///
/// <para>
/// <c>kahuna.snapshot_floor.missing_protected_version_total</c> is the fault-signal counter:
/// it increments when a snapshot read returns missing for a key at a timestamp that an active
/// hold should have protected from reclamation. This counter must remain 0 under normal
/// operation; a non-zero value means F3/F4 floor enforcement has a gap and protection was
/// silently violated.
/// </para>
///
/// <para>
/// Observable gauges for live hold count and effective floor physical timestamp are registered
/// per-instance from <see cref="SnapshotFloorStore"/> so they capture live state without a
/// static singleton. See its constructor.
/// </para>
/// </summary>
internal static class SnapshotFloorMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Increments when a snapshot read at a timestamp protected by an active hold returns
    /// missing from both the in-memory archive and persisted revision history.
    ///
    /// <para>A non-zero value indicates that the floor enforcement in
    /// <c>RemoveExpiredRevisions</c> or <c>PruneKeyValueRevisions</c> failed to preserve a
    /// revision it was required to keep. Should always be 0 in production and in the test suite.</para>
    /// </summary>
    internal static readonly Counter<long> MissingProtectedVersion =
        Meter.CreateCounter<long>(
            "kahuna.snapshot_floor.missing_protected_version_total",
            description: "Snapshot reads that returned missing while an active hold should have protected the version (must stay 0).");
}

using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for the fences that keep snapshot reads repeatable: the
/// in-memory revision archive held back by the background writer's flush lag, the persisted-history reads refused
/// while that lag could hide a revision, and the clock fence between a snapshot read and later commits.
/// </summary>
internal static class KeyValueSnapshotReadMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Archive trims that kept revisions beyond the configured retention count because the background
    /// writer had not yet confirmed them. A steady rate is normal under write load; a rate that tracks
    /// the write rate means the writer is falling behind and the archive is growing with its lag.
    /// </summary>
    internal static readonly Counter<long> RetainedUnflushed =
        Meter.CreateCounter<long>(
            "kahuna.kv.revisions.retained_unflushed_total",
            description: "Revision-archive trims held back because the revisions were not yet flushed.");

    /// <summary>
    /// Snapshot reads that found the persisted revision history behind a revision they could need — still queued
    /// for the background writer and absent from the archive. Such a read is answered from the head when the head
    /// is visible to it, and retried otherwise; a hydrated history in that state is not installed as the archive.
    /// A sustained rate means flushes lag behind snapshot reads of hot keys.
    /// </summary>
    internal static readonly Counter<long> HistoryReadsFenced =
        Meter.CreateCounter<long>(
            "kahuna.kv.revisions.history_reads_fenced_total",
            description: "Snapshot reads that met a persisted revision history lagging a queued flush.");

    /// <summary>
    /// Snapshot reads whose timestamp was too far ahead of the serving node's clock to be folded into it. Such a
    /// read is served without the clock fence, so a commit that starts after it can still land inside its
    /// snapshot. Any non-zero rate means a caller mints snapshots from a clock far ahead of the cluster.
    /// </summary>
    internal static readonly Counter<long> SnapshotClockFenceSkipped =
        Meter.CreateCounter<long>(
            "kahuna.kv.snapshot_clock_fence_skipped_total",
            description: "Snapshot reads served without the clock fence because their timestamp was too far ahead.");
}

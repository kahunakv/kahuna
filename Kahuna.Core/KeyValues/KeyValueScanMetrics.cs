using System.Diagnostics.Metrics;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for disk-backed key-value scans.
///
/// <para>
/// These counters make scan read amplification observable from the outside. Without them the
/// only symptom of a pathological scan is unexplained disk throughput: a snapshot prefix scan
/// over deep revision chains can examine orders of magnitude more physical rows than it
/// returns, and an abandoned scan that still runs to completion shows up as disk reads with no
/// matching client traffic.
/// </para>
/// </summary>
internal static class KeyValueScanMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>
    /// Physical rows examined by snapshot (as-of) prefix scans, across current-head rows and
    /// retained revision-history rows. Compare with
    /// <c>kahuna.scan.snapshot_prefix_entries_returned_total</c>: a large and growing ratio
    /// means revision chains have grown deep enough that each scan reads far more history than
    /// data, which is the read-amplification signature.
    /// </summary>
    internal static readonly Counter<long> SnapshotPrefixRowsExamined =
        Meter.CreateCounter<long>(
            "kahuna.scan.snapshot_prefix_rows_examined_total",
            description: "Physical rows examined by snapshot prefix scans (head plus revision-history rows).");

    /// <summary>
    /// Entries returned by snapshot (as-of) prefix scans. Denominator for the amplification
    /// ratio described on <see cref="SnapshotPrefixRowsExamined"/>.
    /// </summary>
    internal static readonly Counter<long> SnapshotPrefixEntriesReturned =
        Meter.CreateCounter<long>(
            "kahuna.scan.snapshot_prefix_entries_returned_total",
            description: "Entries returned by snapshot prefix scans.");

    /// <summary>
    /// Scheduled backend scans skipped or stopped early because their read continuation was
    /// already expired when the scheduler ran them. Each increment is disk work that would have
    /// been wasted on a result nobody was waiting for; a sustained rate means callers time out
    /// and retry faster than the read scheduler drains, and the retries are re-enqueuing work
    /// whose original waiters are gone.
    /// </summary>
    internal static readonly Counter<long> ScansAbandonedCancelled =
        Meter.CreateCounter<long>(
            "kahuna.scan.abandoned_cancelled_total",
            description: "Backend scans skipped or stopped early because their continuation had expired.");
}

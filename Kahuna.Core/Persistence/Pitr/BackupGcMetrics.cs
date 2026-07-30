using System.Diagnostics.Metrics;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for backup garbage collection (orphan/leftover
/// sweep and retention). Counters are cumulative and safe to observe from any thread. Instrument names
/// follow OpenTelemetry semantic conventions (dot-separated lowercase); Prometheus exporters typically
/// translate dots to underscores.
/// </summary>
internal static class BackupGcMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>GC passes that ran to completion (inline after a backup, or on the periodic tick).</summary>
    internal static readonly Counter<long> Runs =
        Meter.CreateCounter<long>(
            "kahuna.backup.gc.runs",
            description: "Backup garbage-collection passes completed.");

    /// <summary>Orphaned/leftover artifact directories and files reclaimed by the sweep.</summary>
    internal static readonly Counter<long> OrphansReclaimed =
        Meter.CreateCounter<long>(
            "kahuna.backup.gc.orphans_reclaimed",
            description: "Orphaned/leftover backup artifacts reclaimed (dirs with no manifest + staging/temp remnants).");

    /// <summary>Backups deleted by the retention policy (whole chains beyond the configured bounds).</summary>
    internal static readonly Counter<long> RetentionDeletions =
        Meter.CreateCounter<long>(
            "kahuna.backup.gc.retention_deletions",
            description: "Backups deleted by the retention policy.");

    /// <summary>Artifact bytes freed by retention deletions (from manifest-recorded sizes).</summary>
    internal static readonly Counter<long> BytesReclaimed =
        Meter.CreateCounter<long>(
            "kahuna.backup.gc.bytes_reclaimed",
            description: "Artifact bytes freed by retention deletions.");
}

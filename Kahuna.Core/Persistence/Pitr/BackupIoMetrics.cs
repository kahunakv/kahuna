using System.Diagnostics.Metrics;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// <see cref="System.Diagnostics.Metrics"/> instruments for backup and restore I/O. Counters are
/// cumulative and safe to observe from any thread; durations are histograms in milliseconds.
/// Instrument names follow OpenTelemetry conventions (dot-separated lowercase); Prometheus exporters
/// translate dots to underscores.
/// </summary>
internal static class BackupIoMetrics
{
    internal static readonly Meter Meter = new("Kahuna", "1.0");

    /// <summary>Backups that completed and were published (full, incremental, or coordinated).</summary>
    internal static readonly Counter<long> BackupOperations =
        Meter.CreateCounter<long>("kahuna.backup.operations", description: "Backups completed and published.");

    /// <summary>Backup attempts that failed (nothing published).</summary>
    internal static readonly Counter<long> BackupFailures =
        Meter.CreateCounter<long>("kahuna.backup.failures", description: "Backup attempts that failed.");

    /// <summary>Artifact bytes a backup produced (checkpoint files + WAL segments), from the manifest.</summary>
    internal static readonly Counter<long> BackupBytes =
        Meter.CreateCounter<long>("kahuna.backup.bytes", description: "Artifact bytes written by backups.");

    /// <summary>Wall-clock duration of a backup operation, milliseconds.</summary>
    internal static readonly Histogram<double> BackupDurationMs =
        Meter.CreateHistogram<double>("kahuna.backup.duration_ms", unit: "ms", description: "Backup operation duration.");

    /// <summary>Restore operations that completed.</summary>
    internal static readonly Counter<long> RestoreOperations =
        Meter.CreateCounter<long>("kahuna.restore.operations", description: "Restore operations completed.");

    /// <summary>Restore attempts that failed (no usable target produced).</summary>
    internal static readonly Counter<long> RestoreFailures =
        Meter.CreateCounter<long>("kahuna.restore.failures", description: "Restore attempts that failed.");

    /// <summary>Artifact bytes a restore read (checkpoint + replayed segments), from the chain manifests.</summary>
    internal static readonly Counter<long> RestoreBytes =
        Meter.CreateCounter<long>("kahuna.restore.bytes", description: "Artifact bytes read by restores.");

    /// <summary>WAL entries a restore applied during replay.</summary>
    internal static readonly Counter<long> RestoreEntriesApplied =
        Meter.CreateCounter<long>("kahuna.restore.entries_applied", description: "WAL entries applied during restore replay.");

    /// <summary>Wall-clock duration of a restore operation, milliseconds.</summary>
    internal static readonly Histogram<double> RestoreDurationMs =
        Meter.CreateHistogram<double>("kahuna.restore.duration_ms", unit: "ms", description: "Restore operation duration.");
}

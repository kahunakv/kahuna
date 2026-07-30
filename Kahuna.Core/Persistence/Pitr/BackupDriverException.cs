
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Thrown by <see cref="BackupDriver"/> when a backup operation cannot proceed —
/// for example, when the requested incremental range starts below the WAL compaction floor.
/// </summary>
internal sealed class BackupDriverException : Exception
{
    /// <summary>
    /// True when the incremental range starts below the WAL compaction floor and the caller
    /// should retry by taking a new full backup instead.
    /// </summary>
    public bool NeedsFullBackup { get; init; }

    /// <summary>
    /// True when the requested parent backup was not found in the catalog.
    /// </summary>
    public bool ParentMissing { get; init; }

    /// <summary>
    /// True when the restore destination is unsafe or already exists (never-overwrite / overlap).
    /// </summary>
    public bool TargetConflict { get; init; }

    /// <summary>
    /// True when the requested restore timestamp is outside the chain's recoverable coverage
    /// (before its base cut or after its last incremental).
    /// </summary>
    public bool TargetOutsideCoverage { get; init; }

    /// <summary>
    /// True when the persistence backend cannot produce an exact as-of checkpoint, so a full backup
    /// with a proven base cut cannot be taken (a physical copy would over-include post-cut state).
    /// </summary>
    public bool ExactCheckpointUnavailable { get; init; }

    public BackupDriverException(string message) : base(message) { }
}

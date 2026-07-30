namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Stable, public classification of a backup or restore result so consumers can branch on the
/// outcome without matching internal exception type names or parsing messages.
/// </summary>
public enum KahunaBackupOutcome
{
    /// <summary>The operation completed successfully.</summary>
    Ok = 0,

    /// <summary>Backup is not configured on this node (no backup directory set).</summary>
    NotConfigured = 1,

    /// <summary>The requested parent backup does not exist in the catalog.</summary>
    ParentMissing = 2,

    /// <summary>
    /// An incremental was requested but cannot be produced (its start index is below the WAL
    /// compaction floor); a full backup is required instead.
    /// </summary>
    NeedsFull = 3,

    /// <summary>The backup chain is structurally invalid (broken parent link, index gap, cycle).</summary>
    CorruptChain = 4,

    /// <summary>An artifact is missing, truncated, extra, duplicated, or fails its recorded digest.</summary>
    CorruptArtifact = 5,

    /// <summary>The restore destination already exists or overlaps a protected path.</summary>
    TargetConflict = 6,

    /// <summary>The requested restore timestamp lies outside the chain's recoverable coverage.</summary>
    TargetOutsideCoverage = 7,

    /// <summary>The operation was cancelled by the caller.</summary>
    Cancelled = 8,

    /// <summary>An I/O error occurred reading or writing artifacts.</summary>
    IoError = 9,

    /// <summary>Leadership was lost mid-operation; the caller may retry.</summary>
    RetryableLeadershipLoss = 10,

    /// <summary>
    /// The backend cannot produce an exact as-of checkpoint, so a full backup with a proven base cut
    /// cannot be taken. Fail closed rather than publish an image that over-includes post-cut state.
    /// </summary>
    ExactCheckpointUnavailable = 11,

    /// <summary>The backup artifact/manifest is in a legacy or unsupported format and was not upgraded.</summary>
    UnsupportedFormat = 12,
}

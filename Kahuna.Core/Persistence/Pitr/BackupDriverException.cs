
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

    /// <summary>
    /// True when the cluster topology (partition range map or membership) changed between the start of
    /// the backup and publish, so the captured partition set is not a single consistent snapshot. The
    /// backup is aborted with nothing published; the caller may retry once the topology is stable.
    /// </summary>
    public bool TopologyChanged { get; init; }

    /// <summary>
    /// True when meta-partition leadership was lost (or its term advanced) between the start of a
    /// coordinated backup and publish, so this node can no longer stand as the backup coordinator. The
    /// backup is aborted with nothing published; the caller may retry against the current leader.
    /// </summary>
    public bool RetryableLeadershipLoss { get; init; }

    /// <summary>
    /// True when a restore or bootstrap refused a backup chain whose covered partition set does not
    /// reach every partition of the cluster it was captured from. Under per-partition replica
    /// placement a node captures only the partitions it hosts (the manifest records both the covered
    /// and the full cluster set), so restoring such a chain alone would silently reconstruct a
    /// cluster missing the partitions hosted elsewhere. The message names the missing partitions.
    /// </summary>
    public bool RestrictedCoverage { get; init; }

    public BackupDriverException(string message) : base(message) { }
}

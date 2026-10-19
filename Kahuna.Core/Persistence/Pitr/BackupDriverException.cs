
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Thrown by <see cref="BackupDriver"/> when a backup operation cannot proceed —
/// for example, when the requested incremental range starts below the WAL compaction floor.
/// </summary>
internal sealed class BackupDriverException : Exception
{
    /// <summary>
    /// True when an incremental cannot extend its parent chain and the caller should take a new full
    /// backup instead: the range starts below the WAL compaction floor, or the cluster topology
    /// moved since the chain was taken (see <see cref="TopologyDivergedFromParent"/>).
    /// </summary>
    public bool NeedsFullBackup { get; init; }

    /// <summary>
    /// True when the cluster topology generation captured for an incremental differs from the one
    /// recorded by an ancestor in its chain (a membership change or a partition split/merge since the
    /// parent was taken). The chain validator refuses a chain that spans two generations, so such an
    /// incremental must not be published; <see cref="NeedsFullBackup"/> is set alongside this flag.
    /// </summary>
    public bool TopologyDivergedFromParent { get; init; }

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

    /// <summary>
    /// True when a coordinated backup could not prove, after its capture, that its image holds every transaction
    /// with a commit timestamp at or below the cut as a whole: a durable transaction at or below the cut was in
    /// flight during the capture, or a partition could not confirm that this node applied everything the cluster
    /// had committed. Also true when a full backup could not capture the prepared intents its chain's restore needs:
    /// an intent live at a range end settled before the node's intents were walked, and its prepare is outside the
    /// WAL the backup reads. Nothing is published; a retry chooses a new cut and new range ends.
    /// </summary>
    public bool CutUnverified { get; init; }

    /// <summary>
    /// True when a bootstrap refused a chain whose target time is older than the leader's worst-case WAL
    /// compaction floor (now − PITR window − base snapshot interval). The leader sends a full snapshot to such
    /// a node anyway, so the seed would cost a restore and save nothing.
    /// </summary>
    public bool OutsidePitrWindow { get; init; }

    /// <summary>
    /// True when a cluster-wide backup could not get one node's share: the node refused it, failed it, or could
    /// not be reached. Nothing is published. The message names the partitions of that share and the node, and
    /// nothing else (no path or backend text), so it is safe to return to the caller.
    /// </summary>
    public bool PartitionCaptureFailed { get; init; }

    /// <summary>
    /// True when this node had not applied every committed write of a captured partition up to the cut within the
    /// applied barrier's wait, so an image taken now would miss committed data. The condition is transient: the
    /// replica is catching up (it was added or restarted recently) or its writer is stalled. Nothing is published;
    /// the caller retries, and a cluster-wide backup captures the partition from another replica that has the
    /// writes applied. <see cref="LaggingPartitions"/> names the partitions.
    /// </summary>
    public bool NotCaughtUp { get; init; }

    /// <summary>With <see cref="NotCaughtUp"/>: the partitions whose committed writes this node had not applied.</summary>
    public IReadOnlyList<int> LaggingPartitions { get; init; } = [];

    /// <summary>
    /// From a cluster-wide backup, with <see cref="PartitionCaptureFailed"/> or <see cref="NotCaughtUp"/>: the
    /// replicas (node, partition) a retry should capture from only as a last resort, because the node could not be
    /// reached or had not applied the partition's committed writes to the cut. A retry assigns each such partition
    /// to another replica when one is known. Empty when every failed share was refused or failed on a reachable
    /// node that had applied its partitions.
    /// </summary>
    public IReadOnlyList<(string Node, int PartitionId)> ReplicasToAvoid { get; init; } = [];

    public BackupDriverException(string message) : base(message) { }
}

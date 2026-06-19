
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Describes a single backup artifact: its identity, type, WAL coverage per partition,
/// parent link (Incremental only), per-file checksums, and an optional cluster-wide
/// snapshot timestamp for coordinated restores.
/// </summary>
internal sealed class BackupManifest
{
    public Guid BackupId { get; set; } = Guid.NewGuid();

    public BackupType Type { get; set; }

    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Null for Full backups; the <see cref="BackupId"/> of the immediately preceding
    /// artifact for Incremental backups.
    /// </summary>
    public Guid? ParentBackupId { get; set; }

    /// <summary>
    /// WAL coverage for each partition included in this artifact.
    /// For a Full backup FromIndex is 1 (or 0 if the partition is empty); for Incremental
    /// it is <c>parent.ToIndex + 1</c> for each partition.
    /// </summary>
    public List<PartitionBackupRange> PartitionRanges { get; set; } = [];

    /// <summary>
    /// For cluster-wide coordinated backups: the single HLC chosen as the snapshot point.
    /// Stored as raw fields so the manifest remains a plain-JSON document.
    /// </summary>
    public int? ClusterSnapshotNode { get; set; }
    public long? ClusterSnapshotPhysical { get; set; }
    public uint? ClusterSnapshotCounter { get; set; }

    public HLCTimestamp? ClusterSnapshotTime =>
        ClusterSnapshotNode.HasValue
            ? new HLCTimestamp(ClusterSnapshotNode.Value, ClusterSnapshotPhysical!.Value, ClusterSnapshotCounter!.Value)
            : null;

    internal void SetClusterSnapshotTime(HLCTimestamp t)
    {
        ClusterSnapshotNode = t.N;
        ClusterSnapshotPhysical = t.L;
        ClusterSnapshotCounter = t.C;
    }

    /// <summary>SHA-256 hex digests keyed by artifact-relative file path.</summary>
    public Dictionary<string, string> Checksums { get; set; } = [];

    public static BackupManifest CreateFull(List<PartitionBackupRange> partitionRanges) => new()
    {
        Type = BackupType.Full,
        PartitionRanges = partitionRanges
    };

    public static BackupManifest CreateIncremental(Guid parentId, List<PartitionBackupRange> partitionRanges) => new()
    {
        Type = BackupType.Incremental,
        ParentBackupId = parentId,
        PartitionRanges = partitionRanges
    };
}

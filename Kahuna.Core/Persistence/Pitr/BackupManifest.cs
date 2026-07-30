
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Describes a single backup artifact: its identity, type, WAL coverage per partition,
/// parent link (Incremental only), per-file checksums, and an optional cluster-wide
/// snapshot timestamp for coordinated restores.
/// </summary>
internal sealed class BackupManifest
{
    /// <summary>
    /// The format version produced by the current code. Version 1 is the first version that hashes
    /// every checkpoint data file (not just the sidecar), records per-file <see cref="Sizes"/>, and
    /// stamps an explicit <see cref="BaseCut"/>. A manifest deserialized with
    /// <see cref="FormatVersion"/> below this (e.g. 0 — a pre-hardening backup that hashed only the
    /// checkpoint sidecar) cannot be verified against the exact-file-set/coverage rules and is treated
    /// as an unsupported legacy artifact, never as "corrupt".
    /// </summary>
    public const int CurrentFormatVersion = 1;

    /// <summary>Format version this manifest was written with. Absent (0) on pre-hardening manifests.</summary>
    public int FormatVersion { get; set; }

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

    /// <summary>
    /// The HLC the Full backup's checkpoint image was cut at: no committed state with
    /// <c>LastModified &gt; BaseCut</c> should be present in the image. This is the earliest
    /// recoverable point of any chain rooted at this Full (its minimum coverage bound). Only Full
    /// backups set it; incrementals leave it null.
    /// </summary>
    public int? BaseCutNode { get; set; }
    public long? BaseCutPhysical { get; set; }
    public uint? BaseCutCounter { get; set; }

    public HLCTimestamp? BaseCut =>
        BaseCutNode.HasValue
            ? new HLCTimestamp(BaseCutNode.Value, BaseCutPhysical!.Value, BaseCutCounter!.Value)
            : null;

    internal void SetBaseCut(HLCTimestamp t)
    {
        BaseCutNode = t.N;
        BaseCutPhysical = t.L;
        BaseCutCounter = t.C;
    }

    /// <summary>SHA-256 hex digests keyed by artifact-relative file path.</summary>
    public Dictionary<string, string> Checksums { get; set; } = [];

    /// <summary>
    /// Byte length of each artifact file, keyed by the same artifact-relative path as
    /// <see cref="Checksums"/>. Verified alongside the digest so a truncated or padded file is
    /// rejected even before the (more expensive) hash is recomputed. Every key in
    /// <see cref="Checksums"/> has a corresponding entry here for manifests written by the current
    /// driver; older manifests without sizes fall back to digest-only verification.
    /// </summary>
    public Dictionary<string, long> Sizes { get; set; } = [];

    public static BackupManifest CreateFull(List<PartitionBackupRange> partitionRanges) => new()
    {
        FormatVersion = CurrentFormatVersion,
        Type = BackupType.Full,
        PartitionRanges = partitionRanges
    };

    public static BackupManifest CreateIncremental(Guid parentId, List<PartitionBackupRange> partitionRanges) => new()
    {
        FormatVersion = CurrentFormatVersion,
        Type = BackupType.Incremental,
        ParentBackupId = parentId,
        PartitionRanges = partitionRanges
    };
}

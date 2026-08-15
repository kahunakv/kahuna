
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Describes a single backup artifact: its identity, type, WAL coverage per partition,
/// parent link (Incremental only), per-file checksums, and an optional cluster-wide
/// snapshot timestamp for coordinated restores.
/// <para>
/// Public because it is the document a storage target persists and returns, and targets live in
/// separate assemblies (object-storage implementations). The mutators that stamp derived identity
/// (<see cref="ApplyOwnerIdentity"/>, <see cref="SetBaseCut"/>, <see cref="SetClusterSnapshotTime"/>)
/// stay internal: a target stores and retrieves a manifest, it never composes one.
/// </para>
/// </summary>
public sealed class BackupManifest
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

    /// <summary>
    /// Operator-assigned identity of the cluster this backup belongs to (the configured cluster id/name).
    /// Null on legacy manifests and when no cluster id is configured. Chain resolution rejects linking
    /// manifests that carry different (non-null) cluster ids, so a foreign cluster's artifact can never be
    /// chained onto this one. A null value is "unknown", not a wildcard match — it is skipped, never forced equal.
    /// </summary>
    public string? ClusterId { get; set; }

    /// <summary>
    /// The node (endpoint or name) that produced this backup. Informational — recorded for operator
    /// traceability; it is deliberately NOT a chain-gating field, since successive backups in one chain
    /// may legitimately be produced by different coordinators after a leadership change.
    /// </summary>
    public string? CoordinatorNode { get; set; }

    /// <summary>
    /// The meta-partition Raft term observed when this backup was produced. Informational, like
    /// <see cref="CoordinatorNode"/>; not chain-gating.
    /// </summary>
    public long? CoordinatorTerm { get; set; }

    /// <summary>
    /// The persistence-backend storage-engine type this backup was produced from (e.g. "sqlite",
    /// "rocksdb", "memory"). Informational; the storage *revision* below is the chain-gating field.
    /// </summary>
    public string? StorageType { get; set; }

    /// <summary>
    /// The storage format revision the artifacts were written with. Chain resolution rejects linking
    /// manifests with different (non-null) revisions: a revision change can alter the on-disk checkpoint
    /// format, so an incremental written under one revision must not be replayed onto a base written
    /// under another. Null is "unknown" and skipped.
    /// </summary>
    public string? StorageRevision { get; set; }

    /// <summary>
    /// The cluster topology generation captured at backup time (composed from per-partition range
    /// generations and the membership version). Null until the topology-capture path populates it.
    /// Chain resolution rejects linking manifests with different (non-null) generations: a range
    /// split/merge or membership change alters the partition set, so a new full is required rather than
    /// chaining across the boundary. Null is "unknown" and skipped.
    /// </summary>
    public long? TopologyGeneration { get; set; }

    /// <summary>
    /// Every data partition id the cluster's committed map listed at capture time (excluding ranges
    /// mid-move or already merged away). This is what a restore must reconstruct to be whole. Null on
    /// manifests written before coverage was recorded — those could only be produced under full
    /// replication, where one node's capture covers everything by construction.
    /// </summary>
    public List<int>? ClusterPartitions { get; set; }

    /// <summary>
    /// The subset of <see cref="ClusterPartitions"/> this node hosted — and therefore actually
    /// captured — at backup time. A partition listed here but absent from
    /// <see cref="PartitionRanges"/> was covered but empty (no committed WAL entries), which is not
    /// a coverage gap. Under full replication this equals <see cref="ClusterPartitions"/>; under
    /// per-partition replica placement it can be a strict subset, and restore refuses a chain whose
    /// union of covered sets does not reach every cluster partition. Null on pre-coverage manifests.
    /// </summary>
    public List<int>? CoveredPartitions { get; set; }

    /// <summary>
    /// Stamps the owner/topology identity onto this manifest at write time. Only non-null fields on
    /// <paramref name="identity"/> are applied, so a partially-known identity (e.g. before the cluster
    /// coordinator assigns a cluster id) leaves the unknown fields null rather than overwriting them
    /// with a placeholder.
    /// </summary>
    internal void ApplyOwnerIdentity(BackupOwnerIdentity identity)
    {
        if (identity.ClusterId is not null) ClusterId = identity.ClusterId;
        if (identity.CoordinatorNode is not null) CoordinatorNode = identity.CoordinatorNode;
        if (identity.CoordinatorTerm.HasValue) CoordinatorTerm = identity.CoordinatorTerm;
        if (identity.StorageType is not null) StorageType = identity.StorageType;
        if (identity.StorageRevision is not null) StorageRevision = identity.StorageRevision;
        if (identity.TopologyGeneration.HasValue) TopologyGeneration = identity.TopologyGeneration;
    }

    /// <summary>
    /// Optional keyed authentication tag (HMAC-SHA-256, hex) over the manifest's identity, coverage, and
    /// the per-file digest map, computed with a server-held key that is never stored beside the artifacts.
    /// Null when no backup MAC key is configured. When a key IS configured, verification requires this to
    /// be present and to match — so an attacker who can write the backup directory cannot rewrite a file
    /// and its recorded digest, nor strip the tag, without detection. Excluded from its own computation.
    /// </summary>
    public string? Mac { get; set; }

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

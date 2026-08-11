namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Describes a single backup artifact returned by the backup API.
/// </summary>
public sealed class KahunaBackupInfo
{
    public Guid BackupId { get; set; }

    /// <summary>
    /// Manifest format version. 0 indicates a legacy (pre-hardening) backup that cannot be verified
    /// or restored by the current code without an explicit upgrade; ≥1 is a current, fully-verified
    /// format. Exposed so listings are honest about old artifacts rather than labeling them corrupt.
    /// </summary>
    public int FormatVersion { get; set; }

    public string Type { get; set; } = "";
    public DateTime CreatedAtUtc { get; set; }
    public Guid? ParentBackupId { get; set; }
    public int PartitionCount { get; set; }

    /// <summary>
    /// The operator-assigned cluster identity stamped into this backup's manifest, or null when no
    /// cluster id was configured (or on a legacy manifest). Surfaced so an operator listing backups
    /// from any node can see which cluster produced each entry — a catalog mixing cluster ids, or one
    /// showing only some of the cluster's backups, signals a non-shared (per-node, partial) catalog.
    /// </summary>
    public string? ClusterId { get; set; }

    /// <summary>
    /// The node that produced this backup (the coordinator for a coordinated backup), or null on a
    /// legacy manifest. With a shared backup directory every node's listing shows the same entries and
    /// their producing nodes; if a listing shows only backups produced by the local node, the catalog
    /// is node-local and therefore only a partial view of the cluster's backups.
    /// </summary>
    public string? CoordinatorNode { get; set; }
    public int? ClusterSnapshotNode { get; set; }
    public long? ClusterSnapshotPhysical { get; set; }
    public uint? ClusterSnapshotCounter { get; set; }

    /// <summary>
    /// The backup kind the caller asked for ("Full" or "Incremental"). Equal to <see cref="ActualKind"/>
    /// unless the request was substituted (see <see cref="SubstitutionReason"/>).
    /// </summary>
    public string? RequestedKind { get; set; }

    /// <summary>The backup kind actually produced. Equals <see cref="Type"/>.</summary>
    public string? ActualKind { get; set; }

    /// <summary>
    /// When non-null, the requested kind could not be honored and was substituted (e.g. an
    /// incremental fell back to a full because the WAL compaction floor advanced past the parent).
    /// The value explains why so the substitution is observable rather than silent.
    /// </summary>
    public string? SubstitutionReason { get; set; }

    /// <summary>
    /// Set when this entry must not be used: a manifest that could not be read/parsed, or a backup whose
    /// artifacts exist without a manifest (see <see cref="IsIncomplete"/>). When true the entry is a
    /// placeholder — only <see cref="BackupId"/> is meaningful — and <see cref="InvalidReason"/> explains
    /// the problem.
    /// </summary>
    public bool IsInvalid { get; set; }

    /// <summary>
    /// Set when artifacts exist for this backup id but no manifest does, meaning the backup never
    /// completed: the artifacts are published before the manifest, so a missing manifest is the signature
    /// of an upload or write that was interrupted — or of one still in flight.
    /// <para>
    /// Entries flagged this way also carry <see cref="IsInvalid"/>, so a caller that only checks the
    /// older flag still correctly refuses to use them; this one exists to distinguish "started and never
    /// finished" from "finished but the manifest is corrupt", which need different operator responses.
    /// </para>
    /// </summary>
    public bool IsIncomplete { get; set; }

    /// <summary>Human-readable reason this entry is invalid or incomplete; null when the entry is valid.</summary>
    public string? InvalidReason { get; set; }

    /// <summary>
    /// For the Full backup at the head of a resolved chain: the physical-ms component of the
    /// earliest recoverable HLC (the base cut). Null on entries returned by a plain listing or on
    /// non-head chain entries. A restore target is recoverable when it falls within
    /// <see cref="MinRecoverablePhysicalMs"/>..<see cref="MaxRecoverablePhysicalMs"/>, independent of
    /// how much wall-clock time has passed.
    /// </summary>
    public long? MinRecoverablePhysicalMs { get; set; }

    /// <summary>
    /// For the Full backup at the head of a resolved chain: the physical-ms component of the latest
    /// recoverable HLC (the newest captured entry across the whole chain). Null otherwise.
    /// </summary>
    public long? MaxRecoverablePhysicalMs { get; set; }
}

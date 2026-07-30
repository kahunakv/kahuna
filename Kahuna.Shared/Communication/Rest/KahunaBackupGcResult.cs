namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Result of a backup garbage-collection request: what was (or, for a dry run, would be) reclaimed —
/// backups deleted by the retention policy and orphaned/leftover artifacts swept.
/// </summary>
public sealed class KahunaBackupGcResult
{
    /// <summary>False for a dry-run inventory (nothing deleted); true when the pass was applied.</summary>
    public bool Applied { get; set; }

    /// <summary>Artifact bytes freed by retention deletions (from manifest-recorded sizes).</summary>
    public long BytesReclaimed { get; set; }

    /// <summary>Backups the retention policy deleted (or would delete), descendants-first.</summary>
    public List<KahunaBackupGcDeletion> RetentionDeletions { get; set; } = [];

    /// <summary>Orphaned/leftover artifacts the sweep reclaimed (or would reclaim).</summary>
    public List<KahunaBackupGcOrphan> OrphanReclamations { get; set; } = [];
}

/// <summary>A backup deleted (or slated for deletion) by the retention policy.</summary>
public sealed class KahunaBackupGcDeletion
{
    public Guid BackupId { get; set; }
    public string Type { get; set; } = "";
    public DateTime CreatedAtUtc { get; set; }
    public long Bytes { get; set; }
    public string Reason { get; set; } = "";
}

/// <summary>An orphaned/leftover artifact reclaimed (or slated for reclaim) by the sweep. Only the
/// entry name is surfaced — never the absolute server path.</summary>
public sealed class KahunaBackupGcOrphan
{
    public string Name { get; set; } = "";
    public bool IsDirectory { get; set; }
    public string Reason { get; set; } = "";
}

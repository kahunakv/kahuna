namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaBackupIncrementalRequest
{
    public Guid ParentBackupId { get; set; }
}

public sealed class KahunaBackupRestoreRequest
{
    public Guid LeafBackupId { get; set; }
    public string TargetDir { get; set; } = "";
    /// <summary>HLC target time as ms since Unix epoch. 0 = chain max.</summary>
    public long TargetTimeMs { get; set; }
}

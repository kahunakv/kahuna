namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Describes a single backup artifact returned by the backup API.
/// </summary>
public sealed class KahunaBackupInfo
{
    public Guid BackupId { get; set; }
    public string Type { get; set; } = "";
    public DateTime CreatedAtUtc { get; set; }
    public Guid? ParentBackupId { get; set; }
    public int PartitionCount { get; set; }
    public int? ClusterSnapshotNode { get; set; }
    public long? ClusterSnapshotPhysical { get; set; }
    public uint? ClusterSnapshotCounter { get; set; }
}

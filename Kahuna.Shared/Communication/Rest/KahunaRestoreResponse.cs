namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Result of an offline restore-to-directory operation.
/// </summary>
public sealed class KahunaRestoreResponse
{
    public string TargetDir { get; set; } = "";
    public int PartitionsRestored { get; set; }
    public long EntriesApplied { get; set; }
    /// <summary>Physical ms component of the highest HLC applied.</summary>
    public long LastAppliedPhysicalMs { get; set; }
    public List<KahunaBackupInfo> Chain { get; set; } = [];
}

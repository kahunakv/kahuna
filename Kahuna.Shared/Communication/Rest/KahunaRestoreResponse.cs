using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Result of an offline restore-to-directory operation.
/// </summary>
public sealed class KahunaRestoreResponse
{
    [JsonPropertyName("targetDir")]
    public string TargetDir { get; set; } = "";
    [JsonPropertyName("partitionsRestored")]
    public int PartitionsRestored { get; set; }
    [JsonPropertyName("entriesApplied")]
    public long EntriesApplied { get; set; }
    /// <summary>Physical ms component of the highest HLC applied.</summary>
    [JsonPropertyName("lastAppliedPhysicalMs")]
    public long LastAppliedPhysicalMs { get; set; }
    [JsonPropertyName("chain")]
    public List<KahunaBackupInfo> Chain { get; set; } = [];

    /// <summary>
    /// Typed outcome of the restore. <see cref="KahunaBackupOutcome.Ok"/> on success; a failed
    /// restore surfaces the reason here (in addition to throwing) so callers can branch on the enum.
    /// </summary>
    [JsonPropertyName("outcome")]
    public KahunaBackupOutcome Outcome { get; set; } = KahunaBackupOutcome.Ok;

    /// <summary>Physical-ms component of the earliest recoverable HLC of the restored chain (its base cut).</summary>
    [JsonPropertyName("minRecoverablePhysicalMs")]
    public long MinRecoverablePhysicalMs { get; set; }

    /// <summary>Physical-ms component of the latest recoverable HLC of the restored chain.</summary>
    [JsonPropertyName("maxRecoverablePhysicalMs")]
    public long MaxRecoverablePhysicalMs { get; set; }
}

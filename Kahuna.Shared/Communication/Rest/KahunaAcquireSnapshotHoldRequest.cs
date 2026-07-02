using Kommander.Time;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaAcquireSnapshotHoldRequest
{
    [JsonPropertyName("holderId")]
    public string HolderId { get; set; } = "";

    [JsonPropertyName("timestamp")]
    public HLCTimestamp Timestamp { get; set; }

    [JsonPropertyName("leaseMs")]
    public int LeaseMs { get; set; }
}

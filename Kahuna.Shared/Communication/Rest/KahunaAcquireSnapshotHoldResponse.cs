using Kahuna.Shared.KeyValue;
using Kommander.Time;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaAcquireSnapshotHoldResponse
{
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("holdId")]
    public string HoldId { get; set; } = "";

    [JsonPropertyName("leaseExpiry")]
    public HLCTimestamp LeaseExpiry { get; set; }
}

using System.Text.Json.Serialization;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>The REST answer of the history hold registry (see <see cref="HistoryHoldAnswer"/>).</summary>
public sealed class KahunaHistoryHoldResponse
{
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("holds")]
    public List<KahunaHistoryHoldInfoResponse>? Holds { get; set; }
}

/// <summary>One registered history hold, as the REST answer carries it.</summary>
public sealed class KahunaHistoryHoldInfoResponse
{
    [JsonPropertyName("holdId")]
    public string HoldId { get; set; } = "";

    [JsonPropertyName("holderId")]
    public string HolderId { get; set; } = "";

    [JsonPropertyName("partitionId")]
    public int PartitionId { get; set; }

    [JsonPropertyName("timestamp")]
    public HLCTimestamp Timestamp { get; set; }

    [JsonPropertyName("leaseExpiry")]
    public HLCTimestamp LeaseExpiry { get; set; }
}

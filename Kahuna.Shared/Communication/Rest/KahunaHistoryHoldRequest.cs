using System.Text.Json.Serialization;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>The REST body of a request to the history hold registry (see <see cref="HistoryHoldRequest"/>).</summary>
public sealed class KahunaHistoryHoldRequest
{
    [JsonPropertyName("operation")]
    public HistoryHoldOperation Operation { get; set; }

    [JsonPropertyName("holderId")]
    public string? HolderId { get; set; }

    [JsonPropertyName("partitions")]
    public List<int>? Partitions { get; set; }

    [JsonPropertyName("timestamp")]
    public HLCTimestamp Timestamp { get; set; }

    [JsonPropertyName("leaseMs")]
    public int LeaseMs { get; set; }

    [JsonPropertyName("holdIds")]
    public List<string>? HoldIds { get; set; }
}

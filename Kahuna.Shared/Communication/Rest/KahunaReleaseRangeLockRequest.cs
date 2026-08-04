
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaReleaseRangeLockRequest
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }

    [JsonPropertyName("prefix")]
    public string? Prefix { get; set; }

    [JsonPropertyName("startKey")]
    public string? StartKey { get; set; }

    [JsonPropertyName("startInclusive")]
    public bool StartInclusive { get; set; }

    [JsonPropertyName("endKey")]
    public string? EndKey { get; set; }

    [JsonPropertyName("endInclusive")]
    public bool EndInclusive { get; set; }

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }
}

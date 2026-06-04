using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.KeyValue;

public sealed class KahunaDeleteKeyValueRequestItem
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }

    [JsonPropertyName("key")]
    public string? Key { get; set; }

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }
}

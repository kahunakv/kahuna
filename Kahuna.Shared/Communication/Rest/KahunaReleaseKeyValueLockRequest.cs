
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Release request for a per-key or prefix key-value lock.
/// </summary>
public sealed class KahunaReleaseKeyValueLockRequest
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }

    [JsonPropertyName("key")]
    public string? Key { get; set; }

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }
}

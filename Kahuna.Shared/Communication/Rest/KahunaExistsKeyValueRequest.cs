
using Kommander.Time;
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaExistsKeyValueRequest
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }
    
    [JsonPropertyName("key")]
    public string? Key { get; set; }
    
    [JsonPropertyName("revision")]
    public long Revision { get; set; }

    /// <summary>
    /// Snapshot to read as of. Zero reads the latest committed value.
    /// </summary>
    [JsonPropertyName("readTimestamp")]
    public HLCTimestamp ReadTimestamp { get; set; }

    [JsonPropertyName("value")]
    public KeyValueDurability Durability { get; set; }
}
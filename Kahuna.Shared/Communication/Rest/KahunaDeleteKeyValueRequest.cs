
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Represents a request to delete a key-value pair in the Kahuna system.
/// </summary>
public sealed class KahunaDeleteKeyValueRequest
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }   
    
    [JsonPropertyName("key")]
    public string? Key { get; set; }
    
    [JsonPropertyName("value")]
    public KeyValueDurability Durability { get; set; }
}
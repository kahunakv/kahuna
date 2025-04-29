using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.KeyValue;

public sealed class KahunaSetKeyValueRequestItem
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }
    
    [JsonPropertyName("key")]
    public string? Key { get; set; }
    
    [JsonPropertyName("value")]
    public byte[]? Value { get; set; }
    
    [JsonPropertyName("compareValue")]
    public byte[]? CompareValue { get; set; }
    
    [JsonPropertyName("compareRevision")]
    public long CompareRevision { get; set; }
    
    [JsonPropertyName("expiresMs")]
    public int ExpiresMs { get; set; }
    
    [JsonPropertyName("flags")]
    public KeyValueFlags Flags { get; set; }
    
    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }
}

using System.Text.Json.Serialization;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaSetKeyValueRequest
{
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
    
    [JsonPropertyName("consistency")]
    public KeyValueConsistency Consistency { get; set; }
}
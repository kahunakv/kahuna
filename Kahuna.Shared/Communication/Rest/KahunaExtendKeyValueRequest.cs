
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaExtendKeyValueRequest
{
    [JsonPropertyName("key")]
    public string? Key { get; set; }
    
    [JsonPropertyName("expiresMs")]
    public int ExpiresMs { get; set; }
    
    [JsonPropertyName("value")]
    public KeyValueConsistency Consistency { get; set; }
}
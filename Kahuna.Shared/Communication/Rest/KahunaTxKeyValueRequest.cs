
using System.Text.Json.Serialization;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaTxKeyValueRequest
{
    [JsonPropertyName("hash")]
    public string? Hash { get; set; }
    
    [JsonPropertyName("script")]
    public byte[]? Script { get; set; }
    
    [JsonPropertyName("parameters")]
    public List<KeyValueParameter>? Parameters { get; set; }
}
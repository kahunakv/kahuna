
using System.Text.Json.Serialization;

namespace Kahuna.Shared.KeyValue;

public sealed class KeyValueTransactionRequest
{
    [JsonPropertyName("hash")]
    public string? Hash { get; set; }
    
    [JsonPropertyName("script")]
    public byte[]? Script { get; set; }
    
    [JsonPropertyName("parameters")]
    public List<KeyValueParameter>? Parameters { get; set; }
}
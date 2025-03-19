
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaTxKeyValueRequest
{
    [JsonPropertyName("hash")]
    public string? Hash { get; set; }
    
    [JsonPropertyName("script")]
    public byte[]? Script { get; set; }
}
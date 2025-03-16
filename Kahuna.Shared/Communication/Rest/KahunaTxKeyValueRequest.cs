
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaTxKeyValueRequest
{
    [JsonPropertyName("script")]
    public string? Script { get; set; }
}
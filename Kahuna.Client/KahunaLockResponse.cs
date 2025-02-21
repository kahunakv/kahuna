using System.Text.Json.Serialization;

namespace Kahuna.Client;

public sealed class KahunaLockResponse
{
    [JsonPropertyName("type")]
    public int Type { get; set; }
}

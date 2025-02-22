
using System.Text.Json.Serialization;

namespace Kahuna.Client.Communication.Data;

internal sealed class KahunaLockResponse
{
    [JsonPropertyName("type")]
    public int Type { get; set; }
}

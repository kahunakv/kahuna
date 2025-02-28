
using System.Text.Json.Serialization;

namespace Kahuna.Client.Communication.Data;

internal sealed class KahunaLockResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }
    
    [JsonPropertyName("type")]
    public LockResponseType Type { get; set; }
    
    [JsonPropertyName("fencingToken")]
    public long FencingToken { get; set; }
}


using System.Text.Json.Serialization;

namespace Kahuna;

public sealed class ExternLockResponse
{
    [JsonPropertyName("type")]
    public LockResponseType Type { get; set; }
    
    [JsonPropertyName("fencingToken")]
    public long FencingToken { get; set; }
}
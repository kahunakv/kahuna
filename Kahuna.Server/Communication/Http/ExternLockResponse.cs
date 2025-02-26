
using System.Text.Json.Serialization;
using Kahuna.Locks;

namespace Kahuna.Communication.Http;

public sealed class ExternLockResponse
{
    [JsonPropertyName("type")]
    public LockResponseType Type { get; set; }
    
    [JsonPropertyName("fencingToken")]
    public long FencingToken { get; set; }
}
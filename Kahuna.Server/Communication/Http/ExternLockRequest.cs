
using System.Text.Json.Serialization;
using Kahuna.Locks;

namespace Kahuna.Communication.Http;

public sealed class ExternLockRequest
{
    [JsonPropertyName("lockName")]
    public string? LockName { get; set; }
    
    [JsonPropertyName("lockId")]
    public string? LockId { get; set; }
    
    [JsonPropertyName("expiresMs")]
    public int ExpiresMs { get; set; }
    
    [JsonPropertyName("consistency")]
    public LockConsistency Consistency { get; set; }
}
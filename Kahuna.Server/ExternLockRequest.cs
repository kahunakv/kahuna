
using System.Text.Json.Serialization;

namespace Kahuna;

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
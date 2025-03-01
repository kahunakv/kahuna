
using Kahuna.Shared.Locks;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaLockRequest
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
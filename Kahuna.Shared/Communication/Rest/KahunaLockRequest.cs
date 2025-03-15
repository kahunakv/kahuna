
using Kahuna.Shared.Locks;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaLockRequest
{
    [JsonPropertyName("lockName")]
    public string? Resource { get; set; }
    
    [JsonPropertyName("lockId")]
    public byte[]? Owner { get; set; }
    
    [JsonPropertyName("expiresMs")]
    public int ExpiresMs { get; set; }
    
    [JsonPropertyName("consistency")]
    public LockConsistency Consistency { get; set; }
}
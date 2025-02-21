
using System.Text.Json.Serialization;

namespace Kahuna.Client;

public sealed class KahunaLockRequest
{
    [JsonPropertyName("lockName")]
    public string? LockName { get; set; }
    
    [JsonPropertyName("lockId")]
    public string? LockId { get; set; }
    
    [JsonPropertyName("expiresMs")]
    public double ExpiresMs { get; set; }
}
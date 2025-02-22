
using System.Text.Json.Serialization;

namespace Kahuna.Client.Communication.Data;

internal sealed class KahunaLockRequest
{
    [JsonPropertyName("lockName")]
    public string? LockName { get; set; }
    
    [JsonPropertyName("lockId")]
    public string? LockId { get; set; }
    
    [JsonPropertyName("expiresMs")]
    public double ExpiresMs { get; set; }
}
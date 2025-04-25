
using Kahuna.Shared.Locks;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Represents a request to perform operations on a distributed lock in the Kahuna system.
/// </summary>
public sealed class KahunaLockRequest
{
    [JsonPropertyName("resource")]
    public string? Resource { get; set; }
    
    [JsonPropertyName("lockId")]
    public byte[]? Owner { get; set; }
    
    [JsonPropertyName("expiresMs")]
    public int ExpiresMs { get; set; }
    
    [JsonPropertyName("durability")]
    public LockDurability Durability { get; set; }
}
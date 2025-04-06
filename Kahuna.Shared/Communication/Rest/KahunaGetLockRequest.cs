
using Kahuna.Shared.Locks;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaGetLockRequest
{
    [JsonPropertyName("resource")]
    public string? Resource { get; set; }
    
    [JsonPropertyName("durability")]
    public LockDurability Durability { get; set; }
}

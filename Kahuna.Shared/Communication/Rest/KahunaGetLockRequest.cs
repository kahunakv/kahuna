
using Kahuna.Shared.Locks;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaGetLockRequest
{
    [JsonPropertyName("lockName")]
    public string? LockName { get; set; }
    
    [JsonPropertyName("consistency")]
    public LockConsistency Consistency { get; set; }
}

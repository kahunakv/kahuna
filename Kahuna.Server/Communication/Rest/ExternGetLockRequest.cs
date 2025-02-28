
using System.Text.Json.Serialization;
using Kahuna.Locks;

namespace Kahuna.Communication.Rest;

public sealed class ExternGetLockRequest
{
    [JsonPropertyName("lockName")]
    public string? LockName { get; set; }
    
    [JsonPropertyName("consistency")]
    public LockConsistency Consistency { get; set; }
}

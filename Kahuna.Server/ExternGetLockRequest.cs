
using System.Text.Json.Serialization;

namespace Kahuna;

public sealed class ExternGetLockRequest
{
    [JsonPropertyName("lockName")]
    public string? LockName { get; set; }
    
    [JsonPropertyName("consistency")]
    public LockConsistency Consistency { get; set; }
}

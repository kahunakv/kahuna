
using System.Text.Json.Serialization;

namespace Kahuna.Client.Communication.Data;

internal sealed class KahunaGetRequest
{
    [JsonPropertyName("lockName")]
    public string? LockName { get; set; }    
}
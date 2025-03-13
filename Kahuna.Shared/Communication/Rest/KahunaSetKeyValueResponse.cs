
using System.Text.Json.Serialization;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaSetKeyValueResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }
    
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }
    
    [JsonPropertyName("revision")]
    public long Revision { get; set; }
}
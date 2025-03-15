
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaExtendKeyValueResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }
    
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }
    
    [JsonPropertyName("revision")]
    public long Revision { get; set; }
}
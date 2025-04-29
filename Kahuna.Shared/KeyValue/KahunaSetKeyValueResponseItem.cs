
using Kommander.Time;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.KeyValue;

public sealed class KahunaSetKeyValueResponseItem
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }
    
    [JsonPropertyName("key")]
    public string? Key { get; set; }
    
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }
    
    [JsonPropertyName("revision")]
    public long Revision { get; set; }
    
    [JsonPropertyName("lastModified")]
    public HLCTimestamp LastModified { get; set; }
    
    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }
}

using Kommander.Time;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.KeyValue;

public class KeyValueGetByPrefixItem
{
    [JsonPropertyName("key")]
    public string? Key { get; set; }
    
    [JsonPropertyName("value")]
    public byte[]? Value { get; set; }
    
    [JsonPropertyName("revision")]
    public long Revision { get; set; }
    
    [JsonPropertyName("lastModified")]
    public HLCTimestamp LastModified { get; set; }
}
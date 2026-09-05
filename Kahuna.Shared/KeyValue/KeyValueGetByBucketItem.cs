
using Kahuna.Shared.Communication.Rest;
using Kommander.Time;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.KeyValue;

public class KeyValueGetByBucketItem
{
    [JsonPropertyName("key")]
    public string? Key { get; set; }
    
    [JsonPropertyName("value")]
    [JsonConverter(typeof(KeyValuePayloadJsonConverter))]
    public byte[]? Value { get; set; }
    
    [JsonPropertyName("revision")]
    public long Revision { get; set; }
    
    [JsonPropertyName("lastModified")]
    public HLCTimestamp LastModified { get; set; }
}
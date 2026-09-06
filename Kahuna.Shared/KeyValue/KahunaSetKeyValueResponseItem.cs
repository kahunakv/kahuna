
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

    /// <summary>
    /// 1-based index into the enclosing response's <c>Routes</c> table; 0 when no route was
    /// resolved for this item.
    /// </summary>
    [JsonPropertyName("routeIndex")]
    public int RouteIndex { get; set; }
}
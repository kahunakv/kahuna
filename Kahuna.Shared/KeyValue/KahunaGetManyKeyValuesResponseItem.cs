
using System.Text.Json.Serialization;
using Kahuna.Shared.Communication.Rest;
using Kommander.Time;

namespace Kahuna.Shared.KeyValue;

public sealed class KahunaGetManyKeyValuesResponseItem
{
    [JsonPropertyName("key")]
    public string? Key { get; set; }

    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("value")]
    [JsonConverter(typeof(KeyValuePayloadJsonConverter))]
    public byte[]? Value { get; set; }

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

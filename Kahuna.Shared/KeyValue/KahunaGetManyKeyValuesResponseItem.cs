
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.KeyValue;

public sealed class KahunaGetManyKeyValuesResponseItem
{
    [JsonPropertyName("key")]
    public string? Key { get; set; }

    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("value")]
    public byte[]? Value { get; set; }

    [JsonPropertyName("revision")]
    public long Revision { get; set; }

    [JsonPropertyName("lastModified")]
    public HLCTimestamp LastModified { get; set; }

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }
}

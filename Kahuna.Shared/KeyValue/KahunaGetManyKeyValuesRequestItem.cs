
using System.Text.Json.Serialization;

namespace Kahuna.Shared.KeyValue;

public sealed class KahunaGetManyKeyValuesRequestItem
{
    [JsonPropertyName("key")]
    public string? Key { get; set; }

    [JsonPropertyName("revision")]
    public long Revision { get; set; } = -1;

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }
}


using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaGetByRangeResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }

    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("items")]
    public List<KeyValueGetByBucketItem>? Items { get; set; }

    [JsonPropertyName("nextCursor")]
    public string? NextCursor { get; set; }

    [JsonPropertyName("hasMore")]
    public bool HasMore { get; set; }
}

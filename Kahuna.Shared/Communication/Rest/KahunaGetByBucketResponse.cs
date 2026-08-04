
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Response for the bucket and prefix scan endpoints.
/// </summary>
public sealed class KahunaGetByBucketResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }

    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("items")]
    public List<KeyValueGetByBucketItem>? Items { get; set; }
}


using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Batched point-read response, shared by the get-many and exists-many endpoints.
/// </summary>
public sealed class KahunaManyKeyValuesResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }

    [JsonPropertyName("items")]
    public List<KahunaGetManyKeyValuesResponseItem>? Items { get; set; }

    [JsonPropertyName("timeElapsedMs")]
    public int TimeElapsedMs { get; set; }
}

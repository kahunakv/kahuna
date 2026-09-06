using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaDeleteManyKeyValueResponse
{
    /// <summary>
    /// Envelope-level outcome: <see cref="KeyValueResponseType.Deleted"/> when the batch was
    /// answered (per-item outcomes live in <see cref="Items"/>). Lets the substituted MustRetry
    /// body a retryable infrastructure failure produces deserialize as a classifiable refusal
    /// instead of an empty item list.
    /// </summary>
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("items")]
    public List<KahunaDeleteKeyValueResponseItem>? Items { get; set; }

    [JsonPropertyName("timeElapsedMs")]
    public int TimeElapsedMs { get; set; }

    /// <summary>
    /// Deduplicated routing hints for the items in this response, referenced by each item's
    /// <c>RouteIndex</c>, so a large batch does not repeat one endpoint string per item.
    /// </summary>
    [JsonPropertyName("routes")]
    public List<KahunaRouteHint>? Routes { get; set; }
}

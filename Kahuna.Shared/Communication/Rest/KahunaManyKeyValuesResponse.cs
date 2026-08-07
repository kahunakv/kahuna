
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Batched point-read response, shared by the get-many and exists-many endpoints.
/// </summary>
public sealed class KahunaManyKeyValuesResponse
{
    /// <summary>
    /// Envelope-level outcome: <see cref="KeyValueResponseType.Get"/> when the batch was answered
    /// (per-item outcomes live in <see cref="Items"/>). Retryable infrastructure failures are
    /// answered with a substituted <c>{"type":101}</c> (MustRetry) body; without this field that
    /// refusal would deserialize as an empty item list — "none of these keys exist" instead of
    /// "nothing was measured".
    /// </summary>
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }

    [JsonPropertyName("items")]
    public List<KahunaGetManyKeyValuesResponseItem>? Items { get; set; }

    [JsonPropertyName("timeElapsedMs")]
    public int TimeElapsedMs { get; set; }
}

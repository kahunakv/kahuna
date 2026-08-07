using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaSetManyKeyValueResponse
{
    /// <summary>
    /// Envelope-level outcome: <see cref="KeyValueResponseType.Set"/> when the batch was answered
    /// (per-item outcomes live in <see cref="Items"/>). Lets the substituted MustRetry body a
    /// retryable infrastructure failure produces deserialize as a classifiable refusal instead of
    /// an empty item list.
    /// </summary>
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("items")]
    public List<KahunaSetKeyValueResponseItem>? Items { get; set; }

    [JsonPropertyName("timeElapsedMs")]
    public int TimeElapsedMs { get; set; }
}

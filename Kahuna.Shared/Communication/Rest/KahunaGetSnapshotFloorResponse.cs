using Kommander.Time;
using System.Text.Json.Serialization;

using Kahuna.Shared.KeyValue;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaGetSnapshotFloorResponse
{
    /// <summary>
    /// <see cref="KeyValueResponseType.Get"/> on success. Like every other response on the key/value
    /// REST surface this must carry a type: retryable infrastructure failures are answered with a
    /// substituted <c>{"type":101}</c> (MustRetry) body, and without this field that refusal would
    /// deserialize as an empty success — zero live holds instead of "nothing was measured".
    /// </summary>
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("effectiveFloor")]
    public HLCTimestamp EffectiveFloor { get; set; }

    [JsonPropertyName("liveHolds")]
    public int LiveHolds { get; set; }
}

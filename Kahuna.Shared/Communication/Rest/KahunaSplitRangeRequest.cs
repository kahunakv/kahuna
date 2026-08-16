using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Asks for the range covering <see cref="SplitKey"/> to be split at exactly that key:
/// <c>POST /v1/ranges/split</c>.
/// </summary>
public sealed class KahunaSplitRangeRequest
{
    /// <summary>The key space whose range is split.</summary>
    [JsonPropertyName("keySpace")]
    public string KeySpace { get; set; } = "";

    /// <summary>
    /// Where to cut. The covering range <c>[S, E)</c> becomes <c>[S, splitKey)</c> and
    /// <c>[splitKey, E)</c>, so the key itself lands in the <b>upper</b> half. Compared ordinally,
    /// like every other range bound.
    /// </summary>
    [JsonPropertyName("splitKey")]
    public string SplitKey { get; set; } = "";
}

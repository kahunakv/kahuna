
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaScanAllByPrefixRequest
{
    [JsonPropertyName("prefixKey")]
    public string? PrefixKey { get; set; }

    /// <summary>
    /// Snapshot to read as of. Zero reads the latest committed value.
    /// </summary>
    [JsonPropertyName("readTimestamp")]
    public HLCTimestamp ReadTimestamp { get; set; }

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }
}


using Kommander.Time;
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaExistsKeyValueResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }
    
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }
    
    [JsonPropertyName("revision")]
    public long Revision { get; set; }
    
    [JsonPropertyName("expires")]
    public HLCTimestamp Expires { get; set; }

    /// <summary>
    /// When the entry was last written. Callers round-trip this into a later snapshot read, so it
    /// must carry the real commit time rather than a placeholder.
    /// </summary>
    [JsonPropertyName("lastModified")]
    public HLCTimestamp LastModified { get; set; }
}
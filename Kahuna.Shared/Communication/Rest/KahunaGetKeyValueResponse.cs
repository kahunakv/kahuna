
using System.Text.Json.Serialization;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaGetKeyValueResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }
    
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }
    
    [JsonPropertyName("value")]
    [JsonConverter(typeof(KeyValuePayloadJsonConverter))]
    public byte[]? Value { get; set; }
    
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

    /// <summary>
    /// Advisory routing hint for the resource this response is about. Null when the answering node
    /// resolved no owner, and absent from an older server, in both cases leaving the caller on its
    /// existing endpoint selection.
    /// </summary>
    [JsonPropertyName("route")]
    public KahunaRouteHint? Route { get; set; }
}

using Kommander.Time;
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Represents the response for a delete key-value operation in Kahuna.
/// </summary>
public sealed class KahunaDeleteKeyValueResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }
    
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }
    
    [JsonPropertyName("revision")]
    public long Revision { get; set; }
    
    [JsonPropertyName("lastModified")]
    public HLCTimestamp LastModified { get; set; }
}
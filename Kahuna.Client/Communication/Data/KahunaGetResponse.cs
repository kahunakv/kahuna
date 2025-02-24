
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Client.Communication.Data;

internal sealed class KahunaGetResponse
{
    [JsonPropertyName("type")]
    public LockResponseType Type { get; set; }
    
    [JsonPropertyName("owner")]
    public string? Owner { get; set; }
    
    [JsonPropertyName("expires")]
    public HLCTimestamp Expires { get; set; }
    
    [JsonPropertyName("fencingToken")]
    public long FencingToken { get; set; }
}

using Kommander.Time;
using Kahuna.Shared.Locks;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaGetLockResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }
    
    [JsonPropertyName("type")]
    public LockResponseType Type { get; set; }
    
    [JsonPropertyName("owner")]
    public byte[]? Owner { get; set; }
    
    [JsonPropertyName("expires")]
    public HLCTimestamp Expires { get; set; }
    
    [JsonPropertyName("fencingToken")]
    public long FencingToken { get; set; }

    /// <summary>
    /// Advisory routing hint for the resource this response is about. Null when the answering node
    /// resolved no owner, and absent from an older server, in both cases leaving the caller on its
    /// existing endpoint selection.
    /// </summary>
    [JsonPropertyName("route")]
    public KahunaRouteHint? Route { get; set; }
}
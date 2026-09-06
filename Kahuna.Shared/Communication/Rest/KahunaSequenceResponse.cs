using System.Text.Json.Serialization;
using Kahuna.Shared.Sequences;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaSequenceResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }

    [JsonPropertyName("type")]
    public SequenceResponseType Type { get; set; }

    [JsonPropertyName("sequence")]
    public ReadOnlySequenceEntry? Sequence { get; set; }

    [JsonPropertyName("allocation")]
    public SequenceAllocation Allocation { get; set; }

    [JsonPropertyName("revision")]
    public long Revision { get; set; }

    /// <summary>
    /// Advisory routing hint for the resource this response is about. Null when the answering node
    /// resolved no owner, and absent from an older server, in both cases leaving the caller on its
    /// existing endpoint selection.
    /// </summary>
    [JsonPropertyName("route")]
    public KahunaRouteHint? Route { get; set; }
}

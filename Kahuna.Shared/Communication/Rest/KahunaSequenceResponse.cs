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
}

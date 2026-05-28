using System.Text.Json.Serialization;
using Kahuna.Shared.Sequences;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaSequenceCreateRequest
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("initialValue")]
    public long InitialValue { get; set; }

    [JsonPropertyName("increment")]
    public long Increment { get; set; } = 1;

    [JsonPropertyName("maxValue")]
    public long? MaxValue { get; set; }

    [JsonPropertyName("durability")]
    public SequenceDurability Durability { get; set; } = SequenceDurability.Persistent;
}

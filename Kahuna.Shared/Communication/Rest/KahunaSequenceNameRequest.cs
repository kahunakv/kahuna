using System.Text.Json.Serialization;
using Kahuna.Shared.Sequences;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaSequenceNameRequest
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("durability")]
    public SequenceDurability Durability { get; set; } = SequenceDurability.Persistent;
}

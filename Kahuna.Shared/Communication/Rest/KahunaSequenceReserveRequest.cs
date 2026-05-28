using System.Text.Json.Serialization;
using Kahuna.Shared.Sequences;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaSequenceReserveRequest
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    [JsonPropertyName("count")]
    public int Count { get; set; }

    [JsonPropertyName("idempotencyKey")]
    public string? IdempotencyKey { get; set; }

    [JsonPropertyName("durability")]
    public SequenceDurability Durability { get; set; } = SequenceDurability.Persistent;
}

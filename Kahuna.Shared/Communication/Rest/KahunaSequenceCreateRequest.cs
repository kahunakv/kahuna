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

    /// <summary>
    /// Values this sequence reserves per commit. Null leaves it on the server-wide setting; <c>1</c> is
    /// gap-free at one commit, with its fsync, per value.
    /// </summary>
    [JsonPropertyName("blockSize")]
    public int? BlockSize { get; set; }

    [JsonPropertyName("durability")]
    public SequenceDurability Durability { get; set; } = SequenceDurability.Persistent;
}

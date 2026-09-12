using System.Text.Json.Serialization;
using Kahuna.Shared.Sequences;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Body of <c>POST /v1/sequences/update</c>. Every parameter is optional: an omitted field leaves that
/// parameter of the record exactly as it is.
///
/// <para><c>maxValue</c> and <c>blockSize</c> are optional on the record too, so omitting them cannot
/// also mean "remove the setting". Each therefore has a companion <c>remove*</c> flag that does.</para>
/// </summary>
public sealed class KahunaSequenceUpdateRequest
{
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    /// <summary>New reserved high-water mark. The next value issued is this plus the increment.</summary>
    [JsonPropertyName("currentValue")]
    public long? CurrentValue { get; set; }

    [JsonPropertyName("increment")]
    public long? Increment { get; set; }

    /// <summary>New recorded starting value. Descriptive only; it does not move the counter.</summary>
    [JsonPropertyName("initialValue")]
    public long? InitialValue { get; set; }

    [JsonPropertyName("maxValue")]
    public long? MaxValue { get; set; }

    [JsonPropertyName("removeMaxValue")]
    public bool RemoveMaxValue { get; set; }

    /// <summary>Values this sequence reserves per commit. <c>1</c> is gap-free at one commit per value.</summary>
    [JsonPropertyName("blockSize")]
    public int? BlockSize { get; set; }

    [JsonPropertyName("removeBlockSize")]
    public bool RemoveBlockSize { get; set; }

    [JsonPropertyName("durability")]
    public SequenceDurability Durability { get; set; } = SequenceDurability.Persistent;

    public SequenceUpdate ToUpdate() =>
        new(CurrentValue, Increment, InitialValue, MaxValue, RemoveMaxValue, BlockSize, RemoveBlockSize);
}


using System.Text.Json.Serialization;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaTxKeyValueRequest
{
    [JsonPropertyName("hash")]
    public string? Hash { get; set; }
    
    [JsonPropertyName("script")]
    public byte[]? Script { get; set; }
    
    [JsonPropertyName("parameters")]
    public List<KeyValueParameter>? Parameters { get; set; }

    /// <summary>
    /// Admission priority for the transaction. Absent from an older client's payload, which deserializes to
    /// <see cref="TransactionPriority.Normal"/> — the behavior that existed before priorities.
    /// </summary>
    [JsonPropertyName("priority")]
    public TransactionPriority Priority { get; set; } = TransactionPriority.Normal;
}
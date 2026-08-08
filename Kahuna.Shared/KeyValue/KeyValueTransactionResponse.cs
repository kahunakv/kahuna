
using System.Text.Json.Serialization;
using Kahuna.Shared.Communication.Rest;

namespace Kahuna.Shared.KeyValue;

public sealed class KeyValueTransactionResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }

    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("reason")]
    public string? Reason { get; set; }

    [JsonPropertyName("value")]
    public byte[]? Value { get; set; }

    [JsonPropertyName("revision")]
    public long Revision { get; set; }

    /// <summary>Per-value results of the script, matching the gRPC script wire's fidelity.</summary>
    [JsonPropertyName("values")]
    public List<KahunaTxKeyValueResponseItem>? Values { get; set; }
}
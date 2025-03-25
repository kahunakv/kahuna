
using System.Text.Json.Serialization;

namespace Kahuna.Shared.KeyValue;

public sealed class KeyValueTransactionRequest
{
    [JsonPropertyName("script")]
    public byte[]? Script { get; set; }
}
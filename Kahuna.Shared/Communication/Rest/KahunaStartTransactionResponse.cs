
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaStartTransactionResponse
{
    [JsonPropertyName("servedFrom")]
    public string? ServedFrom { get; set; }

    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }
}

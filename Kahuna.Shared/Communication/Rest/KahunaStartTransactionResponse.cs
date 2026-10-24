
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

    /// <summary>
    /// The coordinator key the session is routed by, which the caller sends on every later call. It differs
    /// from the requested key when the request chose a coordinator partition. Null from a server that does not
    /// return it; the requested key then applies.
    /// </summary>
    [JsonPropertyName("coordinatorKey")]
    public string? CoordinatorKey { get; set; }
}

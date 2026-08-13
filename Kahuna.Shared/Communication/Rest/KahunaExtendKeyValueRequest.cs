
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaExtendKeyValueRequest
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }
    
    [JsonPropertyName("key")]
    public string? Key { get; set; }
    
    [JsonPropertyName("expiresMs")]
    public int ExpiresMs { get; set; }
    
    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }

    [JsonPropertyName("coordinatorKey")]
    public string? CoordinatorKey { get; set; }

    [JsonPropertyName("operationIdHigh")]
    public ulong OperationIdHigh { get; set; }

    [JsonPropertyName("operationIdLow")]
    public ulong OperationIdLow { get; set; }
}
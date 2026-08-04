
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaGetByBucketRequest
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }

    [JsonPropertyName("prefixKey")]
    public string? PrefixKey { get; set; }

    /// <summary>
    /// Snapshot to read as of. Zero reads the latest committed value.
    /// </summary>
    [JsonPropertyName("readTimestamp")]
    public HLCTimestamp ReadTimestamp { get; set; }

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }

    [JsonPropertyName("coordinatorKey")]
    public string? CoordinatorKey { get; set; }

    [JsonPropertyName("operationIdHigh")]
    public ulong OperationIdHigh { get; set; }

    [JsonPropertyName("operationIdLow")]
    public ulong OperationIdLow { get; set; }
}

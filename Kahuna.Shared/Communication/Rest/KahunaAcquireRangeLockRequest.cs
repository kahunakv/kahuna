
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaAcquireRangeLockRequest
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }

    [JsonPropertyName("prefix")]
    public string? Prefix { get; set; }

    [JsonPropertyName("startKey")]
    public string? StartKey { get; set; }

    [JsonPropertyName("startInclusive")]
    public bool StartInclusive { get; set; }

    [JsonPropertyName("endKey")]
    public string? EndKey { get; set; }

    [JsonPropertyName("endInclusive")]
    public bool EndInclusive { get; set; }

    [JsonPropertyName("expiresMs")]
    public int ExpiresMs { get; set; }

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }

    /// <summary>
    /// Shared or exclusive acquisition. Matches <see cref="RangeLockMode"/>.
    /// </summary>
    [JsonPropertyName("mode")]
    public RangeLockMode Mode { get; set; }

    [JsonPropertyName("coordinatorKey")]
    public string? CoordinatorKey { get; set; }

    [JsonPropertyName("operationIdHigh")]
    public ulong OperationIdHigh { get; set; }

    [JsonPropertyName("operationIdLow")]
    public ulong OperationIdLow { get; set; }
}

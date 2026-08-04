
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Acquire request for a per-key or prefix key-value lock. <see cref="Key"/> carries the key for the
/// per-key endpoint and the prefix for the prefix endpoint.
/// </summary>
public sealed class KahunaAcquireKeyValueLockRequest
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

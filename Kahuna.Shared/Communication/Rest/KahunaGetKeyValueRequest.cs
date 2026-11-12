
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaGetKeyValueRequest
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }
    
    [JsonPropertyName("key")]
    public string? Key { get; set; }
    
    [JsonPropertyName("revision")]
    public long Revision { get; set; }

    /// <summary>
    /// Snapshot to read as of. Zero reads the latest committed value.
    /// </summary>
    [JsonPropertyName("readTimestamp")]
    public HLCTimestamp ReadTimestamp { get; set; }

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }

    /// <summary>
    /// How the read treats the transaction's own state at <see cref="ReadTimestamp"/>. Absent means the default.
    /// </summary>
    [JsonPropertyName("readMode")]
    public KeyValueReadMode ReadMode { get; set; }

    /// <summary>
    /// Most milliseconds the first latest read of the key by a transaction may wait behind a live write intent of
    /// another transaction (see <see cref="KeyValueIntentWait"/>). Absent or zero means no wait.
    /// </summary>
    [JsonPropertyName("intentWaitMs")]
    public int IntentWaitMs { get; set; }

    /// <summary>Whether the reading transaction already holds a write intent or a lock (see <see cref="KeyValueIntentWait"/>).</summary>
    [JsonPropertyName("intentWaitHoldsLocks")]
    public bool IntentWaitHoldsLocks { get; set; }

    [JsonPropertyName("coordinatorKey")]
    public string? CoordinatorKey { get; set; }

    [JsonPropertyName("operationIdHigh")]
    public ulong OperationIdHigh { get; set; }

    [JsonPropertyName("operationIdLow")]
    public ulong OperationIdLow { get; set; }
}

using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Opens an interactive transaction session. Mirrors the options the gRPC start RPC carries.
/// </summary>
public sealed class KahunaStartTransactionRequest
{
    [JsonPropertyName("coordinatorKey")]
    public string? CoordinatorKey { get; set; }

    [JsonPropertyName("timeout")]
    public int Timeout { get; set; }

    [JsonPropertyName("lockingType")]
    public KeyValueTransactionLocking LockingType { get; set; }

    [JsonPropertyName("asyncRelease")]
    public bool AsyncRelease { get; set; }

    [JsonPropertyName("autoCommit")]
    public bool AutoCommit { get; set; }

    [JsonPropertyName("readValidation")]
    public ReadValidation ReadValidation { get; set; }

    [JsonPropertyName("decisionDurability")]
    public DecisionDurability DecisionDurability { get; set; }

    [JsonPropertyName("priority")]
    public TransactionPriority Priority { get; set; }

    /// <summary>
    /// Transaction-wide snapshot for reads. Zero means latest.
    /// </summary>
    [JsonPropertyName("readTimestamp")]
    public HLCTimestamp ReadTimestamp { get; set; }
}

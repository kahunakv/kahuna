using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Client;

/// <summary>
/// Represents transaction options that can be configured when opening a Kahuna session.
/// </summary>
public sealed class KahunaTransactionOptions
{
    /// <summary>
    /// Timeout in milliseconds for the transaction.
    /// </summary>
    public int Timeout { get; set; }

    /// <summary>
    /// Specifies the locking strategy to be used for key-value transactions.
    /// </summary>
    public KeyValueTransactionLocking Locking { get; set; } = KeyValueTransactionLocking.Pessimistic;

    /// <summary>
    /// Whether the locks should be released asynchronously.
    /// </summary>
    public bool AsyncRelease { get; set; }
    
    /// <summary>
    /// Whether the transaction should be automatically committed after the session is disposed
    /// </summary>
    public bool AutoCommit { get; set; } = true;

    /// <summary>
    /// Controls whether reads are tracked and validated for write-skew at commit time.
    /// </summary>
    public ReadValidation ReadValidation { get; set; } = ReadValidation.None;

    /// <summary>
    /// Controls how durable the coordinator decision record must be before the client receives the outcome.
    /// </summary>
    public DecisionDurability DecisionDurability { get; set; } = DecisionDurability.BestEffort;

    /// <summary>
    /// Transaction-wide snapshot timestamp for reads. Zero means "latest".
    /// </summary>
    public HLCTimestamp ReadTimestamp { get; set; }
}
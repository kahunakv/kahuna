
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Represents configuration options for an interactive key-value transaction.
/// </summary>
public sealed class KeyValueTransactionOptions
{
    /// <summary>
    /// A stable key that pins this transaction to the partition whose leader owns the coordinator session.
    /// The client generates it once (typically a GUID) and reuses it for the lifetime of the transaction
    /// so that start, commit, and rollback are all routed to the same node.
    /// </summary>
    public string CoordinatorKey { get; set; } = string.Empty;

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
    /// Whether the transaction should be automatically committed after all operations are completed.
    /// </summary>
    public bool AutoCommit { get; set; }

    /// <summary>
    /// Controls whether reads are tracked and validated for write-skew at commit time.
    /// </summary>
    public ReadValidation ReadValidation { get; set; } = ReadValidation.None;

    /// <summary>
    /// Controls how durable the coordinator decision record must be before the client receives the outcome.
    /// </summary>
    public DecisionDurability DecisionDurability { get; set; } = DecisionDurability.BestEffort;
}

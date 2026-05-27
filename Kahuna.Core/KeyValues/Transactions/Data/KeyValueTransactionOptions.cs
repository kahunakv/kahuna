
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Represents configuration options for an interactive key-value transaction.
/// </summary>
public sealed class KeyValueTransactionOptions
{
    /// <summary>
    /// A unque identifier that allows to redirect the transaction to a specific partition.
    /// </summary>
    public string UniqueId { get; set; } = string.Empty;
    
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
}
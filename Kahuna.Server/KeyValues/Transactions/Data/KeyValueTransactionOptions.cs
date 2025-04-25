
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Represents configuration options for an interactive key-value transaction.
/// </summary>
public sealed class KeyValueTransactionOptions
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
}
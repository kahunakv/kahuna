
namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Stores the tracking state for a single operation registered under a transaction.
/// The coordinator uses this to detect duplicate submissions and return cached responses.
/// </summary>
internal sealed class OperationRecord
{
    public OperationKind Kind { get; init; }
    public OperationStatus Status { get; set; } = OperationStatus.Pending;

    /// <summary>
    /// Optional content-hash of the operation payload for deterministic duplicate detection
    /// when the same logical write is submitted more than once.
    /// </summary>
    public byte[]? PayloadDigest { get; init; }

    /// <summary>
    /// The response returned for this operation when it completed, stored so a retry can
    /// receive the same answer without re-executing the operation.
    /// </summary>
    public object? CachedResponse { get; set; }
}

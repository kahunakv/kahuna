
namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Tracks the completion state of a registered operation within a transaction. A cancelled operation is
/// removed from the registry outright (its id is released for re-registration), so it needs no state here.
/// </summary>
internal enum OperationStatus
{
    Pending,
    Completed
}

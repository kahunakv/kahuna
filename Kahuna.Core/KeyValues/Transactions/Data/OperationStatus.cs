
namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>Tracks the completion state of a registered operation within a transaction.</summary>
internal enum OperationStatus
{
    Pending,
    Completed,
    Cancelled
}

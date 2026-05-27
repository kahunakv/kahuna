
namespace Kahuna.Server.KeyValues.Transactions.Data;

public enum KeyValueTransactionAction
{
    Commit,
    Abort
}

public enum KeyValueExecutionStatus
{
    Continue,
    Stop
}
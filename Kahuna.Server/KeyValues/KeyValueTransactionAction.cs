
namespace Kahuna.Server.KeyValues;

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

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>Classifies the type of KV operation registered under a transaction.</summary>
public enum OperationKind
{
    Set,
    Delete,
    Extend,
    Get,
    Exists,
    Scan,
    PointLock,
    PrefixLock,
    RangeLock
}

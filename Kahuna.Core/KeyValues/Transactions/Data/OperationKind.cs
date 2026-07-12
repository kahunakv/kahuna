
namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// Classifies the type of KV operation registered under a transaction. These integer values are the
/// inter-node wire encoding (the gRPC transport blind-casts this enum onto <c>GrpcOperationKind</c>), so
/// they are stable protocol constants: keep them in lockstep with <c>GrpcOperationKind</c> in the proto,
/// assign new kinds the next free value, and never renumber an existing one.
/// </summary>
public enum OperationKind
{
    Set = 0,
    Delete = 1,
    DeleteMany = 2,
    Extend = 3,
    Get = 4,
    Exists = 5,
    Scan = 6,
    PointLock = 7,
    PrefixLock = 8,
    RangeLock = 9
}

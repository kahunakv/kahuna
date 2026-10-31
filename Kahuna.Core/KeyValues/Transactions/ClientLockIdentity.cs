using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// A key-value lock always belongs to a transaction. A lock a client asks for must therefore name the session that owns
/// it — its coordinator key and an operation id — so the coordinator registers the lock, renews it, and releases it at
/// commit or rollback. Without that identity the lock is planted under whatever non-zero timestamp the caller sends:
/// no coordinator tracks it, nothing releases it before its lease ends, and the caller can renew it by asking again.
/// Applied at the client entry points only; the inter-node path forwards locks the entry node already admitted.
/// </summary>
public static class ClientLockIdentity
{
    /// <summary>Whether a client lock request names the session that owns the lock.</summary>
    public static bool IsRegistered(string? coordinatorKey, ulong operationIdHigh, ulong operationIdLow) =>
        !string.IsNullOrEmpty(coordinatorKey) && !new TransactionOperationId(operationIdHigh, operationIdLow).IsEmpty;
}

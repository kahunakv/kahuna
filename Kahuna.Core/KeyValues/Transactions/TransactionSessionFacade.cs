using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The transaction session surface: starting, committing, rolling back and closing a session, routing those
/// calls to the coordinator node when the session lives elsewhere, and mapping a session's working set onto
/// the wire shape.
///
/// Script transactions and interactive sessions take deliberately separate admission gates — a script holds a
/// slot only for its own bounded execution, an interactive session for as long as its client stays connected —
/// so the two orderers are distinct and must stay that way.
/// </summary>
internal sealed class TransactionSessionFacade
{
    private readonly KeyValuesRuntime runtime;

    private readonly TransactionCoordinator txCoordinator;

    private readonly ScriptTransactionExecutor scriptExecutor;

    internal TransactionSessionFacade(
        KeyValuesRuntime runtime,
        TransactionCoordinator txCoordinator,
        ScriptTransactionExecutor scriptExecutor)
    {
        this.runtime = runtime;
        this.txCoordinator = txCoordinator;
        this.scriptExecutor = scriptExecutor;
    }

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private KeyValueLocator locator => runtime.Locator;

    /// Locates the appropriate key-value partition and starts a transaction.
    /// </summary>
    /// <param name="options">The options for the key-value transaction.</param>
    /// <param name="cancellationToken">The cancellation token for the operation.</param>
    /// <returns>A task representing the asynchronous operation, containing the result of the transaction initiation
    /// as a tuple consisting of the response type and the associated HLC timestamp.</returns>
    public Task<(KeyValueResponseType, TransactionHandle)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        return locator.LocateAndStartTransaction(options, cancellationToken);
    }

    /// <summary>
    /// Attempts to locate and commit the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="acquiredLocks">A list of keys that have been locked during the transaction.</param>
    /// <param name="modifiedKeys">A list of keys that were modified as part of the transaction.</param>
    /// <param name="readKeys">A list of keys read during the transaction.</param>
    /// <param name="cancellationToken">A token used to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation, containing the result of the transaction operation.</returns>
    public Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        return locator.LocateAndCommitTransaction(handle, cancellationToken);
    }


    /// <summary>Routes a working-set query to the coordinator node identified by <paramref name="coordinatorKey"/>.</summary>
    public Task<TransactionWorkingSet?> LocateAndGetTransactionWorkingSet(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        return locator.LocateAndGetTransactionWorkingSet(coordinatorKey, transactionId, cancellationToken);
    }

    /// <summary>Routes a close-and-snapshot to the coordinator node identified by <paramref name="coordinatorKey"/>.</summary>
    public Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        return locator.LocateAndCloseTransaction(coordinatorKey, transactionId, cancellationToken);
    }

    /// <summary>Node-local working-set query: the session lives here (this node leads the coordinator partition).</summary>
    public TransactionWorkingSet? GetTransactionWorkingSet(HLCTimestamp transactionId)
    {
        WorkingSetSnapshot? snapshot = txCoordinator.GetTransactionWorkingSet(transactionId);
        return snapshot is null ? null : MapWorkingSet(snapshot);
    }

    /// <summary>Node-local close-and-snapshot: freezes the session and returns its frozen working set.</summary>
    public async Task<(KeyValueResponseType, TransactionWorkingSet?)> CloseTransaction(HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        (KeyValueResponseType type, WorkingSetSnapshot? snapshot) = await txCoordinator.CloseTransaction(transactionId, cancellationToken);
        return (type, snapshot is null ? null : MapWorkingSet(snapshot));
    }

    private static TransactionWorkingSet MapWorkingSet(WorkingSetSnapshot snapshot) => new()
    {
        ModifiedKeys = ToModifiedList(snapshot.ModifiedKeys),
        AcquiredLocks = ToModifiedList(snapshot.LocksAcquired),
        AcquiredPrefixLocks = ToModifiedList(snapshot.PrefixLocksAcquired),
        AcquiredRangeLocks = ToRangeLockList(snapshot.RangeLocksAcquired),
        ReadKeys = snapshot.ReadKeys is null
            ? []
            : snapshot.ReadKeys.Values.OrderBy(r => r.Key, StringComparer.Ordinal).ToList(),
        RecordAnchorKey = snapshot.RecordAnchorKey,
        PendingOperationCount = snapshot.PendingOperationCount
    };

    private static List<KeyValueTransactionRangeLock> ToRangeLockList(IReadOnlyDictionary<RangeLockKey, RangeLockMode>? ranges)
    {
        if (ranges is null)
            return [];

        return ranges
            .OrderBy(x => x.Key.Prefix, StringComparer.Ordinal)
            .ThenBy(x => x.Key.StartKey, StringComparer.Ordinal)
            .ThenBy(x => x.Key.EndKey, StringComparer.Ordinal)
            .Select(x => new KeyValueTransactionRangeLock
            {
                Prefix = x.Key.Prefix,
                StartKey = x.Key.StartKey,
                StartInclusive = x.Key.StartInclusive,
                EndKey = x.Key.EndKey,
                EndInclusive = x.Key.EndInclusive,
                Durability = x.Key.Durability,
                Mode = x.Value
            })
            .ToList();
    }

    private static List<KeyValueTransactionModifiedKey> ToModifiedList(IReadOnlySet<(string Key, KeyValueDurability Durability)>? set)
    {
        if (set is null)
            return [];

        return set
            .OrderBy(x => x.Key, StringComparer.Ordinal)
            .Select(x => new KeyValueTransactionModifiedKey { Key = x.Key, Durability = x.Durability })
            .ToList();
    }

    /// <summary>
    /// Locates and rolls back the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="cancellationToken">A cancellation token to observe while waiting for the task to complete.</param>
    /// <returns>A task that represents the asynchronous operation, containing the result of the rollback operation as a <see cref="KeyValueResponseType"/>.</returns>
    public Task<KeyValueResponseType> LocateAndRollbackTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        return locator.LocateAndRollbackTransaction(handle, cancellationToken);
    }
    /// <summary>
    /// Schedule a key/value transaction to be executed
    /// </summary>
    /// <param name="script"></param>
    /// <param name="hash"></param>
    /// <param name="parameters"></param>
    /// <returns></returns>
    public Task<KeyValueTransactionResult> TryExecuteTx(ReadOnlyMemory<byte> script, string? hash, List<KeyValueParameter>? parameters, TransactionPriority priority = TransactionPriority.Normal)
    {
        return scriptExecutor.TryExecuteTx(script, hash, parameters, priority);
    }


    /// <summary>
    /// Starts a new transaction with the specified options.
    /// </summary>
    /// <param name="options">The options for configuring the transaction.</param>
    /// <returns>Returns an <c>HLCTimestamp</c> representing the timestamp of the started transaction.</returns>
    public Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(KeyValueTransactionOptions options)
    {
        return txCoordinator.StartTransaction(options);
    }

    /// <summary>
    /// Reads the decision-durability policy recorded for an active interactive session, or null when no
    /// active session with that id exists. Reflects exactly what Begin captured from the caller's options.
    /// </summary>
    internal DecisionDurability? GetRecordedDecisionDurability(HLCTimestamp transactionId)
    {
        return txCoordinator.GetRecordedDecisionDurability(transactionId);
    }

    /// <summary>
    /// Reads the clamped session timeout recorded for an active interactive session, or null when no active
    /// session with that id exists. Reflects the value after the MaxTransactionTimeout clamp applied in Begin.
    /// </summary>
    internal int? GetRecordedSessionTimeout(HLCTimestamp transactionId)
    {
        return txCoordinator.GetRecordedSessionTimeout(transactionId);
    }

    /// <summary>
    /// Commits the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="StartTransaction"/>.</param>
    /// <returns>A task that represents the asynchronous operation containing the commit result.</returns>
    public Task<(KeyValueResponseType, string?)> CommitTransaction(TransactionHandle handle)
    {
        return txCoordinator.CommitTransaction(handle);
    }

    /// <summary>
    /// Rolls back the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="StartTransaction"/>.</param>
    /// <returns></returns>
    public Task<KeyValueResponseType> RollbackTransaction(TransactionHandle handle)
    {
        return txCoordinator.RollbackTransaction(handle);
    }

    /// <summary>
    /// Renews the range locks of every live interactive session so they outlive their original acquire TTL
    /// without a client heartbeat. Driven periodically by the transaction reaper; exposed directly so a caller
    /// (or a test) can trigger the sweep deterministically.
    /// </summary>
    internal Task RenewSessionRangeLocks()
    {
        return txCoordinator.RenewSessionRangeLocks();
    }

    /// <summary>
    /// Reclaims interactive sessions abandoned without commit or rollback, releasing their held locks and read
    /// snapshots. Driven periodically by the transaction reaper; exposed directly so a caller (or a test) can
    /// trigger the sweep deterministically.
    /// </summary>
    internal Task ReapAbandonedSessions()
    {
        return txCoordinator.ReapAbandonedSessions();
    }
}

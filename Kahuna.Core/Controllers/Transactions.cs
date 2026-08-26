
using Kommander.Time;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna;

/// <summary>
/// Transaction surface: interactive sessions, script execution, the 2PC steps and the
/// registered-operation (coordinator) protocol.
/// </summary>
public sealed partial class KahunaManager
{
    public Task<bool> DurableOperationLocal(int partitionId, int kind, string logType, byte[] payload, CancellationToken cancellationToken) =>
        keyValues.DurableOperationLocal(partitionId, kind, logType, payload, cancellationToken);

    public Task<byte[]?> LookupTransactionRecordLocal(int partitionId, HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken) =>
        keyValues.LookupTransactionRecordLocal(partitionId, transactionId, epoch, anchorKey, cancellationToken);

    public Task<(bool Ok, List<CompletionReceiptRecord> Receipts, byte[] TransactionRecords, byte[] PreparedIntents)> GetRangeTransactionStateLocal(int partitionId, string? startKey, string? endKey, CancellationToken cancellationToken) =>
        keyValues.GetRangeTransactionStateLocal(partitionId, startKey, endKey, cancellationToken);

    /// <summary>
    /// Locates the leader node for the given key and executes the TryPrepareMutations request.
    /// </summary>
    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        long routedGeneration = 0,
        string? recordAnchorKey = null
    )
    {
        return keyValues.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancellationToken, routedGeneration, recordAnchorKey);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryPrepareManyMutations request.
    /// </summary>
    public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken,
        string? recordAnchorKey = null
    )
    {
        return keyValues.LocateAndTryPrepareManyMutations(transactionId, commitId, keys, cancellationToken, recordAnchorKey);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryCommitMutations request.
    /// </summary>
    public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(
        HLCTimestamp transactionId, 
        string key, 
        HLCTimestamp ticketId, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given keys and executes the TryCommitMutations request.
    /// </summary>
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryCommitManyMutations(transactionId, keys, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given key and executes the TryRollbackMutations request.
    /// </summary>
    public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(
        HLCTimestamp transactionId, 
        string key, 
        HLCTimestamp ticketId, 
        KeyValueDurability durability, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryRollbackMutations(transactionId, key, ticketId, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryRollbackMutations request.
    /// </summary>
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancellationToken
    )
    {
        return keyValues.LocateAndTryRollbackManyMutations(transactionId, keys, cancellationToken);
    }

    /// <summary>
    /// Starts a transaction for a key-value operation by locating the appropriate node and setting up the transaction.
    /// </summary>
    /// <param name="options">The options specifying the parameters of the transaction.</param>
    /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the response type and the timestamp of the initiated transaction.</returns>
    public Task<(KeyValueResponseType, TransactionHandle)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndStartTransaction(options, cancellationToken);
    }

    /// <summary>
    /// Commits the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous operation with the commit outcome.</returns>
    public Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndCommitTransaction(handle, cancellationToken);
    }

    /// <summary>
    /// Rolls back the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="LocateAndStartTransaction"/>.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>A <see cref="KeyValueResponseType"/> indicating the result of the rollback operation.</returns>
    public Task<KeyValueResponseType> LocateAndRollbackTransaction(TransactionHandle handle, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndRollbackTransaction(handle, cancellationToken);
    }

    public Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndBeginOperation(coordinatorKey, transactionId, operationId, kind, payloadDigest, cancellationToken).AsTask();
    }

    public Task<(KeyValueResponseType outcome, string? anchor)> LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndCompleteOperation(coordinatorKey, transactionId, operationId, payload, cancellationToken).AsTask();
    }

    public (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey) BeginOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest)
    {
        return keyValues.BeginOperation(transactionId, operationId, kind, payloadDigest);
    }

    public string? CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        return keyValues.CompleteOperation(transactionId, operationId, payload);
    }

    public Task<(KeyValueResponseType outcome, string? anchor)> CompleteOperationInbound(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        return keyValues.CompleteOperationInbound(coordinatorKey, transactionId, operationId, payload);
    }

    public Task<TransactionWorkingSet?> LocateAndGetTransactionWorkingSet(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndGetTransactionWorkingSet(coordinatorKey, transactionId, cancellationToken);
    }

    public Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        return keyValues.LocateAndCloseTransaction(coordinatorKey, transactionId, cancellationToken);
    }

    public TransactionWorkingSet? GetTransactionWorkingSet(HLCTimestamp transactionId)
    {
        return keyValues.GetTransactionWorkingSet(transactionId);
    }

    public Task<(KeyValueResponseType, TransactionWorkingSet?)> CloseTransaction(HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        return keyValues.CloseTransaction(transactionId, cancellationToken);
    }

    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        long routedGeneration = 0,
        string? recordAnchorKey = null
    )
    {
        return keyValues.TryPrepareMutations(transactionId, commitId, key, durability, routedGeneration, recordAnchorKey);
    }

    /// <summary>
    /// Attempts to commit mutations for a specified transaction and key with the given durability.
    /// </summary>
    /// <param name="transactionId">The timestamp representing the transaction ID.</param>
    /// <param name="key">The key for which the mutations are being committed.</param>
    /// <param name="proposalTicketId">The timestamp representing the ID of the proposal ticket.</param>
    /// <param name="durability">The durability level of the transaction, indicating whether it is ephemeral or persistent.</param>
    /// <returns>A task representing the asynchronous operation, containing a tuple where the first element is the result of the operation as a <see cref="KeyValueResponseType"/>, and the second element is a long value associated with the operation.</returns>
    public Task<(KeyValueResponseType, long)> TryCommitMutations(
        HLCTimestamp transactionId,
        string key,
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability
    )
    {
        return keyValues.TryCommitMutations(transactionId, key, proposalTicketId, durability);
    }

    /// <summary>
    /// Attempts to rollback mutations for a given key, transaction, and proposal ticket.
    /// </summary>
    /// <param name="transactionId">The unique identifier of the transaction to rollback.</param>
    /// <param name="key">The key associated with the mutations.</param>
    /// <param name="proposalTicketId">The identifier of the proposal ticket related to the mutation.</param>
    /// <param name="durability">The durability level for the operation.</param>
    /// <returns>A task containing a tuple with the response type and the associated transaction version.</returns>
    public Task<(KeyValueResponseType, long)> TryRollbackMutations(
        HLCTimestamp transactionId,
        string key,
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability
    )
    {
        return keyValues.TryRollbackMutations(transactionId, key, proposalTicketId, durability);
    }

    /// <summary>
    /// Attempts to execute a transaction script with the provided parameters and returns the result.
    /// </summary>
    /// <param name="script">The transaction script to execute, as read-only memory over the script bytes.</param>
    /// <param name="hash">An optional hash representing the script for validation or identification purposes.</param>
    /// <param name="parameters">An optional list of parameters to be passed into the script during execution.</param>
    /// <returns>A task that represents the asynchronous operation and resolves to the result of the transaction execution.</returns>
    public Task<KeyValueTransactionResult> TryExecuteTransactionScript(ReadOnlyMemory<byte> script, string? hash, List<KeyValueParameter>? parameters, TransactionPriority priority = TransactionPriority.Normal)
    {
        return keyValues.TryExecuteTx(script, hash, parameters, priority);
    }

    /// <summary>
    /// Starts a new interactive transaction with the specified options.
    /// </summary>
    /// <param name="options">The options to configure the transaction.</param>
    /// <returns>Returns the timestamp of the started transaction.</returns>
    public Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(KeyValueTransactionOptions options)
    {
        return keyValues.StartTransaction(options);
    }

    /// <summary>
    /// Reads the decision-durability policy recorded for an active interactive session, or null when no
    /// active session with that id exists. Reflects exactly what Begin captured from the caller's options.
    /// </summary>
    internal DecisionDurability? GetRecordedDecisionDurability(HLCTimestamp transactionId)
    {
        return keyValues.GetRecordedDecisionDurability(transactionId);
    }

    /// <summary>
    /// Reads the clamped session timeout recorded for an active interactive session, or null when no active
    /// session with that id exists. Reflects the value after the MaxTransactionTimeout clamp applied in Begin.
    /// </summary>
    internal int? GetRecordedSessionTimeout(HLCTimestamp transactionId)
    {
        return keyValues.GetRecordedSessionTimeout(transactionId);
    }

    /// <summary>
    /// Renews the range locks of every live interactive session so they outlive their original acquire TTL
    /// without a client heartbeat. Driven periodically by the transaction reaper; exposed for a deterministic
    /// trigger.
    /// </summary>
    internal Task RenewSessionRangeLocks() => keyValues.RenewSessionRangeLocks();

    /// <summary>
    /// Reclaims interactive sessions abandoned without commit or rollback, releasing their held locks. Driven
    /// periodically by the transaction reaper; exposed for a deterministic trigger.
    /// </summary>
    internal Task ReapAbandonedSessions() => keyValues.ReapAbandonedSessions();

    /// <summary>
    /// Persistent keys settled through the manual two-phase-commit ticket path on this node. Stays at zero for
    /// every all-persistent or mixed transaction, which finalize through the durable-intent path instead;
    /// non-zero would mean a crash-atomic mutation took the retired manual path. Diagnostic/test access.
    /// </summary>
    public long ManualTicketPersistentSettlementCount => keyValues.ManualTicketPersistentSettlementCount;

    /// <summary>
    /// Commits the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="StartTransaction"/>.</param>
    /// <returns>A task containing the commit outcome.</returns>
    public Task<(KeyValueResponseType, string?)> CommitTransaction(TransactionHandle handle)
    {
        return keyValues.CommitTransaction(handle);
    }

    /// <summary>
    /// Rolls back the transaction identified by <paramref name="handle"/>.
    /// </summary>
    /// <param name="handle">The handle returned by <see cref="StartTransaction"/>.</param>
    /// <returns>A task containing the rollback outcome.</returns>
    public Task<KeyValueResponseType> RollbackTransaction(TransactionHandle handle)
    {
        return keyValues.RollbackTransaction(handle);
    }

    public Task ImportCompletionReceipts(IReadOnlyCollection<CompletionReceiptRecord> receipts)
    {
        keyValues.ImportCompletionReceipts(receipts);
        return Task.CompletedTask;
    }

    public Task<bool> ImportCompletionReceiptsReplicated(int partitionId, IReadOnlyCollection<CompletionReceiptRecord> receipts) =>
        keyValues.ImportCompletionReceiptsReplicated(partitionId, receipts, CancellationToken.None);

    public Task<bool> ForgetCompletionReceiptsReplicated(int partitionId, IReadOnlyCollection<CompletionReceiptRecord> receipts) =>
        keyValues.ForgetCompletionReceiptsReplicated(partitionId, receipts, CancellationToken.None);
}

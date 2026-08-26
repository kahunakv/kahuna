using Nixie;
using Nixie.Routers;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Node-local surface of <see cref="KeyValuesManager"/>: the <c>Try*</c> operations the locator calls back
/// into once a request has reached the right node, the local scans, the two-phase-commit ticket calls, the
/// transaction session lifecycle, and the periodic node upkeep. Each member forwards to the collaborator
/// that owns it.
/// </summary>
internal sealed partial class KeyValuesManager
{

    /// <summary>

    public ValueTask<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        long routedGeneration = 0
    ) =>
        localKeyValues.TrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, routedGeneration);

    public Task<List<KahunaSetKeyValueResponseItem>> SetManyNodeKeyValue(List<KahunaSetKeyValueRequestItem> items) =>
        localKeyValues.SetManyNodeKeyValue(items);

    public Task<List<KahunaDeleteKeyValueResponseItem>> DeleteManyNodeKeyValue(List<KahunaDeleteKeyValueRequestItem> items) =>
        localKeyValues.DeleteManyNodeKeyValue(items);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(
        HLCTimestamp transactionId,
        string key, 
        int expiresMs, 
        KeyValueDurability durability
    ) =>
        localKeyValues.TryExtendKeyValue(transactionId, key, expiresMs, durability);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(
        HLCTimestamp transactionId, 
        string key, 
        KeyValueDurability durability
    ) =>
        localKeyValues.TryDeleteKeyValue(transactionId, key, durability);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability
    ) =>
        localKeyValueReads.TryGetValue(transactionId, key, revision, readTimestamp, durability);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability
    ) =>
        localKeyValueReads.TryExistsValue(transactionId, key, revision, readTimestamp, durability);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys
    ) =>
        localKeyValueReads.TryGetManyValues(transactionId, readTimestamp, keys);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryExistsManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys
    ) =>
        localKeyValueReads.TryExistsManyValues(transactionId, readTimestamp, keys);

    public Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> TryCheckManyWriteIntentValues(
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys
    ) =>
        localKeyValueReads.TryCheckManyWriteIntentValues(transactionId, keys);

    public Task<KeyValueResponseType> TryCheckWriteIntentValue(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        KeyValueConflictChecks checks = KeyValueConflictChecks.WriteIntent
    ) =>
        localKeyValueReads.TryCheckWriteIntentValue(transactionId, key, durability, checks);


    public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability) =>
        localLocks.TryAcquireExclusiveLock(transactionId, key, expiresMs, durability);

    public Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(
        HLCTimestamp transactionId, 
        string prefixKey, 
        int expiresMs, 
        KeyValueDurability durability
    ) =>
        localLocks.TryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> TryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys
    ) =>
        localLocks.TryAcquireManyExclusiveLocks(transactionId, keys);

    public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability) =>
        localLocks.TryReleaseExclusiveLock(transactionId, key, durability);

    public Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability) =>
        localLocks.TryReleaseExclusivePrefixLock(transactionId, prefixKey, durability);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability
    ) => localLocks.TryAcquireExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode
    ) =>
        localLocks.TryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode);

    public Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability
    ) =>
        localLocks.TryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability);

    internal Task RecoverPreparedIntents(CancellationToken cancellationToken) =>
        durableMaintenance.RecoverPreparedIntents(cancellationToken);

    internal Task CollectDurableTransactionRecords(CancellationToken cancellationToken) =>
        durableMaintenance.CollectDurableTransactionRecords(cancellationToken);

    internal void CollectExpiredCompletionReceipts() =>
        durableMaintenance.CollectExpiredCompletionReceipts();

    internal Task<int> TryResolveDecidedDurableBlockersAsync(
        int partitionId, IReadOnlyList<PreparedIntent> intents, HLCTimestamp transactionId, long epoch, CancellationToken cancellationToken) =>
        durableMaintenance.TryResolveDecidedDurableBlockersAsync(partitionId, intents, transactionId, epoch, cancellationToken);

    internal Task<bool> SettleMovingRangeIntentsAsync(
        int sourcePartitionId, string? startKey, string? endKey, CancellationToken cancellationToken) =>
        durableMaintenance.SettleMovingRangeIntentsAsync(sourcePartitionId, startKey, endKey, cancellationToken);

    internal Task<KeyValueResponseType> ReleaseExclusiveRangeLockOnPartitionLeaderAsync(
        int partitionId,
        HLCTimestamp transactionId,
        string keySpace,
        string? startKey, bool startInclusive,
        string? endKey, bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken) =>
        durableMaintenance.ReleaseExclusiveRangeLockOnPartitionLeaderAsync(partitionId, transactionId, keySpace, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);


    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> TryReleaseManyExclusiveLocks(
        HLCTimestamp transactionId, 
        List<(string key, KeyValueDurability durability)> keys
    ) =>
        localMutationTickets.TryReleaseManyExclusiveLocks(transactionId, keys);

    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        long routedGeneration = 0,
        string? recordAnchorKey = null
    ) =>
        localMutationTickets.TryPrepareMutations(transactionId, commitId, key, durability, routedGeneration, recordAnchorKey);

    public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> TryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys,
        string? recordAnchorKey = null
    ) =>
        localMutationTickets.TryPrepareManyMutations(transactionId, commitId, keys, recordAnchorKey);

    public Task<(KeyValueResponseType, long)> TryCommitMutations(
        HLCTimestamp transactionId, 
        string key,
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability
    ) =>
        localMutationTickets.TryCommitMutations(transactionId, key, proposalTicketId, durability);

    public Task<List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>> TryCommitManyMutations(
        HLCTimestamp transactionId,
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> keys
    ) =>
        localMutationTickets.TryCommitManyMutations(transactionId, keys);

    public Task<(KeyValueResponseType, long)> TryRollbackMutations(
        HLCTimestamp transactionId, 
        string key,
        HLCTimestamp proposalTicketId,
        KeyValueDurability durability
    ) =>
        localMutationTickets.TryRollbackMutations(transactionId, key, proposalTicketId, durability);

    public Task<List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)>> TryRollbackManyMutations(
        HLCTimestamp transactionId,
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> keys
    ) =>
        localMutationTickets.TryRollbackManyMutations(transactionId, keys);


    public Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken) =>
        localScans.ScanAllByPrefix(prefixKeyName, readTimestamp, durability, cancellationToken);

    public Task<KeyValueGetByBucketResult> ScanByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, bool includeTombstones = false) =>
        localScans.ScanByPrefix(prefixKeyName, readTimestamp, durability, includeTombstones);

    public Task<KeyValueGetByBucketResult> ScanByPrefixFromDisk(string prefixKeyName, HLCTimestamp readTimestamp, bool includeTombstones = false) =>
        localScans.ScanByPrefixFromDisk(prefixKeyName, readTimestamp, includeTombstones);

    public Task<KeyValueGetByBucketResult> GetByBucket(HLCTimestamp transactionId, string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability) =>
        localScans.GetByBucket(transactionId, prefixKeyName, readTimestamp, durability);

    public Task<KeyValueGetByRangeResult> GetByRange(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability) =>
        localScans.GetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability);



    public Task<(KeyValueResponseType, TransactionHandle)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken) =>
        transactionSessions.LocateAndStartTransaction(options, cancellationToken);

    public Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(TransactionHandle handle, CancellationToken cancellationToken) =>
        transactionSessions.LocateAndCommitTransaction(handle, cancellationToken);

    public Task<TransactionWorkingSet?> LocateAndGetTransactionWorkingSet(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken) =>
        transactionSessions.LocateAndGetTransactionWorkingSet(coordinatorKey, transactionId, cancellationToken);

    public Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken) =>
        transactionSessions.LocateAndCloseTransaction(coordinatorKey, transactionId, cancellationToken);

    public TransactionWorkingSet? GetTransactionWorkingSet(HLCTimestamp transactionId) =>
        transactionSessions.GetTransactionWorkingSet(transactionId);

    public Task<(KeyValueResponseType, TransactionWorkingSet?)> CloseTransaction(HLCTimestamp transactionId, CancellationToken cancellationToken) =>
        transactionSessions.CloseTransaction(transactionId, cancellationToken);

    public Task<KeyValueResponseType> LocateAndRollbackTransaction(TransactionHandle handle, CancellationToken cancellationToken) =>
        transactionSessions.LocateAndRollbackTransaction(handle, cancellationToken);

    public Task<KeyValueTransactionResult> TryExecuteTx(ReadOnlyMemory<byte> script, string? hash, List<KeyValueParameter>? parameters, TransactionPriority priority = TransactionPriority.Normal) =>
        transactionSessions.TryExecuteTx(script, hash, parameters, priority);

    public Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(KeyValueTransactionOptions options) =>
        transactionSessions.StartTransaction(options);

    internal DecisionDurability? GetRecordedDecisionDurability(HLCTimestamp transactionId) =>
        transactionSessions.GetRecordedDecisionDurability(transactionId);

    internal int? GetRecordedSessionTimeout(HLCTimestamp transactionId) =>
        transactionSessions.GetRecordedSessionTimeout(transactionId);

    public Task<(KeyValueResponseType, string?)> CommitTransaction(TransactionHandle handle) =>
        transactionSessions.CommitTransaction(handle);

    public Task<KeyValueResponseType> RollbackTransaction(TransactionHandle handle) =>
        transactionSessions.RollbackTransaction(handle);

    internal Task RenewSessionRangeLocks() =>
        transactionSessions.RenewSessionRangeLocks();

    internal Task ReapAbandonedSessions() =>
        transactionSessions.ReapAbandonedSessions();


    internal Task RunCollectOnAllInstancesAsync() =>
        nodeMaintenance.RunCollectOnAllInstancesAsync();

    internal Task<HLCTimestamp> GetSafeTimestampAsync() =>
        nodeMaintenance.GetSafeTimestampAsync();

    internal Task<bool> PurgeUnhostedPartitionDataAsync(int partitionId, Func<bool> stillUnhosted, CancellationToken cancellationToken) =>
        nodeMaintenance.PurgeUnhostedPartitionDataAsync(partitionId, stillUnhosted, cancellationToken);

    internal Task EvictPartitionEntriesAsync(int partitionId) =>
        nodeMaintenance.EvictPartitionEntriesAsync(partitionId);

    public Task DrainWritesAsync(TimeSpan timeout) =>
        nodeMaintenance.DrainWritesAsync(timeout);

}

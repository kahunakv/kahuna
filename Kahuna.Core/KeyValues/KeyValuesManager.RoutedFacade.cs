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
/// Routing surface of <see cref="KeyValuesManager"/>: the <c>LocateAnd*</c> entry points that send an
/// operation to the Raft leader for its key, registering it on its transaction coordinator first where the
/// request carries a register-remote identity. Each member forwards to the routed-operation class for its
/// family.
/// </summary>
internal sealed partial class KeyValuesManager
{


    
    public ValueTask<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(
        HLCTimestamp transactionId,
        string key,
        byte[]? value,
        byte[]? compareValue,
        long compareRevision,
        KeyValueFlags flags,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        long routedGeneration = 0,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedWrites.LocateAndTrySetKeyValue(transactionId, key, value, compareValue, compareRevision, flags, expiresMs, durability, cancellationToken, routedGeneration, coordinatorKey, operationId);

    public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(
        List<KahunaSetKeyValueRequestItem> setManyItems,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedWrites.LocateAndTrySetManyKeyValue(setManyItems, cancellationToken, coordinatorKey, operationId);

    public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(
        List<KahunaDeleteKeyValueRequestItem> deleteManyItems,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedWrites.LocateAndTryDeleteManyKeyValue(deleteManyItems, cancellationToken, coordinatorKey, operationId);

    internal Task<(KeyValueResponseType, long, HLCTimestamp)> SystemSetKeyValue(
        string key,
        byte[]? value,
        long compareRevision,
        KeyValueFlags flags,
        CancellationToken cancellationToken
    ) =>
        routedWrites.SystemSetKeyValue(key, value, compareRevision, flags, cancellationToken);

    internal Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> SystemGetKeyValue(string key, CancellationToken cancellationToken) =>
        routedWrites.SystemGetKeyValue(key, cancellationToken);

    internal Task<(KeyValueResponseType, long, HLCTimestamp)> SystemDeleteKeyValue(string key, CancellationToken cancellationToken) =>
        routedWrites.SystemDeleteKeyValue(key, cancellationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default) =>
        routedWrites.LocateAndTryDeleteKeyValue(transactionId, key, durability, cancellationToken, coordinatorKey, operationId);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default) =>
        routedWrites.LocateAndTryExtendKeyValue(transactionId, key, expiresMs, durability, cancellationToken, coordinatorKey, operationId);


    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedReads.LocateAndTryGetValue(transactionId, key, revision, readTimestamp, durability, cancellationToken, coordinatorKey, operationId);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedReads.LocateAndTryExistsValue(transactionId, key, revision, readTimestamp, durability, cancellationToken, coordinatorKey, operationId);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedReads.LocateAndTryExistsManyValues(transactionId, readTimestamp, keys, cancellationToken, coordinatorKey, operationId);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValuesUnconfirmed(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken
    ) =>
        routedReads.LocateAndTryExistsManyValuesUnconfirmed(transactionId, readTimestamp, keys, cancellationToken);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedReads.LocateAndTryGetManyValues(transactionId, readTimestamp, keys, cancellationToken, coordinatorKey, operationId);

    public Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> LocateAndTryCheckManyWriteIntents(
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys,
        CancellationToken cancellationToken
    ) =>
        routedReads.LocateAndTryCheckManyWriteIntents(transactionId, keys, cancellationToken);

    public Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    ) =>
        routedReads.LocateAndTryCheckWriteIntent(transactionId, key, durability, cancellationToken);




    public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveLock(
        HLCTimestamp transactionId,
        string key,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancelationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedLocks.LocateAndTryAcquireExclusiveLock(transactionId, key, expiresMs, durability, cancelationToken, coordinatorKey, operationId);

    public Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedLocks.LocateAndTryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability, cancellationToken, coordinatorKey, operationId);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys,
        CancellationToken cancelationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedLocks.LocateAndTryAcquireManyExclusiveLocks(transactionId, keys, cancelationToken, coordinatorKey, operationId);

    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancelationToken, string coordinatorKey = "", TransactionOperationId operationId = default) =>
        routedLocks.LocateAndTryReleaseExclusiveLock(transactionId, key, durability, cancelationToken, coordinatorKey, operationId);

    public Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedLocks.LocateAndTryReleaseExclusivePrefixLock(transactionId, prefixKey, durability, cancellationToken, coordinatorKey, operationId);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedLocks.LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken, coordinatorKey, operationId);

    internal Task<(KeyValueResponseType, HLCTimestamp)> RegisterAndAcquireRangeLockWithHook(
        HLCTimestamp transactionId, string coordinatorKey, TransactionOperationId operationId, string prefix,
        string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs,
        KeyValueDurability durability, RangeLockMode mode, Func<Task> afterSnapshot, CancellationToken cancellationToken) =>
        routedLocks.RegisterAndAcquireRangeLockWithHook(transactionId, coordinatorKey, operationId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, afterSnapshot, cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    ) =>
        routedLocks.LocateAndTryAcquireExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, cancellationToken);

    internal Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireExclusiveRangeLockWithHook(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        Func<Task> afterSnapshot,
        CancellationToken cancellationToken
    ) =>
        routedLocks.LocateAndTryAcquireExclusiveRangeLockWithHook(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, afterSnapshot, cancellationToken);

    public Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        string coordinatorKey = "",
        TransactionOperationId operationId = default
    ) =>
        routedLocks.LocateAndTryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken, coordinatorKey, operationId);


    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancelationToken) =>
        routedScans.LocateAndTryReleaseManyExclusiveLocks(transactionId, keys, cancelationToken);

    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancelationToken,
        long routedGeneration = 0,
        string? recordAnchorKey = null
    ) =>
        routedScans.LocateAndTryPrepareMutations(transactionId, commitId, key, durability, cancelationToken, routedGeneration, recordAnchorKey);

    public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> keys,
        CancellationToken cancelationToken,
        string? recordAnchorKey = null
    ) =>
        routedScans.LocateAndTryPrepareManyMutations(transactionId, commitId, keys, cancelationToken, recordAnchorKey);

    public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken) =>
        routedScans.LocateAndTryCommitMutations(transactionId, key, ticketId, durability, cancelationToken);

    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancelationToken) =>
        routedScans.LocateAndTryCommitManyMutations(transactionId, keys, cancelationToken);

    public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancelationToken) =>
        routedScans.LocateAndTryRollbackMutations(transactionId, key, ticketId, durability, cancelationToken);

    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(
        HLCTimestamp transactionId, 
        List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, 
        CancellationToken cancellationToken
    ) =>
        routedScans.LocateAndTryRollbackManyMutations(transactionId, keys, cancellationToken);

    public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default) =>
        routedScans.LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, cancellationToken, coordinatorKey, operationId);

    internal Task<KeyValueGetByBucketResult> LocateAndGetByBucketWithHooks(
        HLCTimestamp transactionId, string prefixedKey, KeyValueDurability durability,
        Func<int, Task>? beforeQuery, Func<int, Task>? afterDescriptor,
        CancellationToken cancellationToken) =>
        routedScans.LocateAndGetByBucketWithHooks(transactionId, prefixedKey, durability, beforeQuery, afterDescriptor, cancellationToken);

    public Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default) =>
        routedScans.LocateAndGetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken, coordinatorKey, operationId);

    public IAsyncEnumerable<(string Key, ReadOnlyKeyValueEntry Entry)> LocateAndScanRange(
        HLCTimestamp txId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int pageSize,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken ct,
        string coordinatorKey = "",
        TransactionOperationId operationId = default) =>
        routedScans.LocateAndScanRange(txId, prefix, startKey, startInclusive, endKey, endInclusive, pageSize, readTimestamp, durability, ct, coordinatorKey, operationId);

}

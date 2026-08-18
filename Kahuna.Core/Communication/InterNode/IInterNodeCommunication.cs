
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander.Time;

namespace Kahuna.Server.Communication.Internode;

/// <summary>
/// Provides methods for inter-node communication within a distributed system.
/// </summary>
public interface IInterNodeCommunication
{
    public Task<(LockResponseType, long)> TryLock(string node, string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken);
    
    public Task<(LockResponseType, long)> TryExtendLock(string node, string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken);
    
    public Task<LockResponseType> TryUnlock(string node, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken);

    public Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(string node, string resource, LockDurability durability, CancellationToken cancellationToken);

    public Task<(SequenceResponseType, long)> CreateSequence(string node, string name, long initialValue, long increment, long? maxValue, SequenceDurability durability, CancellationToken cancellationToken);

    public Task<(SequenceResponseType, ReadOnlySequenceEntry?)> GetSequence(string node, string name, SequenceDurability durability, CancellationToken cancellationToken);

    public Task<(SequenceResponseType, SequenceAllocation)> NextSequenceValue(string node, string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken);

    public Task<(SequenceResponseType, SequenceAllocation)> ReserveSequenceRange(string node, string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken);

    public Task<SequenceResponseType> DeleteSequence(string node, string name, SequenceDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(string node, HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, long routedGeneration, CancellationToken cancellationToken);
    
    public Task TrySetManyNodeKeyValue(string node, List<KahunaSetKeyValueRequestItem> items, Lock lockSync, List<KahunaSetKeyValueResponseItem> responses, CancellationToken cancellationToken);

    public Task TryDeleteManyNodeKeyValue(string node, List<KahunaDeleteKeyValueRequestItem> items, Lock lockSync, List<KahunaDeleteKeyValueResponseItem> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(string node, HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(string node, HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryGetManyNodeValues(string node, HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses, CancellationToken cancellationToken);

    public Task TryExistsManyNodeValues(string node, HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> responses, CancellationToken cancellationToken);

    public Task<KeyValueResponseType> TryCheckWriteIntentValue(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);

    /// <summary>Probes every key in <paramref name="keys"/> — all owned by <paramref name="node"/> — for concurrent
    /// write intents in a single call, returning one result per requested key.</summary>
    public Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> TryCheckManyWriteIntents(string node, HLCTimestamp transactionId, List<KeyValueConflictProbe> keys, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveLock(string node, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(string node, HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(string node, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(string node, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryAcquireNodeExclusiveLocks(string node, HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder)> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(string node, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(string node, HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(string node, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task TryReleaseNodeExclusiveLocks(string node, HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(string node, HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, long routedGeneration, CancellationToken cancellationToken, string? recordAnchorKey = null);

    public Task TryPrepareNodeMutations(string node, HLCTimestamp transactionId, HLCTimestamp commitId, List<(string key, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses, CancellationToken cancellationToken, string? recordAnchorKey = null);

    public Task<(KeyValueResponseType, long)> TryCommitMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken);

    /// <summary>Forwards a durable-intent 2PC operation to the partition leader node (kind 0 replicate delta, 1
    /// commit-apply, 2 rollback-apply) and returns whether it committed/applied.</summary>
    public Task<bool> DurableOperation(string node, int partitionId, int kind, string logType, byte[] payload, CancellationToken cancellationToken);

    /// <summary>Routes a linearizable canonical transaction-record lookup to the partition leader that owns the
    /// record's anchor key, returning the serialized record (null when absent). Used by the consult sites so a
    /// remote anchor's decision is authoritative rather than a node-local projection that would otherwise retry.</summary>
    public Task<byte[]?> LookupTransactionRecord(string node, int partitionId, HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken);

    public Task TryCommitNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken);
    
    public Task<(KeyValueResponseType, long)> TryRollbackMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task TryRollbackNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken);
    
    public Task<KeyValueGetByBucketResult> GetByBucket(string node, HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<KeyValueGetByRangeResult> GetByRange(string node, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<KeyValueGetByBucketResult> ScanByPrefix(string node, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, bool includeTombstones, CancellationToken cancellationToken);
    
    public Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(string node, KeyValueTransactionOptions options, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, string?)> CommitTransaction(string node, TransactionHandle handle, CancellationToken cancellationToken);

    public Task<KeyValueResponseType> RollbackTransaction(string node, TransactionHandle handle, CancellationToken cancellationToken);

    public Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> BeginOperation(string node, string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType outcome, string? anchor)> CompleteOperation(string node, string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken);

    public Task<TransactionWorkingSet?> GetTransactionWorkingSet(string node, string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, TransactionWorkingSet?)> CloseTransaction(string node, string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken);

    public Task<bool> EnsureKeyRangeSeeded(string node, string keySpace, CancellationToken cancellationToken);

    public Task<bool> EnsureKeyRangeRemoved(string node, string keySpace, CancellationToken cancellationToken);

    /// <summary>Reads live range locks for <paramref name="keySpace"/> from the actor pool on <paramref name="node"/>.</summary>
    public Task<List<KeyValueRangeLock>> GetRangeLocks(string node, string keySpace, CancellationToken cancellationToken);

    /// <summary>Injects clamped lock entries into the actor pool for <paramref name="keySpace"/> on <paramref name="node"/>.</summary>
    public Task ImportRangeLocks(string node, string keySpace, List<KeyValueRangeLock> locks, CancellationToken cancellationToken);

    /// <summary>
    /// Forwards a receipt batch to <paramref name="node"/> (the partition leader) so it replicates the record —
    /// or, when <paramref name="forget"/> is set, the forget — onto <paramref name="partitionId"/>'s Raft log.
    /// Returns whether the operation was durable on that partition.
    /// </summary>
    public Task<bool> ImportCompletionReceipts(string node, int partitionId, IReadOnlyCollection<CompletionReceiptRecord> receipts, CancellationToken cancellationToken, bool forget = false);

    /// <summary>
    /// Forwards one checksummed page of key-values moved by a range split/merge to <paramref name="node"/>
    /// (the destination partition's leader) so it replicates the page's entries onto
    /// <paramref name="partitionId"/>'s Raft log. Returns whether the page committed.
    /// </summary>
    public Task<bool> ReplicateKeyValueRangePage(string node, int partitionId, byte[] page, CancellationToken cancellationToken);

    /// <summary>
    /// Reads the transaction state a moving key range carries — completion receipts plus serialized
    /// canonical transaction records and prepared intents in <c>[startKey, endKey)</c> — from
    /// <paramref name="node"/> (the source partition's leader). Ok is false when the remote node could
    /// not confirm leadership of <paramref name="partitionId"/> and refused to answer from possibly
    /// lagging stores.
    /// </summary>
    public Task<(bool Ok, List<CompletionReceiptRecord> Receipts, byte[] TransactionRecords, byte[] PreparedIntents)> GetRangeTransactionState(string node, int partitionId, string? startKey, string? endKey, CancellationToken cancellationToken);


    /// <summary>Forwards a snapshot-hold acquire to the meta-partition leader on <paramref name="node"/>.</summary>
    public Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)> AcquireSnapshotHold(string node, string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken cancellationToken);

    /// <summary>Forwards a snapshot-hold renew to the meta-partition leader on <paramref name="node"/>.</summary>
    public Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)> RenewSnapshotHold(string node, string holdId, int leaseMs, CancellationToken cancellationToken);

    /// <summary>Forwards a snapshot-hold release to the meta-partition leader on <paramref name="node"/>.</summary>
    public Task<KeyValueResponseType> ReleaseSnapshotHold(string node, string holdId, CancellationToken cancellationToken);

    /// <summary>
    /// Reads the authoritative snapshot floor from the meta-partition leader on <paramref name="node"/>.
    /// The floor and count are meaningful only when Type is <see cref="KeyValueResponseType.Get"/>;
    /// <see cref="KeyValueResponseType.MustRetry"/> means the remote node could not confirm
    /// leadership and refused to answer from possibly-stale local state.
    /// </summary>
    public Task<(KeyValueResponseType Type, HLCTimestamp Floor, int LiveHolds)> GetSnapshotFloor(string node, CancellationToken cancellationToken);
}


using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Locks;
using Kahuna.Server.Locks.Data;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;

namespace Kahuna;

public interface IKahuna
{
    public Task<(LockResponseType, long)> LocateAndTryLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken);

    public Task<(LockResponseType, long)> LocateAndTryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability, CancellationToken cancellationToken);

    public Task<LockResponseType> LocateAndTryUnlock(string resource, byte[] owner, LockDurability durability,CancellationToken cancellationToken);

    public Task<(LockResponseType, ReadOnlyLockEntry?)> LocateAndGetLock(string resource, LockDurability durability, CancellationToken cancellationToken);
    
    public Task<(LockResponseType, long)> TryLock(string resource, byte[] owner, int expiresMs, LockDurability durability);

    public Task<(LockResponseType, long)> TryExtendLock(string resource, byte[] owner, int expiresMs, LockDurability durability);

    public Task<LockResponseType> TryUnlock(string resource, byte[] owner, LockDurability durability);
    
    public Task<(LockResponseType, ReadOnlyLockEntry?)> GetLock(string resource, LockDurability durability);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, long routedGeneration = 0, string coordinatorKey = "", TransactionOperationId operationId = default);
    
    public Task<List<KahunaSetKeyValueResponseItem>> LocateAndTrySetManyKeyValue(List<KahunaSetKeyValueRequestItem> setManyItems, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<List<KahunaDeleteKeyValueResponseItem>> LocateAndTryDeleteManyKeyValue(List<KahunaDeleteKeyValueRequestItem> deleteManyItems, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryExistsValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> LocateAndTryGetValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryGetManyValues(HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> LocateAndTryExistsManyValues(HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);
    
    public Task<(KeyValueResponseType, long, HLCTimestamp)> LocateAndTryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<KeyValueGetByRangeResult> LocateAndGetByRange(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    /// <summary>
    /// Streams all key-value entries whose keys start with <paramref name="prefix"/> as an
    /// <see cref="IAsyncEnumerable{T}"/>, fetching results in cursor-paged batches of
    /// <paramref name="pageSize"/> items. When <paramref name="readTimestamp"/> is non-Zero the whole
    /// scan is pinned to that snapshot; when Zero the snapshot is captured on the first page. Either
    /// way it is held fixed across all subsequent pages for a consistent read.
    /// </summary>
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
        TransactionOperationId operationId = default);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TrySetKeyValue(HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, long compareRevision, KeyValueFlags flags, int expiresMs, KeyValueDurability durability, long routedGeneration = 0);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryExtendKeyValue(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability);

    public Task<(KeyValueResponseType, long, HLCTimestamp)> TryDeleteKeyValue(HLCTimestamp transactionId, string key, KeyValueDurability durability);

    public Task<List<KahunaDeleteKeyValueResponseItem>> DeleteManyNodeKeyValue(List<KahunaDeleteKeyValueRequestItem> items);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryGetValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryGetManyValues(HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys);

    public Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> TryExistsValue(HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)>> TryExistsManyValues(HLCTimestamp transactionId, HLCTimestamp readTimestamp, List<(string key, long revision, KeyValueDurability durability)> keys);

    /// <summary>Runs a durable-intent 2PC operation forwarded from a coordinator because this node is the partition
    /// leader: kind 0 replicates a delta through the local scheduler, kind 1/2 applies a committed/aborted intent's
    /// resolution on the local KV state. Returns whether it committed/applied.</summary>
    public Task<bool> DurableOperationLocal(int partitionId, int kind, string logType, byte[] payload, CancellationToken cancellationToken);

    public Task<KeyValueResponseType> TryCheckWriteIntentValue(HLCTimestamp transactionId, string key, KeyValueDurability durability);

    public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)>> LocateAndTryAcquireManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, int expiresMs, KeyValueDurability durability)> keys, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);
    
    public Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);
    
    public Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);

    public Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default);
    
    public Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(HLCTimestamp transactionId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancellationToken);
    
    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> LocateAndTryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, CancellationToken cancellationToken, long routedGeneration = 0, string? recordAnchorKey = null, CoordinatorDecisionRecord? embeddedDecision = null);

    public Task<List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)>> LocateAndTryPrepareManyMutations(HLCTimestamp transactionId, HLCTimestamp commitId, List<(string key, KeyValueDurability durability)> keys, CancellationToken cancellationToken, string? recordAnchorKey = null);
    
    public Task<(KeyValueResponseType, long)> LocateAndTryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryCommitManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancellationToken);
    
    public Task<(KeyValueResponseType, long)> LocateAndTryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken);
    
    public Task<List<(KeyValueResponseType, string, long, KeyValueDurability)>> LocateAndTryRollbackManyMutations(HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> keys, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, TransactionHandle)> LocateAndStartTransaction(KeyValueTransactionOptions options, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, string?)> LocateAndCommitTransaction(TransactionHandle handle, CancellationToken cancellationToken);

    public Task<KeyValueResponseType> LocateAndRollbackTransaction(TransactionHandle handle, CancellationToken cancellationToken);

    /// <summary>
    /// Registers a transaction-scoped operation on the coordinator session (routed by
    /// <paramref name="coordinatorKey"/>) before the operation mutates a participant. Returns whether
    /// the operation is new, already in flight, already completed (with the cached write response to
    /// replay), or rejected. This is the participant-side idempotency guard: a retry carrying the same
    /// <paramref name="operationId"/> after a lost response replays the cached answer instead of
    /// applying the mutation twice.
    /// </summary>
    public Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey)> LocateAndBeginOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken);

    /// <summary>
    /// Records a transaction-scoped operation's confirmed effect (modified key, point lock, and/or read
    /// observation) into the coordinator-owned working set and caches its write response, keyed by
    /// <paramref name="operationId"/>. Routed by <paramref name="coordinatorKey"/> to the coordinator node.
    /// Returns the transaction's record anchor after the effect is folded in, or null if none yet.
    /// </summary>
    public Task<(KeyValueResponseType outcome, string? anchor)> LocateAndCompleteOperation(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken);

    /// <summary>Node-local operation registration on the coordinator that owns the session (called after routing).</summary>
    public (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey) BeginOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest);

    /// <summary>Node-local operation completion on the coordinator that owns the session (called after routing). Returns the record anchor after the effect folds in, or null if none.</summary>
    public string? CompleteOperation(HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload);

    /// <summary>
    /// Inbound landing point used by both transports for a remote completion. Re-checks local leadership
    /// before folding; returns <c>MustRetry</c> if this node is no longer the coordinator-partition leader.
    /// Does not re-forward — callers retry through a fresh route.
    /// </summary>
    public Task<(KeyValueResponseType outcome, string? anchor)> CompleteOperationInbound(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload);

    /// <summary>
    /// Returns a snapshot of the coordinator-owned working set (modified keys, held locks, read
    /// observations, pending count) for the transaction, routed by <paramref name="coordinatorKey"/>.
    /// Null when no session exists. This is a live query and does not close the session.
    /// </summary>
    public Task<TransactionWorkingSet?> LocateAndGetTransactionWorkingSet(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken);

    /// <summary>
    /// Closes the transaction to new operations, waits for in-flight ones to drain, and returns the
    /// frozen working set idempotently. A drain-deadline timeout returns <c>MustRetry</c> with no set.
    /// Routed by <paramref name="coordinatorKey"/>.
    /// </summary>
    public Task<(KeyValueResponseType, TransactionWorkingSet?)> LocateAndCloseTransaction(string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken);

    /// <summary>Node-local working-set query on the coordinator that owns the session (called after routing).</summary>
    public TransactionWorkingSet? GetTransactionWorkingSet(HLCTimestamp transactionId);

    /// <summary>Node-local close-and-snapshot on the coordinator that owns the session (called after routing).</summary>
    public Task<(KeyValueResponseType, TransactionWorkingSet?)> CloseTransaction(HLCTimestamp transactionId, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability);

    public Task<KeyValueResponseType> TryAcquireExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> TryAcquireExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability);

    /// <summary>Returns live range locks for <paramref name="keySpace"/> from the local actor (export).</summary>
    public Task<List<KeyValueRangeLock>> GetRangeLocks(string keySpace);

    /// <summary>Injects clamped lock entries into the local actor for <paramref name="keySpace"/> (import).</summary>
    public Task ImportRangeLocks(string keySpace, List<KeyValueRangeLock> locks);

    /// <summary>Records transferred completion receipts into this node's local receipt store (state-transfer seeding).</summary>
    public Task ImportCompletionReceipts(IReadOnlyCollection<CompletionReceiptRecord> receipts);

    /// <summary>Merges transferred coordinator decision records into this node's local store (state-transfer seeding).</summary>
    public Task ImportCoordinatorDecisions(IReadOnlyCollection<CoordinatorDecisionRecord> records);

    /// <summary>
    /// Replicates a split/merge receipt handoff onto <paramref name="partitionId"/>'s Raft log on this node
    /// (the destination leader), returning whether it was durable. The caller gates cutover on the result.
    /// </summary>
    public Task<bool> ImportCompletionReceiptsReplicated(int partitionId, IReadOnlyCollection<CompletionReceiptRecord> receipts);

    /// <summary>
    /// Replicates a split/merge decision handoff onto <paramref name="partitionId"/>'s Raft log on this node
    /// (the destination leader), returning whether it was durable. The caller gates cutover on the result.
    /// </summary>
    public Task<bool> ImportCoordinatorDecisionsReplicated(int partitionId, IReadOnlyCollection<CoordinatorDecisionRecord> records);

    /// <summary>
    /// Replicates a completion-receipt forget onto <paramref name="partitionId"/>'s Raft log on this node (the
    /// participant partition leader), so every replica drops the proof. Returns whether it was durable; the
    /// coordinator persists <c>ReceiptReleased</c> only on a durable forget.
    /// </summary>
    public Task<bool> ForgetCompletionReceiptsReplicated(int partitionId, IReadOnlyCollection<CompletionReceiptRecord> receipts);

    public Task<(KeyValueResponseType, string)> TryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability);
    
    public Task<KeyValueResponseType> TryReleaseExclusivePrefixLock(HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability);

    public Task<KeyValueResponseType> TryReleaseExclusiveRangeLock(HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability);

    public Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(HLCTimestamp transactionId, HLCTimestamp commitId, string key, KeyValueDurability durability, long routedGeneration = 0, string? recordAnchorKey = null, CoordinatorDecisionRecord? embeddedDecision = null);

    public Task<(KeyValueResponseType, long)> TryCommitMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability);
    
    public Task<(KeyValueResponseType, long)> TryRollbackMutations(HLCTimestamp transactionId, string key, HLCTimestamp proposalTicketId, KeyValueDurability durability);
    
    public Task<KeyValueTransactionResult> TryExecuteTransactionScript(ReadOnlyMemory<byte> script, string? hash, List<KeyValueParameter>? parameters);
    
    public Task<KeyValueGetByBucketResult> GetByBucket(HLCTimestamp transactionId, string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability);

    public Task<KeyValueGetByBucketResult> ScanByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability);

    public Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken);

    public Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(KeyValueTransactionOptions options);

    public Task<(KeyValueResponseType, string?)> CommitTransaction(TransactionHandle handle);

    public Task<KeyValueResponseType> RollbackTransaction(TransactionHandle handle);

    public Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(string name, SequenceDurability durability, CancellationToken cancellationToken);

    public Task<(SequenceResponseType, long)> LocateAndCreateSequence(string name, long initialValue, long increment, long? maxValue, SequenceDurability durability, CancellationToken cancellationToken);

    public Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken);

    public Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken);

    public Task<SequenceResponseType> LocateAndDeleteSequence(string name, SequenceDurability durability, CancellationToken cancellationToken);
    
    public Task<bool> OnLogRestored(int partitionId, RaftLog log);

    public Task<bool> OnReplicationReceived(int partitionId, RaftLog log);

    public void OnReplicationError(int partitionId, RaftLog log);

    public Task<bool> OnLeaderChanged(int partitionId, string node);

    public Task FlushPersistenceAsync();

    /// <summary>
    /// Seeds this node's persistence backend and Raft WAL from a PITR backup chain so that when
    /// the node joins an existing cluster with <c>--join-existing</c>, the leader can catch it
    /// up via a small AppendEntries delta rather than a full <c>InstallSnapshot</c>.
    /// <para>
    /// <paramref name="backupDir"/> is both the catalog root (where <c>.manifest</c> files live)
    /// and the artifacts root (where per-backup subdirectories with checkpoint/WAL files live).
    /// <paramref name="leafBackupId"/> is the most-recent backup in the chain to restore from.
    /// When <paramref name="targetTime"/> is <see cref="HLCTimestamp.Zero"/> the restore uses
    /// the chain's natural maximum HLC (equivalent to "restore everything").
    /// </para>
    /// <para>Throws <c>BackupDriverException</c> when the chain is invalid, the backup ID is not
    /// found, or the target time falls outside the PITR retention window.</para>
    /// </summary>
    public Task BootstrapFromPitrBackupAsync(
        string backupDir,
        Guid leafBackupId,
        HLCTimestamp targetTime,
        IWAL walAdapter,
        TimeSpan pitrWindow,
        TimeSpan baseSnapshotInterval);

    /// <summary>
    /// Marks <paramref name="keySpace"/> as key-range routed on this node. Must be called on every
    /// node at startup before accepting writes for the space (registry is node-local in-memory state;
    /// it is not replicated). Idempotent.
    /// <para>
    /// This only flips the routing <i>mode</i>; it does not create the initial range descriptor, so a
    /// subsequent write throws until one exists. Prefer <see cref="RegisterKeyRangeAsync"/>, which also
    /// auto-seeds the initial whole-space descriptor on the meta-partition leader.
    /// </para>
    /// </summary>
    public void RegisterKeyRange(string keySpace);

    /// <summary>
    /// Marks <paramref name="keySpace"/> as key-range routed on this node <b>and</b> auto-seeds its
    /// initial whole-space descriptor (<c>[-inf, +inf)</c>) on the meta-partition leader if none exists.
    /// Call on every node at startup; the mode flip is node-local while the seed is a single replicated
    /// meta write that only the meta-partition leader commits (a no-op elsewhere — the descriptor arrives
    /// by replication). Idempotent. The seed needs no key-distribution or PK-type knowledge: it is the
    /// trivial whole-space range, and the auto-splitter discovers real boundaries later from live data.
    /// </summary>
    /// <returns><c>true</c> iff this call committed the seed descriptor.</returns>
    public Task<bool> RegisterKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes all descriptors for <paramref name="keySpace"/> from the replicated range map and
    /// clears its routing mode on every node via the normal replication path. Inverse of
    /// <see cref="RegisterKeyRangeAsync"/>. Idempotent: if no descriptors exist the map is
    /// unchanged, but the call still succeeds.
    /// </summary>
    /// <returns>
    /// The return value has three distinct meanings:
    /// <list type="bullet">
    ///   <item><description>
    ///     <b><c>true</c> — committed (including idempotent no-op).</b>
    ///     Either the descriptors were removed and replicated, or none existed and the
    ///     (unchanged) map was re-replicated by <c>MutateAsync</c>. Either way the space is
    ///     absent from the range map after this call.
    ///   </description></item>
    ///   <item><description>
    ///     <b><c>false</c> — transient; retry after a short delay.</b>
    ///     A split quiesce window is open. The window is short (milliseconds); the caller
    ///     should wait and retry.
    ///   </description></item>
    ///   <item><description>
    ///     <b><c>false</c> — permanent; do not retry.</b>
    ///     Either <c>InitialPartitions &lt; 1</c> (key-range sharding is disabled for this
    ///     cluster — the space was never registered) or <paramref name="keySpace"/> ends with
    ///     <c>/meta</c> (schema-log spaces are never key-range-routed). In both cases the space
    ///     was never registered, so there is nothing to remove.
    ///   </description></item>
    /// </list>
    /// Callers that need to distinguish "retry" from "permanent no-op" can inspect cluster
    /// configuration (<c>InitialPartitions</c>) or key-space name before calling.
    /// </returns>
    public Task<bool> RemoveKeyRangeAsync(string keySpace, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks every KeyRange descriptor and splits any that exceed the configured size threshold.
    /// Returns the number of splits performed. Only executes on the node holding leadership of
    /// both the system partition (0) and meta partition (1); returns 0 on other nodes.
    /// </summary>
    public Task<int> TriggerAutoSplitAsync(CancellationToken ct = default);

    /// <summary>
    /// Scans all KeyRange spaces for adjacent under-min descriptor pairs and merges them.
    /// Only executes on the node that simultaneously holds leadership of both the system partition (0)
    /// and meta partition (1); returns 0 on other nodes.
    /// </summary>
    public Task<int> TriggerAutoMergeAsync(CancellationToken ct = default);

    // ── Backup / PITR ──────────────────────────────────────────────────────────────────────

    /// <summary>Returns true when a backup directory is configured on this node.</summary>
    public bool IsBackupConfigured { get; }

    /// <summary>Takes a full backup and returns its manifest summary.</summary>
    public Task<KahunaBackupInfo> TakeFullBackupAsync(CancellationToken ct = default);

    /// <summary>Takes an incremental backup on top of the given parent and returns the manifest.</summary>
    public Task<KahunaBackupInfo> TakeIncrementalBackupAsync(Guid parentBackupId, CancellationToken ct = default);

    /// <summary>
    /// Computes the cluster-wide safe snapshot timestamp and takes a coordinated full backup
    /// capped at that T.  All partitions present a state as of the same HLC.
    /// </summary>
    public Task<KahunaBackupInfo> TakeCoordinatedBackupAsync(CancellationToken ct = default);

    /// <summary>Lists all backup manifests in the local catalog.</summary>
    public Task<IReadOnlyList<KahunaBackupInfo>> ListBackupsAsync(CancellationToken ct = default);

    /// <summary>
    /// Resolves and validates the backup chain ending at <paramref name="leafBackupId"/>.
    /// Returns the chain in chronological order (Full first, leaf last).
    /// </summary>
    public Task<IReadOnlyList<KahunaBackupInfo>> GetBackupChainAsync(Guid leafBackupId, CancellationToken ct = default);

    /// <summary>
    /// Offline restore: copies the Full backup's checkpoint to <paramref name="targetDir"/> and
    /// replays incremental WAL segments up to <paramref name="targetTimeMs"/> (ms since Unix epoch;
    /// 0 = chain max). The operator can then start a fresh node with <c>--storage-path=targetDir</c>.
    /// </summary>
    public Task<KahunaRestoreResponse> RestoreToAsync(
        Guid leafBackupId,
        string targetDir,
        long targetTimeMs,
        CancellationToken ct = default);

    // ── MVCC snapshot floor ─────────────────────────────────────────────────────────────────

    /// <summary>
    /// Acquires or renews a refcounted hold protecting all revisions at/after
    /// <paramref name="timestamp"/>. Idempotent by (holderId, timestamp): a repeat returns the same
    /// holdId and renews the lease. While the hold is live, Kahuna keeps the revision current at
    /// <paramref name="timestamp"/> readable via every read path that honors <c>readTimestamp</c>.
    /// </summary>
    public Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)>
        LocateAndAcquireSnapshotHold(string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken ct);

    /// <summary>
    /// Renews an existing hold's lease. Returns a non-Set type when the hold has already
    /// expired or was never registered.
    /// </summary>
    public Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)>
        LocateAndRenewSnapshotHold(string holdId, int leaseMs, CancellationToken ct);

    /// <summary>
    /// Releases a hold. The effective floor rises when the lowest hold is released.
    /// </summary>
    public Task<KeyValueResponseType>
        LocateAndReleaseSnapshotHold(string holdId, CancellationToken ct);

    /// <summary>
    /// Returns the current effective floor (minimum live held timestamp, or
    /// <see cref="HLCTimestamp.Zero"/> when no hold is live) and the count of live holds.
    /// </summary>
    public Task<(HLCTimestamp EffectiveFloor, int LiveHolds)>
        GetSnapshotFloor(CancellationToken ct);
}

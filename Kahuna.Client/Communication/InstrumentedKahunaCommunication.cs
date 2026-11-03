/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Client.Diagnostics;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kommander.Time;

namespace Kahuna.Client.Communication;

/// <summary>
/// Wraps a transport so that every call to a node is one client span and one duration sample.
/// <para>
/// The gRPC transport sends most operations as frames on one shared duplex stream, so the HTTP and
/// gRPC client instrumentation sees one long-lived call and not the operations in it. This layer is the
/// first place where one operation is one call.
/// </para>
/// <para>
/// Each method calls the inner transport exactly once and adds no retry, no buffering and no change to
/// arguments or results. With telemetry disabled, the inner task is returned as it is.
/// </para>
/// <para>
/// <see cref="ScanByRange"/> is a pass-through: a scan is consumed page by page at the caller's pace, so
/// a span over it would measure the caller and not the node.
/// </para>
/// </summary>
internal sealed class InstrumentedKahunaCommunication : IKahunaCommunication
{
    private readonly IKahunaCommunication inner;

    public InstrumentedKahunaCommunication(IKahunaCommunication inner)
    {
        this.inner = inner;
    }

    internal IKahunaCommunication Inner => inner;

    public Task<(KahunaLockAcquireResult, long, string?)> TryAcquireLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("AcquireLock", url);
        try { return op.Observe(inner.TryAcquireLock(url, resource, owner, expiryTime, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<bool> TryUnlock(string url, string resource, byte[] owner, LockDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("Unlock", url);
        try { return op.Observe(inner.TryUnlock(url, resource, owner, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(bool, long)> TryExtendLock(string url, string resource, byte[] owner, int expiryTime, LockDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("ExtendLock", url);
        try { return op.Observe(inner.TryExtendLock(url, resource, owner, expiryTime, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaLockInfo?> GetLock(string url, string resource, LockDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("GetLock", url);
        try { return op.Observe(inner.GetLock(url, resource, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(bool, long, int)> TrySetKeyValue(string url, HLCTimestamp transactionId, string key, byte[]? value, int expiryTime, KeyValueFlags flags, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("SetKeyValue", url);
        try { return op.Observe(inner.TrySetKeyValue(url, transactionId, key, value, expiryTime, flags, durability, cancellationToken, coordinatorKey, operationId)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(List<KahunaSetKeyValueResponseItem>, int)> TrySetManyKeyValues(string url, IEnumerable<KahunaSetKeyValueRequestItem> requestItems, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("SetManyKeyValues", url);
        try { return op.Observe(inner.TrySetManyKeyValues(url, requestItems, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(List<KahunaDeleteKeyValueResponseItem>, int)> TryDeleteManyKeyValues(string url, IEnumerable<KahunaDeleteKeyValueRequestItem> requestItems, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("DeleteManyKeyValues", url);
        try { return op.Observe(inner.TryDeleteManyKeyValues(url, requestItems, cancellationToken, coordinatorKey, operationId)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryGetManyKeyValues(string url, HLCTimestamp transactionId, IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("GetManyKeyValues", url);
        try { return op.Observe(inner.TryGetManyKeyValues(url, transactionId, requestItems, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(List<KahunaGetManyKeyValuesResponseItem>, int)> TryExistsManyKeyValues(string url, HLCTimestamp transactionId, IEnumerable<KahunaGetManyKeyValuesRequestItem> requestItems, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("ExistsManyKeyValues", url);
        try { return op.Observe(inner.TryExistsManyKeyValues(url, transactionId, requestItems, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(bool, long, int)> TryCompareValueAndSetKeyValue(string url, HLCTimestamp transactionId, string key, byte[]? value, byte[]? compareValue, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("CompareValueAndSetKeyValue", url);
        try { return op.Observe(inner.TryCompareValueAndSetKeyValue(url, transactionId, key, value, compareValue, expiryTime, durability, cancellationToken, coordinatorKey, operationId)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(bool, long, int)> TryCompareRevisionAndSetKeyValue(string url, HLCTimestamp transactionId, string key, byte[]? value, long compareRevision, int expiryTime, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("CompareRevisionAndSetKeyValue", url);
        try { return op.Observe(inner.TryCompareRevisionAndSetKeyValue(url, transactionId, key, value, compareRevision, expiryTime, durability, cancellationToken, coordinatorKey, operationId)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(bool, byte[]?, long, HLCTimestamp, int)> TryGetKeyValue(string url, HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default, KeyValueReadMode readMode = KeyValueReadMode.Default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("GetKeyValue", url);
        try { return op.Observe(inner.TryGetKeyValue(url, transactionId, key, revision, readTimestamp, durability, cancellationToken, coordinatorKey, operationId, readMode)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(bool, long, int)> TryExistsKeyValue(string url, HLCTimestamp transactionId, string key, long revision, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("ExistsKeyValue", url);
        try { return op.Observe(inner.TryExistsKeyValue(url, transactionId, key, revision, readTimestamp, durability, cancellationToken, coordinatorKey, operationId)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(bool, long, int)> TryDeleteKeyValue(string url, HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("DeleteKeyValue", url);
        try { return op.Observe(inner.TryDeleteKeyValue(url, transactionId, key, durability, cancellationToken, coordinatorKey, operationId)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(bool, long, int)> TryExtendKeyValue(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("ExtendKeyValue", url);
        try { return op.Observe(inner.TryExtendKeyValue(url, transactionId, key, expiresMs, durability, cancellationToken, coordinatorKey, operationId)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaKeyValueTransactionResult> TryExecuteKeyValueTransactionScript(string url, byte[] script, string? hash, List<KeyValueParameter>? parameters, CancellationToken cancellationToken, TransactionPriority priority = TransactionPriority.Normal)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("ExecuteKeyValueTransactionScript", url);
        try { return op.Observe(inner.TryExecuteKeyValueTransactionScript(url, script, hash, parameters, cancellationToken, priority)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<bool> TryAcquireExclusiveKeyValueLock(string url, HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("AcquireExclusiveKeyValueLock", url);
        try { return op.Observe(inner.TryAcquireExclusiveKeyValueLock(url, transactionId, key, expiresMs, durability, cancellationToken, coordinatorKey, operationId)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<bool> TryAcquireExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, int expiresMs, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("AcquireExclusivePrefixKeyValueLock", url);
        try { return op.Observe(inner.TryAcquireExclusivePrefixKeyValueLock(url, transactionId, prefixKey, expiresMs, durability, cancellationToken, coordinatorKey, operationId)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task TryReleaseExclusivePrefixKeyValueLock(string url, HLCTimestamp transactionId, string prefixKey, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("ReleaseExclusivePrefixKeyValueLock", url);
        try { return op.Observe(inner.TryReleaseExclusivePrefixKeyValueLock(url, transactionId, prefixKey, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<bool> TryAcquireRangeKeyValueLock(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int expiresMs, KeyValueDurability durability, RangeLockMode mode, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("AcquireRangeKeyValueLock", url);
        try { return op.Observe(inner.TryAcquireRangeKeyValueLock(url, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken, coordinatorKey, operationId)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task TryReleaseExclusiveRangeKeyValueLock(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("ReleaseExclusiveRangeKeyValueLock", url);
        try { return op.Observe(inner.TryReleaseExclusiveRangeKeyValueLock(url, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KeyValueGetByRangePageResult> GetByRange(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int limit, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default, KeyValueReadMode readMode = KeyValueReadMode.Default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("GetByRange", url);
        try { return op.Observe(inner.GetByRange(url, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken, coordinatorKey, operationId, readMode)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    // The span covers opening the stream and receiving its first answer, not the life of the watch.
    public Task<KahunaWatchOpening> OpenWatch(string url, KeyValueWatchRequest request, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("OpenWatch", url);
        try { return op.Observe(inner.OpenWatch(url, request, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public IAsyncEnumerable<KeyValueGetByBucketItem> ScanByRange(string url, HLCTimestamp transactionId, string prefix, string? startKey, bool startInclusive, string? endKey, bool endInclusive, int pageSize, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, KeyValueReadMode readMode = KeyValueReadMode.Default) =>
        inner.ScanByRange(url, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, pageSize, readTimestamp, durability, cancellationToken, readMode);

    public Task<List<KeyValueGetByBucketItem>> GetByBucket(string url, HLCTimestamp transactionId, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken, string coordinatorKey = "", TransactionOperationId operationId = default)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("GetByBucket", url);
        try { return op.Observe(inner.GetByBucket(url, transactionId, prefixKey, readTimestamp, durability, cancellationToken, coordinatorKey, operationId)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<List<KeyValueGetByBucketItem>> ScanAllByPrefix(string url, string prefixKey, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("ScanAllByPrefix", url);
        try { return op.Observe(inner.ScanAllByPrefix(url, prefixKey, readTimestamp, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(string, HLCTimestamp transactionId)> StartTransactionSession(string url, string uniqueId, KahunaTransactionOptions txOptions, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("StartTransactionSession", url);
        try { return op.Observe(inner.StartTransactionSession(url, uniqueId, txOptions, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(bool committed, string? recordAnchorKey, HLCTimestamp commitTimestamp)> CommitTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, string? recordAnchorKey, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("CommitTransactionSession", url);
        try { return op.Observe(inner.CommitTransactionSession(url, uniqueId, transactionId, recordAnchorKey, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<bool> RollbackTransactionSession(string url, string uniqueId, HLCTimestamp transactionId, string? recordAnchorKey, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("RollbackTransactionSession", url);
        try { return op.Observe(inner.RollbackTransactionSession(url, uniqueId, transactionId, recordAnchorKey, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(SequenceResponseType, ReadOnlySequenceEntry?, int)> GetSequence(string url, string name, SequenceDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("GetSequence", url);
        try { return op.Observe(inner.GetSequence(url, name, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(SequenceResponseType, long, int)> CreateSequence(string url, string name, long initialValue, long increment, long? maxValue, int? blockSize, SequenceDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("CreateSequence", url);
        try { return op.Observe(inner.CreateSequence(url, name, initialValue, increment, maxValue, blockSize, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(SequenceResponseType, long, int)> UpdateSequence(string url, string name, SequenceUpdate update, SequenceDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("UpdateSequence", url);
        try { return op.Observe(inner.UpdateSequence(url, name, update, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(SequenceResponseType, SequenceAllocation, int)> NextSequenceValue(string url, string name, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("NextSequenceValue", url);
        try { return op.Observe(inner.NextSequenceValue(url, name, idempotencyKey, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(SequenceResponseType, SequenceAllocation, int)> ReserveSequenceRange(string url, string name, int count, string? idempotencyKey, SequenceDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("ReserveSequenceRange", url);
        try { return op.Observe(inner.ReserveSequenceRange(url, name, count, idempotencyKey, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(SequenceResponseType, int)> DeleteSequence(string url, string name, SequenceDurability durability, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("DeleteSequence", url);
        try { return op.Observe(inner.DeleteSequence(url, name, durability, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaRegisterKeyRangeResponse> RegisterKeyRange(string url, string keySpace, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("RegisterKeyRange", url);
        try { return op.Observe(inner.RegisterKeyRange(url, keySpace, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaRemoveKeyRangeResponse> RemoveKeyRange(string url, string keySpace, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("RemoveKeyRange", url);
        try { return op.Observe(inner.RemoveKeyRange(url, keySpace, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaRangeMapResponse> GetRanges(string url, string? keySpace, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("GetRanges", url);
        try { return op.Observe(inner.GetRanges(url, keySpace, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaSplitRangeResponse> SplitRange(string url, string keySpace, string splitKey, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("SplitRange", url);
        try { return op.Observe(inner.SplitRange(url, keySpace, splitKey, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaMergeRangesResponse> MergeRanges(string url, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("MergeRanges", url);
        try { return op.Observe(inner.MergeRanges(url, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry)> AcquireSnapshotHold(string url, string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("AcquireSnapshotHold", url);
        try { return op.Observe(inner.AcquireSnapshotHold(url, holderId, timestamp, leaseMs, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(KeyValueResponseType type, HLCTimestamp leaseExpiry)> RenewSnapshotHold(string url, string holdId, int leaseMs, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("RenewSnapshotHold", url);
        try { return op.Observe(inner.RenewSnapshotHold(url, holdId, leaseMs, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KeyValueResponseType> ReleaseSnapshotHold(string url, string holdId, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("ReleaseSnapshotHold", url);
        try { return op.Observe(inner.ReleaseSnapshotHold(url, holdId, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<(HLCTimestamp effectiveFloor, int liveHolds)> GetSnapshotFloor(string url, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("GetSnapshotFloor", url);
        try { return op.Observe(inner.GetSnapshotFloor(url, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<HistoryHoldAnswer> HistoryHold(string url, HistoryHoldRequest request, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("HistoryHold", url);
        try { return op.Observe(inner.HistoryHold(url, request, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaClusterMembershipResponse> GetClusterMembership(string url, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("GetClusterMembership", url);
        try { return op.Observe(inner.GetClusterMembership(url, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaClusterLeaveResponse> LeaveCluster(string url, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("LeaveCluster", url);
        try { return op.Observe(inner.LeaveCluster(url, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaClusterPlacementResponse> GetClusterPlacement(string url, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("GetClusterPlacement", url);
        try { return op.Observe(inner.GetClusterPlacement(url, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaSetReplicationFactorResponse> SetReplicationFactor(string url, int partitionId, int replicationFactor, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("SetReplicationFactor", url);
        try { return op.Observe(inner.SetReplicationFactor(url, partitionId, replicationFactor, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaBackupInfo> TakeFullBackup(string url, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("TakeFullBackup", url);
        try { return op.Observe(inner.TakeFullBackup(url, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaBackupInfo> TakeIncrementalBackup(string url, Guid parentBackupId, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("TakeIncrementalBackup", url);
        try { return op.Observe(inner.TakeIncrementalBackup(url, parentBackupId, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaBackupInfo> TakeCoordinatedBackup(string url, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("TakeCoordinatedBackup", url);
        try { return op.Observe(inner.TakeCoordinatedBackup(url, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<List<KahunaBackupInfo>> ListBackups(string url, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("ListBackups", url);
        try { return op.Observe(inner.ListBackups(url, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<List<KahunaBackupInfo>> GetBackupChain(string url, Guid leafBackupId, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("GetBackupChain", url);
        try { return op.Observe(inner.GetBackupChain(url, leafBackupId, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaRestoreResponse> Restore(string url, Guid leafBackupId, string targetDir, long targetTimeMs, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("Restore", url);
        try { return op.Observe(inner.Restore(url, leafBackupId, targetDir, targetTimeMs, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }

    public Task<KahunaBackupGcResult> RunBackupGarbageCollection(string url, bool dryRun, CancellationToken cancellationToken)
    {
        KahunaClientOperation op = KahunaClientOperation.Begin("RunBackupGarbageCollection", url);
        try { return op.Observe(inner.RunBackupGarbageCollection(url, dryRun, cancellationToken)); }
        catch (Exception ex) { op.Fail(ex); throw; }
    }
}

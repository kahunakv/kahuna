
namespace Kahuna.Server.Communication.Internode.Grpc;

/// <summary>
/// A single queued inter-node batch operation's request. Exactly one operation payload is ever
/// populated, so rather than a wide class with one nullable field per operation type (an allocation
/// per queued operation) the single payload is held as one reference in a value type. The
/// per-operation accessors preserve the original API by narrowing that reference.
/// </summary>
internal readonly struct GrpcServerBatcherRequest
{
    private readonly object? payload;

    public GrpcServerBatcherRequest(GrpcTryLockRequest tryLock) => payload = tryLock;

    public GrpcServerBatcherRequest(GrpcUnlockRequest unlock) => payload = unlock;

    public GrpcServerBatcherRequest(GrpcExtendLockRequest extendLock) => payload = extendLock;

    public GrpcServerBatcherRequest(GrpcGetLockRequest getLock) => payload = getLock;

    public GrpcServerBatcherRequest(GrpcTrySetKeyValueRequest trySetKeyValue) => payload = trySetKeyValue;

    public GrpcServerBatcherRequest(GrpcTrySetManyKeyValueRequest trySetManyKeyValue) => payload = trySetManyKeyValue;

    public GrpcServerBatcherRequest(GrpcTryDeleteManyKeyValueRequest tryDeleteManyKeyValue) => payload = tryDeleteManyKeyValue;

    public GrpcServerBatcherRequest(GrpcTryGetKeyValueRequest tryGetKeyValue) => payload = tryGetKeyValue;

    public GrpcServerBatcherRequest(GrpcTryGetManyValuesRequest tryGetManyValues) => payload = tryGetManyValues;

    public GrpcServerBatcherRequest(GrpcTryDeleteKeyValueRequest tryDeleteKeyValue) => payload = tryDeleteKeyValue;

    public GrpcServerBatcherRequest(GrpcTryExtendKeyValueRequest tryExtendKeyValue) => payload = tryExtendKeyValue;

    public GrpcServerBatcherRequest(GrpcTryExistsKeyValueRequest tryExistsKeyValue) => payload = tryExistsKeyValue;

    public GrpcServerBatcherRequest(GrpcTryExistsManyValuesRequest tryExistsManyValues) => payload = tryExistsManyValues;

    public GrpcServerBatcherRequest(GrpcTryCheckWriteIntentRequest tryCheckWriteIntent) => payload = tryCheckWriteIntent;

    public GrpcServerBatcherRequest(GrpcGetByBucketRequest getByBucket) => payload = getByBucket;

    public GrpcServerBatcherRequest(GrpcGetByRangeRequest getByRange) => payload = getByRange;

    public GrpcServerBatcherRequest(GrpcScanByPrefixRequest scanByPrefix) => payload = scanByPrefix;

    public GrpcServerBatcherRequest(GrpcTryExecuteTransactionScriptRequest tryExecuteTransactionScript) => payload = tryExecuteTransactionScript;

    public GrpcServerBatcherRequest(GrpcTryAcquireExclusiveLockRequest tryAcquireExclusiveLock) => payload = tryAcquireExclusiveLock;

    public GrpcServerBatcherRequest(GrpcTryAcquireExclusivePrefixLockRequest tryAcquireExclusivePrefixLock) => payload = tryAcquireExclusivePrefixLock;

    public GrpcServerBatcherRequest(GrpcTryAcquireExclusiveRangeLockRequest tryAcquireExclusiveRangeLock) => payload = tryAcquireExclusiveRangeLock;

    public GrpcServerBatcherRequest(GrpcTryAcquireManyExclusiveLocksRequest tryAcquireManyExclusiveLocks) => payload = tryAcquireManyExclusiveLocks;

    public GrpcServerBatcherRequest(GrpcTryReleaseExclusiveLockRequest tryReleaseExclusiveLock) => payload = tryReleaseExclusiveLock;

    public GrpcServerBatcherRequest(GrpcTryReleaseExclusivePrefixLockRequest tryReleaseExclusivePrefixLock) => payload = tryReleaseExclusivePrefixLock;

    public GrpcServerBatcherRequest(GrpcTryReleaseExclusiveRangeLockRequest tryReleaseExclusiveRangeLock) => payload = tryReleaseExclusiveRangeLock;

    public GrpcServerBatcherRequest(GrpcTryReleaseManyExclusiveLocksRequest tryReleaseManyExclusiveLocks) => payload = tryReleaseManyExclusiveLocks;

    public GrpcServerBatcherRequest(GrpcTryPrepareMutationsRequest tryPrepareMutations) => payload = tryPrepareMutations;

    public GrpcServerBatcherRequest(GrpcTryPrepareManyMutationsRequest tryPrepareManyMutations) => payload = tryPrepareManyMutations;

    public GrpcServerBatcherRequest(GrpcTryCommitMutationsRequest tryCommitMutations) => payload = tryCommitMutations;

    public GrpcServerBatcherRequest(GrpcTryCommitManyMutationsRequest tryCommitManyMutations) => payload = tryCommitManyMutations;

    public GrpcServerBatcherRequest(GrpcTryRollbackMutationsRequest tryRollbackMutations) => payload = tryRollbackMutations;

    public GrpcServerBatcherRequest(GrpcTryRollbackManyMutationsRequest tryRollbackManyMutations) => payload = tryRollbackManyMutations;

    public GrpcServerBatcherRequest(GrpcStartTransactionRequest startTransaction) => payload = startTransaction;

    public GrpcServerBatcherRequest(GrpcCommitTransactionRequest commitTransaction) => payload = commitTransaction;

    public GrpcServerBatcherRequest(GrpcRollbackTransactionRequest rollbackTransaction) => payload = rollbackTransaction;

    public GrpcServerBatcherRequest(GrpcEnsureKeyRangeSeededRequest ensureKeyRangeSeeded) => payload = ensureKeyRangeSeeded;

    public GrpcServerBatcherRequest(GrpcEnsureKeyRangeRemovedRequest ensureKeyRangeRemoved) => payload = ensureKeyRangeRemoved;

    public GrpcServerBatcherRequest(GrpcGetRangeLocksRequest getRangeLocks) => payload = getRangeLocks;

    public GrpcServerBatcherRequest(GrpcImportRangeLocksRequest importRangeLocks) => payload = importRangeLocks;

    public GrpcServerBatcherRequest(GrpcAcquireSnapshotHoldRequest acquireSnapshotHold) => payload = acquireSnapshotHold;

    public GrpcServerBatcherRequest(GrpcRenewSnapshotHoldRequest renewSnapshotHold) => payload = renewSnapshotHold;

    public GrpcServerBatcherRequest(GrpcReleaseSnapshotHoldRequest releaseSnapshotHold) => payload = releaseSnapshotHold;

    public GrpcServerBatcherRequest(GrpcGetSnapshotFloorRequest getSnapshotFloor) => payload = getSnapshotFloor;

    public GrpcServerBatcherRequest(GrpcBeginOperationRequest beginOperation) => payload = beginOperation;

    public GrpcServerBatcherRequest(GrpcCompleteOperationRequest completeOperation) => payload = completeOperation;

    public GrpcServerBatcherRequest(GrpcGetTransactionWorkingSetRequest getTransactionWorkingSet) => payload = getTransactionWorkingSet;

    public GrpcServerBatcherRequest(GrpcCloseTransactionRequest closeTransaction) => payload = closeTransaction;

    public GrpcServerBatcherRequest(GrpcImportCompletionReceiptsRequest importCompletionReceipts) => payload = importCompletionReceipts;

    public GrpcServerBatcherRequest(GrpcImportCoordinatorDecisionsRequest importCoordinatorDecisions) => payload = importCoordinatorDecisions;

    public GrpcTryLockRequest? TryLock => payload as GrpcTryLockRequest;

    public GrpcUnlockRequest? Unlock => payload as GrpcUnlockRequest;

    public GrpcExtendLockRequest? ExtendLock => payload as GrpcExtendLockRequest;

    public GrpcGetLockRequest? GetLock => payload as GrpcGetLockRequest;

    public GrpcTrySetKeyValueRequest? TrySetKeyValue => payload as GrpcTrySetKeyValueRequest;

    public GrpcTrySetManyKeyValueRequest? TrySetManyKeyValue => payload as GrpcTrySetManyKeyValueRequest;

    public GrpcTryDeleteManyKeyValueRequest? TryDeleteManyKeyValue => payload as GrpcTryDeleteManyKeyValueRequest;

    public GrpcTryGetKeyValueRequest? TryGetKeyValue => payload as GrpcTryGetKeyValueRequest;

    public GrpcTryGetManyValuesRequest? TryGetManyValues => payload as GrpcTryGetManyValuesRequest;

    public GrpcTryDeleteKeyValueRequest? TryDeleteKeyValue => payload as GrpcTryDeleteKeyValueRequest;

    public GrpcTryExtendKeyValueRequest? TryExtendKeyValue => payload as GrpcTryExtendKeyValueRequest;

    public GrpcTryExistsKeyValueRequest? TryExistsKeyValue => payload as GrpcTryExistsKeyValueRequest;

    public GrpcTryExistsManyValuesRequest? TryExistsManyValues => payload as GrpcTryExistsManyValuesRequest;

    public GrpcTryCheckWriteIntentRequest? TryCheckWriteIntent => payload as GrpcTryCheckWriteIntentRequest;

    public GrpcGetByBucketRequest? GetByBucket => payload as GrpcGetByBucketRequest;

    public GrpcGetByRangeRequest? GetByRange => payload as GrpcGetByRangeRequest;

    public GrpcScanByPrefixRequest? ScanByPrefix => payload as GrpcScanByPrefixRequest;

    public GrpcTryExecuteTransactionScriptRequest? TryExecuteTransactionScript => payload as GrpcTryExecuteTransactionScriptRequest;

    public GrpcTryAcquireExclusiveLockRequest? TryAcquireExclusiveLock => payload as GrpcTryAcquireExclusiveLockRequest;

    public GrpcTryAcquireExclusivePrefixLockRequest? TryAcquireExclusivePrefixLock => payload as GrpcTryAcquireExclusivePrefixLockRequest;

    public GrpcTryAcquireExclusiveRangeLockRequest? TryAcquireExclusiveRangeLock => payload as GrpcTryAcquireExclusiveRangeLockRequest;

    public GrpcTryAcquireManyExclusiveLocksRequest? TryAcquireManyExclusiveLocks => payload as GrpcTryAcquireManyExclusiveLocksRequest;

    public GrpcTryReleaseExclusiveLockRequest? TryReleaseExclusiveLock => payload as GrpcTryReleaseExclusiveLockRequest;

    public GrpcTryReleaseExclusivePrefixLockRequest? TryReleaseExclusivePrefixLock => payload as GrpcTryReleaseExclusivePrefixLockRequest;

    public GrpcTryReleaseExclusiveRangeLockRequest? TryReleaseExclusiveRangeLock => payload as GrpcTryReleaseExclusiveRangeLockRequest;

    public GrpcTryReleaseManyExclusiveLocksRequest? TryReleaseManyExclusiveLocks => payload as GrpcTryReleaseManyExclusiveLocksRequest;

    public GrpcTryPrepareMutationsRequest? TryPrepareMutations => payload as GrpcTryPrepareMutationsRequest;

    public GrpcTryPrepareManyMutationsRequest? TryPrepareManyMutations => payload as GrpcTryPrepareManyMutationsRequest;

    public GrpcTryCommitMutationsRequest? TryCommitMutations => payload as GrpcTryCommitMutationsRequest;

    public GrpcTryCommitManyMutationsRequest? TryCommitManyMutations => payload as GrpcTryCommitManyMutationsRequest;

    public GrpcTryRollbackMutationsRequest? TryRollbackMutations => payload as GrpcTryRollbackMutationsRequest;

    public GrpcTryRollbackManyMutationsRequest? TryRollbackManyMutations => payload as GrpcTryRollbackManyMutationsRequest;

    public GrpcStartTransactionRequest? StartTransaction => payload as GrpcStartTransactionRequest;

    public GrpcCommitTransactionRequest? CommitTransaction => payload as GrpcCommitTransactionRequest;

    public GrpcRollbackTransactionRequest? RollbackTransaction => payload as GrpcRollbackTransactionRequest;

    public GrpcEnsureKeyRangeSeededRequest? EnsureKeyRangeSeeded => payload as GrpcEnsureKeyRangeSeededRequest;

    public GrpcEnsureKeyRangeRemovedRequest? EnsureKeyRangeRemoved => payload as GrpcEnsureKeyRangeRemovedRequest;

    public GrpcGetRangeLocksRequest? GetRangeLocks => payload as GrpcGetRangeLocksRequest;

    public GrpcImportRangeLocksRequest? ImportRangeLocks => payload as GrpcImportRangeLocksRequest;

    public GrpcAcquireSnapshotHoldRequest? AcquireSnapshotHold => payload as GrpcAcquireSnapshotHoldRequest;

    public GrpcRenewSnapshotHoldRequest? RenewSnapshotHold => payload as GrpcRenewSnapshotHoldRequest;

    public GrpcReleaseSnapshotHoldRequest? ReleaseSnapshotHold => payload as GrpcReleaseSnapshotHoldRequest;

    public GrpcGetSnapshotFloorRequest? GetSnapshotFloor => payload as GrpcGetSnapshotFloorRequest;

    public GrpcBeginOperationRequest? BeginOperation => payload as GrpcBeginOperationRequest;

    public GrpcCompleteOperationRequest? CompleteOperation => payload as GrpcCompleteOperationRequest;

    public GrpcGetTransactionWorkingSetRequest? GetTransactionWorkingSet => payload as GrpcGetTransactionWorkingSetRequest;

    public GrpcCloseTransactionRequest? CloseTransaction => payload as GrpcCloseTransactionRequest;

    public GrpcImportCompletionReceiptsRequest? ImportCompletionReceipts => payload as GrpcImportCompletionReceiptsRequest;

    public GrpcImportCoordinatorDecisionsRequest? ImportCoordinatorDecisions => payload as GrpcImportCoordinatorDecisionsRequest;
}

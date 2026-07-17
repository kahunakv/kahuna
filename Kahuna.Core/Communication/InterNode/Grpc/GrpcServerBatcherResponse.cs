
namespace Kahuna.Server.Communication.Internode.Grpc;

/// <summary>
/// A single inter-node batch operation's response. Exactly one operation payload is ever populated,
/// so rather than a wide class with one nullable field per operation type (an allocation per
/// completed operation) the single payload is held as one reference in a value type. The
/// per-operation accessors preserve the original API by narrowing that reference.
/// </summary>
internal readonly struct GrpcServerBatcherResponse
{
    private readonly object? payload;

    public GrpcServerBatcherResponse(GrpcTryLockResponse tryLock) => payload = tryLock;

    public GrpcServerBatcherResponse(GrpcUnlockResponse unlock) => payload = unlock;

    public GrpcServerBatcherResponse(GrpcExtendLockResponse extendLock) => payload = extendLock;

    public GrpcServerBatcherResponse(GrpcGetLockResponse getLock) => payload = getLock;

    public GrpcServerBatcherResponse(GrpcTrySetKeyValueResponse trySetKeyValue) => payload = trySetKeyValue;

    public GrpcServerBatcherResponse(GrpcTrySetManyKeyValueResponse trySetManyKeyValue) => payload = trySetManyKeyValue;

    public GrpcServerBatcherResponse(GrpcTryDeleteManyKeyValueResponse tryDeleteManyKeyValue) => payload = tryDeleteManyKeyValue;

    public GrpcServerBatcherResponse(GrpcTryGetKeyValueResponse tryGetKeyValue) => payload = tryGetKeyValue;

    public GrpcServerBatcherResponse(GrpcTryGetManyValuesResponse tryGetManyValues) => payload = tryGetManyValues;

    public GrpcServerBatcherResponse(GrpcTryDeleteKeyValueResponse tryDeleteKeyValue) => payload = tryDeleteKeyValue;

    public GrpcServerBatcherResponse(GrpcTryExtendKeyValueResponse tryExtendKeyValue) => payload = tryExtendKeyValue;

    public GrpcServerBatcherResponse(GrpcTryExistsKeyValueResponse tryExistsKeyValue) => payload = tryExistsKeyValue;

    public GrpcServerBatcherResponse(GrpcTryExistsManyValuesResponse tryExistsManyValues) => payload = tryExistsManyValues;

    public GrpcServerBatcherResponse(GrpcTryCheckWriteIntentResponse tryCheckWriteIntent) => payload = tryCheckWriteIntent;

    public GrpcServerBatcherResponse(GrpcGetByBucketResponse getByBucket) => payload = getByBucket;

    public GrpcServerBatcherResponse(GrpcGetByRangeResponse getByRange) => payload = getByRange;

    public GrpcServerBatcherResponse(GrpcScanByPrefixResponse scanByPrefix) => payload = scanByPrefix;

    public GrpcServerBatcherResponse(GrpcTryExecuteTransactionScriptResponse tryExecuteTransactionScript) => payload = tryExecuteTransactionScript;

    public GrpcServerBatcherResponse(GrpcTryAcquireExclusiveLockResponse tryAcquireExclusiveLock) => payload = tryAcquireExclusiveLock;

    public GrpcServerBatcherResponse(GrpcTryAcquireExclusivePrefixLockResponse tryAcquireExclusivePrefixLock) => payload = tryAcquireExclusivePrefixLock;

    public GrpcServerBatcherResponse(GrpcTryAcquireExclusiveRangeLockResponse tryAcquireExclusiveRangeLock) => payload = tryAcquireExclusiveRangeLock;

    public GrpcServerBatcherResponse(GrpcTryAcquireManyExclusiveLocksResponse tryAcquireManyExclusiveLocks) => payload = tryAcquireManyExclusiveLocks;

    public GrpcServerBatcherResponse(GrpcTryReleaseExclusiveLockResponse tryReleaseExclusiveLock) => payload = tryReleaseExclusiveLock;

    public GrpcServerBatcherResponse(GrpcTryReleaseExclusivePrefixLockResponse tryReleaseExclusivePrefixLock) => payload = tryReleaseExclusivePrefixLock;

    public GrpcServerBatcherResponse(GrpcTryReleaseExclusiveRangeLockResponse tryReleaseExclusiveRangeLock) => payload = tryReleaseExclusiveRangeLock;

    public GrpcServerBatcherResponse(GrpcTryReleaseManyExclusiveLocksResponse tryReleaseManyExclusiveLocks) => payload = tryReleaseManyExclusiveLocks;

    public GrpcServerBatcherResponse(GrpcTryPrepareMutationsResponse tryPrepareMutations) => payload = tryPrepareMutations;

    public GrpcServerBatcherResponse(GrpcTryPrepareManyMutationsResponse tryPrepareManyMutations) => payload = tryPrepareManyMutations;

    public GrpcServerBatcherResponse(GrpcTryCommitMutationsResponse tryCommitMutations) => payload = tryCommitMutations;

    public GrpcServerBatcherResponse(GrpcTryCommitManyMutationsResponse tryCommitManyMutations) => payload = tryCommitManyMutations;

    public GrpcServerBatcherResponse(GrpcTryRollbackMutationsResponse tryRollbackMutations) => payload = tryRollbackMutations;

    public GrpcServerBatcherResponse(GrpcTryRollbackManyMutationsResponse tryRollbackManyMutations) => payload = tryRollbackManyMutations;

    public GrpcServerBatcherResponse(GrpcStartTransactionResponse startTransaction) => payload = startTransaction;

    public GrpcServerBatcherResponse(GrpcCommitTransactionResponse commitTransaction) => payload = commitTransaction;

    public GrpcServerBatcherResponse(GrpcRollbackTransactionResponse rollbackTransaction) => payload = rollbackTransaction;

    public GrpcServerBatcherResponse(GrpcEnsureKeyRangeSeededResponse ensureKeyRangeSeeded) => payload = ensureKeyRangeSeeded;

    public GrpcServerBatcherResponse(GrpcEnsureKeyRangeRemovedResponse ensureKeyRangeRemoved) => payload = ensureKeyRangeRemoved;

    public GrpcServerBatcherResponse(GrpcGetRangeLocksResponse getRangeLocks) => payload = getRangeLocks;

    public GrpcServerBatcherResponse(GrpcImportRangeLocksResponse importRangeLocks) => payload = importRangeLocks;

    public GrpcServerBatcherResponse(GrpcImportCompletionReceiptsResponse importCompletionReceipts) => payload = importCompletionReceipts;

    public GrpcServerBatcherResponse(GrpcImportCoordinatorDecisionsResponse importCoordinatorDecisions) => payload = importCoordinatorDecisions;

    public GrpcServerBatcherResponse(GrpcAcquireSnapshotHoldResponse acquireSnapshotHold) => payload = acquireSnapshotHold;

    public GrpcServerBatcherResponse(GrpcRenewSnapshotHoldResponse renewSnapshotHold) => payload = renewSnapshotHold;

    public GrpcServerBatcherResponse(GrpcReleaseSnapshotHoldResponse releaseSnapshotHold) => payload = releaseSnapshotHold;

    public GrpcServerBatcherResponse(GrpcGetSnapshotFloorResponse getSnapshotFloor) => payload = getSnapshotFloor;

    public GrpcServerBatcherResponse(GrpcBeginOperationResponse beginOperation) => payload = beginOperation;

    public GrpcServerBatcherResponse(GrpcCompleteOperationResponse completeOperation) => payload = completeOperation;

    public GrpcServerBatcherResponse(GrpcGetTransactionWorkingSetResponse getTransactionWorkingSet) => payload = getTransactionWorkingSet;

    public GrpcServerBatcherResponse(GrpcCloseTransactionResponse closeTransaction) => payload = closeTransaction;

    public GrpcTryLockResponse? TryLock => payload as GrpcTryLockResponse;

    public GrpcUnlockResponse? Unlock => payload as GrpcUnlockResponse;

    public GrpcExtendLockResponse? ExtendLock => payload as GrpcExtendLockResponse;

    public GrpcGetLockResponse? GetLock => payload as GrpcGetLockResponse;

    public GrpcTrySetKeyValueResponse? TrySetKeyValue => payload as GrpcTrySetKeyValueResponse;

    public GrpcTrySetManyKeyValueResponse? TrySetManyKeyValue => payload as GrpcTrySetManyKeyValueResponse;

    public GrpcTryDeleteManyKeyValueResponse? TryDeleteManyKeyValue => payload as GrpcTryDeleteManyKeyValueResponse;

    public GrpcTryGetKeyValueResponse? TryGetKeyValue => payload as GrpcTryGetKeyValueResponse;

    public GrpcTryGetManyValuesResponse? TryGetManyValues => payload as GrpcTryGetManyValuesResponse;

    public GrpcTryDeleteKeyValueResponse? TryDeleteKeyValue => payload as GrpcTryDeleteKeyValueResponse;

    public GrpcTryExtendKeyValueResponse? TryExtendKeyValue => payload as GrpcTryExtendKeyValueResponse;

    public GrpcTryExistsKeyValueResponse? TryExistsKeyValue => payload as GrpcTryExistsKeyValueResponse;

    public GrpcTryExistsManyValuesResponse? TryExistsManyValues => payload as GrpcTryExistsManyValuesResponse;

    public GrpcTryCheckWriteIntentResponse? TryCheckWriteIntent => payload as GrpcTryCheckWriteIntentResponse;

    public GrpcGetByBucketResponse? GetByBucket => payload as GrpcGetByBucketResponse;

    public GrpcGetByRangeResponse? GetByRange => payload as GrpcGetByRangeResponse;

    public GrpcScanByPrefixResponse? ScanByPrefix => payload as GrpcScanByPrefixResponse;

    public GrpcTryExecuteTransactionScriptResponse? TryExecuteTransactionScript => payload as GrpcTryExecuteTransactionScriptResponse;

    public GrpcTryAcquireExclusiveLockResponse? TryAcquireExclusiveLock => payload as GrpcTryAcquireExclusiveLockResponse;

    public GrpcTryAcquireExclusivePrefixLockResponse? TryAcquireExclusivePrefixLock => payload as GrpcTryAcquireExclusivePrefixLockResponse;

    public GrpcTryAcquireExclusiveRangeLockResponse? TryAcquireExclusiveRangeLock => payload as GrpcTryAcquireExclusiveRangeLockResponse;

    public GrpcTryAcquireManyExclusiveLocksResponse? TryAcquireManyExclusiveLocks => payload as GrpcTryAcquireManyExclusiveLocksResponse;

    public GrpcTryReleaseExclusiveLockResponse? TryReleaseExclusiveLock => payload as GrpcTryReleaseExclusiveLockResponse;

    public GrpcTryReleaseExclusivePrefixLockResponse? TryReleaseExclusivePrefixLock => payload as GrpcTryReleaseExclusivePrefixLockResponse;

    public GrpcTryReleaseExclusiveRangeLockResponse? TryReleaseExclusiveRangeLock => payload as GrpcTryReleaseExclusiveRangeLockResponse;

    public GrpcTryReleaseManyExclusiveLocksResponse? TryReleaseManyExclusiveLocks => payload as GrpcTryReleaseManyExclusiveLocksResponse;

    public GrpcTryPrepareMutationsResponse? TryPrepareMutations => payload as GrpcTryPrepareMutationsResponse;

    public GrpcTryPrepareManyMutationsResponse? TryPrepareManyMutations => payload as GrpcTryPrepareManyMutationsResponse;

    public GrpcTryCommitMutationsResponse? TryCommitMutations => payload as GrpcTryCommitMutationsResponse;

    public GrpcTryCommitManyMutationsResponse? TryCommitManyMutations => payload as GrpcTryCommitManyMutationsResponse;

    public GrpcTryRollbackMutationsResponse? TryRollbackMutations => payload as GrpcTryRollbackMutationsResponse;

    public GrpcTryRollbackManyMutationsResponse? TryRollbackManyMutations => payload as GrpcTryRollbackManyMutationsResponse;

    public GrpcStartTransactionResponse? StartTransaction => payload as GrpcStartTransactionResponse;

    public GrpcCommitTransactionResponse? CommitTransaction => payload as GrpcCommitTransactionResponse;

    public GrpcRollbackTransactionResponse? RollbackTransaction => payload as GrpcRollbackTransactionResponse;

    public GrpcEnsureKeyRangeSeededResponse? EnsureKeyRangeSeeded => payload as GrpcEnsureKeyRangeSeededResponse;

    public GrpcEnsureKeyRangeRemovedResponse? EnsureKeyRangeRemoved => payload as GrpcEnsureKeyRangeRemovedResponse;

    public GrpcGetRangeLocksResponse? GetRangeLocks => payload as GrpcGetRangeLocksResponse;

    public GrpcImportRangeLocksResponse? ImportRangeLocks => payload as GrpcImportRangeLocksResponse;

    public GrpcImportCompletionReceiptsResponse? ImportCompletionReceipts => payload as GrpcImportCompletionReceiptsResponse;

    public GrpcImportCoordinatorDecisionsResponse? ImportCoordinatorDecisions => payload as GrpcImportCoordinatorDecisionsResponse;

    public GrpcAcquireSnapshotHoldResponse? AcquireSnapshotHold => payload as GrpcAcquireSnapshotHoldResponse;

    public GrpcRenewSnapshotHoldResponse? RenewSnapshotHold => payload as GrpcRenewSnapshotHoldResponse;

    public GrpcReleaseSnapshotHoldResponse? ReleaseSnapshotHold => payload as GrpcReleaseSnapshotHoldResponse;

    public GrpcGetSnapshotFloorResponse? GetSnapshotFloor => payload as GrpcGetSnapshotFloorResponse;

    public GrpcBeginOperationResponse? BeginOperation => payload as GrpcBeginOperationResponse;

    public GrpcCompleteOperationResponse? CompleteOperation => payload as GrpcCompleteOperationResponse;

    public GrpcGetTransactionWorkingSetResponse? GetTransactionWorkingSet => payload as GrpcGetTransactionWorkingSetResponse;

    public GrpcCloseTransactionResponse? CloseTransaction => payload as GrpcCloseTransactionResponse;
}

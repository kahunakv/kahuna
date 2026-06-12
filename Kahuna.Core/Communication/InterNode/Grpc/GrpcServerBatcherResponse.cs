
namespace Kahuna.Server.Communication.Internode.Grpc;

internal sealed class GrpcServerBatcherResponse
{
    public GrpcTryLockResponse? TryLock { get; }
    
    public GrpcUnlockResponse? Unlock { get; }
    
    public GrpcExtendLockResponse? ExtendLock { get; }
    
    public GrpcGetLockResponse? GetLock { get; }
    
    public GrpcTrySetKeyValueResponse? TrySetKeyValue { get; }
    
    public GrpcTrySetManyKeyValueResponse? TrySetManyKeyValue { get; }

    public GrpcTryDeleteManyKeyValueResponse? TryDeleteManyKeyValue { get; }
    
    public GrpcTryGetKeyValueResponse? TryGetKeyValue { get; }

    public GrpcTryGetManyValuesResponse? TryGetManyValues { get; }
    
    public GrpcTryDeleteKeyValueResponse? TryDeleteKeyValue { get; }
    
    public GrpcTryExtendKeyValueResponse? TryExtendKeyValue { get; }

    public GrpcTryExistsKeyValueResponse? TryExistsKeyValue { get; }

    public GrpcTryExistsManyValuesResponse? TryExistsManyValues { get; }

    public GrpcTryCheckWriteIntentResponse? TryCheckWriteIntent { get; }

    public GrpcGetByBucketResponse? GetByBucket { get; }

    public GrpcGetByRangeResponse? GetByRange { get; }
    
    public GrpcScanByPrefixResponse? ScanByPrefix { get; }
    
    public GrpcTryExecuteTransactionScriptResponse? TryExecuteTransactionScript { get; }
    
    public GrpcTryAcquireExclusiveLockResponse? TryAcquireExclusiveLock { get; }
    
    public GrpcTryAcquireExclusivePrefixLockResponse? TryAcquireExclusivePrefixLock { get; }

    public GrpcTryAcquireExclusiveRangeLockResponse? TryAcquireExclusiveRangeLock { get; }

    public GrpcTryAcquireManyExclusiveLocksResponse? TryAcquireManyExclusiveLocks { get; }

    public GrpcTryReleaseExclusiveLockResponse? TryReleaseExclusiveLock { get; }

    public GrpcTryReleaseExclusivePrefixLockResponse? TryReleaseExclusivePrefixLock { get; }

    public GrpcTryReleaseExclusiveRangeLockResponse? TryReleaseExclusiveRangeLock { get; }
    
    public GrpcTryReleaseManyExclusiveLocksResponse? TryReleaseManyExclusiveLocks { get; }
    
    public GrpcTryPrepareMutationsResponse? TryPrepareMutations { get; }
    
    public GrpcTryPrepareManyMutationsResponse? TryPrepareManyMutations { get; }
    
    public GrpcTryCommitMutationsResponse? TryCommitMutations { get; }
    
    public GrpcTryCommitManyMutationsResponse? TryCommitManyMutations { get; }
    
    public GrpcTryRollbackMutationsResponse? TryRollbackMutations { get; }
    
    public GrpcTryRollbackManyMutationsResponse? TryRollbackManyMutations { get; }
    
    public GrpcStartTransactionResponse? StartTransaction { get; }
    
    public GrpcCommitTransactionResponse? CommitTransaction { get; }
    
    
    public GrpcRollbackTransactionResponse? RollbackTransaction { get; }

    public GrpcEnsureKeyRangeSeededResponse? EnsureKeyRangeSeeded { get; }

    public GrpcGetRangeLocksResponse? GetRangeLocks { get; }

    public GrpcImportRangeLocksResponse? ImportRangeLocks { get; }

    public GrpcServerBatcherResponse(GrpcTryLockResponse tryLock)
    {
        TryLock = tryLock;
    }
    
    public GrpcServerBatcherResponse(GrpcUnlockResponse unlock)
    {
        Unlock = unlock;
    }
    
    public GrpcServerBatcherResponse(GrpcExtendLockResponse extendLock)
    {
        ExtendLock = extendLock;
    }
    
    public GrpcServerBatcherResponse(GrpcGetLockResponse getLock)
    {
        GetLock = getLock;
    }

    public GrpcServerBatcherResponse(GrpcTrySetKeyValueResponse trySetKeyValue)
    {
        TrySetKeyValue = trySetKeyValue;
    }

    public GrpcServerBatcherResponse(GrpcTrySetManyKeyValueResponse trySetManyKeyValue)
    {
        TrySetManyKeyValue = trySetManyKeyValue;
    }

    public GrpcServerBatcherResponse(GrpcTryDeleteManyKeyValueResponse tryDeleteManyKeyValue)
    {
        TryDeleteManyKeyValue = tryDeleteManyKeyValue;
    }
    
    public GrpcServerBatcherResponse(GrpcTryGetKeyValueResponse tryGetKeyValue)
    {
        TryGetKeyValue = tryGetKeyValue;
    }

    public GrpcServerBatcherResponse(GrpcTryGetManyValuesResponse tryGetManyValues)
    {
        TryGetManyValues = tryGetManyValues;
    }
    
    public GrpcServerBatcherResponse(GrpcTryDeleteKeyValueResponse tryDeleteKeyValue)
    {
        TryDeleteKeyValue = tryDeleteKeyValue;
    }
    
    public GrpcServerBatcherResponse(GrpcTryExtendKeyValueResponse tryExtendKeyValue)
    {
        TryExtendKeyValue = tryExtendKeyValue;
    }
    
    public GrpcServerBatcherResponse(GrpcTryExistsKeyValueResponse tryExistsKeyValue)
    {
        TryExistsKeyValue = tryExistsKeyValue;
    }

    public GrpcServerBatcherResponse(GrpcTryExistsManyValuesResponse tryExistsManyValues)
    {
        TryExistsManyValues = tryExistsManyValues;
    }

    public GrpcServerBatcherResponse(GrpcTryCheckWriteIntentResponse tryCheckWriteIntent)
    {
        TryCheckWriteIntent = tryCheckWriteIntent;
    }

    public GrpcServerBatcherResponse(GrpcGetByBucketResponse getByBucket)
    {
        GetByBucket = getByBucket;
    }

    public GrpcServerBatcherResponse(GrpcGetByRangeResponse getByRange)
    {
        GetByRange = getByRange;
    }
    
    public GrpcServerBatcherResponse(GrpcScanByPrefixResponse scanByPrefix)
    {
        ScanByPrefix = scanByPrefix;
    }
    
    public GrpcServerBatcherResponse(GrpcTryExecuteTransactionScriptResponse tryExecuteTransactionScript)
    {
        TryExecuteTransactionScript = tryExecuteTransactionScript;
    }
    
    public GrpcServerBatcherResponse(GrpcTryAcquireExclusiveLockResponse tryAcquireExclusiveLock)
    {
        TryAcquireExclusiveLock = tryAcquireExclusiveLock;
    }
    
    public GrpcServerBatcherResponse(GrpcTryAcquireExclusivePrefixLockResponse tryAcquireExclusivePrefixLock)
    {
        TryAcquireExclusivePrefixLock = tryAcquireExclusivePrefixLock;
    }
    
    public GrpcServerBatcherResponse(GrpcTryAcquireManyExclusiveLocksResponse tryAcquireManyExclusiveLocks)
    {
        TryAcquireManyExclusiveLocks = tryAcquireManyExclusiveLocks;
    }
    
    public GrpcServerBatcherResponse(GrpcTryReleaseExclusiveLockResponse tryReleaseExclusiveLock)
    {
        TryReleaseExclusiveLock = tryReleaseExclusiveLock;
    }
    
    public GrpcServerBatcherResponse(GrpcTryAcquireExclusiveRangeLockResponse tryAcquireExclusiveRangeLock)
    {
        TryAcquireExclusiveRangeLock = tryAcquireExclusiveRangeLock;
    }

    public GrpcServerBatcherResponse(GrpcTryReleaseExclusivePrefixLockResponse tryReleaseExclusivePrefixLock)
    {
        TryReleaseExclusivePrefixLock = tryReleaseExclusivePrefixLock;
    }

    public GrpcServerBatcherResponse(GrpcTryReleaseExclusiveRangeLockResponse tryReleaseExclusiveRangeLock)
    {
        TryReleaseExclusiveRangeLock = tryReleaseExclusiveRangeLock;
    }

    public GrpcServerBatcherResponse(GrpcTryReleaseManyExclusiveLocksResponse tryReleaseManyExclusiveLocks)
    {
        TryReleaseManyExclusiveLocks = tryReleaseManyExclusiveLocks;
    }
    
    public GrpcServerBatcherResponse(GrpcTryPrepareMutationsResponse tryPrepareMutations)
    {
        TryPrepareMutations = tryPrepareMutations;
    }
    
    public GrpcServerBatcherResponse(GrpcTryPrepareManyMutationsResponse tryPrepareManyMutations)
    {
        TryPrepareManyMutations = tryPrepareManyMutations;
    }
    
    public GrpcServerBatcherResponse(GrpcTryCommitMutationsResponse tryCommitMutations)
    {
        TryCommitMutations = tryCommitMutations;
    }
    
    public GrpcServerBatcherResponse(GrpcTryCommitManyMutationsResponse tryCommitManyMutations)
    {
        TryCommitManyMutations = tryCommitManyMutations;
    }
    
    public GrpcServerBatcherResponse(GrpcTryRollbackMutationsResponse tryRollbackMutations)
    {
        TryRollbackMutations = tryRollbackMutations;
    }
    
    public GrpcServerBatcherResponse(GrpcTryRollbackManyMutationsResponse tryRollbackManyMutations)
    {
        TryRollbackManyMutations = tryRollbackManyMutations;
    }
    
    public GrpcServerBatcherResponse(GrpcStartTransactionResponse startTransaction)
    {
        StartTransaction = startTransaction;
    }
    
    public GrpcServerBatcherResponse(GrpcCommitTransactionResponse commitTransaction)
    {
        CommitTransaction = commitTransaction;
    }
    
    public GrpcServerBatcherResponse(GrpcRollbackTransactionResponse rollbackTransaction)
    {
        RollbackTransaction = rollbackTransaction;
    }

    public GrpcServerBatcherResponse(GrpcEnsureKeyRangeSeededResponse ensureKeyRangeSeeded)
    {
        EnsureKeyRangeSeeded = ensureKeyRangeSeeded;
    }

    public GrpcServerBatcherResponse(GrpcGetRangeLocksResponse getRangeLocks)
    {
        GetRangeLocks = getRangeLocks;
    }

    public GrpcServerBatcherResponse(GrpcImportRangeLocksResponse importRangeLocks)
    {
        ImportRangeLocks = importRangeLocks;
    }
}

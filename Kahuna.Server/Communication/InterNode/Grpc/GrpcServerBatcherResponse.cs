
namespace Kahuna.Server.Communication.Internode.Grpc;

internal sealed class GrpcServerBatcherResponse
{
    public GrpcTryLockResponse? TryLock { get; }
    
    public GrpcUnlockResponse? Unlock { get; }
    
    public GrpcExtendLockResponse? ExtendLock { get; }
    
    public GrpcGetLockResponse? GetLock { get; }
    
    public GrpcTrySetKeyValueResponse? TrySetKeyValue { get; }
    
    public GrpcTrySetManyKeyValueResponse? TrySetManyKeyValue { get; }
    
    public GrpcTryGetKeyValueResponse? TryGetKeyValue { get; }
    
    public GrpcTryDeleteKeyValueResponse? TryDeleteKeyValue { get; }
    
    public GrpcTryExtendKeyValueResponse? TryExtendKeyValue { get; }

    public GrpcTryExistsKeyValueResponse? TryExistsKeyValue { get; }
    
    public GrpcGetByPrefixResponse? GetByPrefix { get; }
    
    public GrpcScanByPrefixResponse? ScanByPrefix { get; }
    
    public GrpcTryExecuteTransactionScriptResponse? TryExecuteTransactionScript { get; }
    
    public GrpcTryAcquireExclusiveLockResponse? TryAcquireExclusiveLock { get; }
    
    public GrpcTryAcquireManyExclusiveLocksResponse? TryAcquireManyExclusiveLocks { get; }
    
    public GrpcTryReleaseExclusiveLockResponse? TryReleaseExclusiveLock { get; }
    
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
    
    public GrpcServerBatcherResponse(GrpcTryGetKeyValueResponse tryGetKeyValue)
    {
        TryGetKeyValue = tryGetKeyValue;
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
    
    public GrpcServerBatcherResponse(GrpcGetByPrefixResponse getByPrefix)
    {
        GetByPrefix = getByPrefix;
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
    
    public GrpcServerBatcherResponse(GrpcTryAcquireManyExclusiveLocksResponse tryAcquireManyExclusiveLocks)
    {
        TryAcquireManyExclusiveLocks = tryAcquireManyExclusiveLocks;
    }
    
    public GrpcServerBatcherResponse(GrpcTryReleaseExclusiveLockResponse tryReleaseExclusiveLock)
    {
        TryReleaseExclusiveLock = tryReleaseExclusiveLock;
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
}
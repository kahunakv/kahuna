
namespace Kahuna.Server.Communication.Internode.Grpc;

internal sealed class GrpcServerBatcherRequest
{   
    public GrpcTryLockRequest? TryLock { get; }

    public GrpcUnlockRequest? Unlock { get; }
    
    public GrpcExtendLockRequest? ExtendLock { get; }
    
    public GrpcGetLockRequest? GetLock { get; }
    
    public GrpcTrySetKeyValueRequest? TrySetKeyValue { get; }
    
    public GrpcTryGetKeyValueRequest? TryGetKeyValue { get; }
    
    public GrpcTryDeleteKeyValueRequest? TryDeleteKeyValue { get; }
    
    public GrpcTryExtendKeyValueRequest? TryExtendKeyValue { get; }
    
    public GrpcTryExistsKeyValueRequest? TryExistsKeyValue { get; }
    
    public GrpcTryExecuteTransactionScriptRequest? TryExecuteTransactionScript { get; }
    
    public GrpcTryAcquireExclusiveLockRequest? TryAcquireExclusiveLock { get; }
    
    public GrpcTryAcquireManyExclusiveLocksRequest? TryAcquireManyExclusiveLocks { get; }
    
    public GrpcTryReleaseExclusiveLockRequest? TryReleaseExclusiveLock { get; }
    
    public GrpcTryReleaseManyExclusiveLocksRequest? TryReleaseManyExclusiveLocks { get; }
    
    public GrpcTryPrepareMutationsRequest? TryPrepareMutations { get; }
    
    public GrpcTryPrepareManyMutationsRequest? TryPrepareManyMutations { get; }
    
    public GrpcTryCommitMutationsRequest? TryCommitMutations { get; }
    
    public GrpcTryCommitManyMutationsRequest? TryCommitManyMutations { get; }
    
    public GrpcTryRollbackMutationsRequest? TryRollbackMutations { get; }
    
    public GrpcTryRollbackManyMutationsRequest? TryRollbackManyMutations { get; }
    
    public GrpcStartTransactionRequest? StartTransaction { get; }
    
    public GrpcCommitTransactionRequest? CommitTransaction { get; }
    
    public GrpcRollbackTransactionRequest? RollbackTransaction { get; }
    
    public GrpcServerBatcherRequest(GrpcTryLockRequest tryLock)
    {
        TryLock = tryLock;
    }
    
    public GrpcServerBatcherRequest(GrpcUnlockRequest unlock)
    {
        Unlock = unlock;
    }
    
    public GrpcServerBatcherRequest(GrpcExtendLockRequest extendLock)
    {
        ExtendLock = extendLock;
    }
    
    public GrpcServerBatcherRequest(GrpcGetLockRequest getLock)
    {
        GetLock = getLock;
    }

    public GrpcServerBatcherRequest(GrpcTrySetKeyValueRequest trySetKeyValue)
    {
        TrySetKeyValue = trySetKeyValue;
    }
    
    public GrpcServerBatcherRequest(GrpcTryGetKeyValueRequest tryGetKeyValue)
    {
        TryGetKeyValue = tryGetKeyValue;
    }
    
    public GrpcServerBatcherRequest(GrpcTryDeleteKeyValueRequest tryDeleteKeyValue)
    {
        TryDeleteKeyValue = tryDeleteKeyValue;
    }
    
    public GrpcServerBatcherRequest(GrpcTryExtendKeyValueRequest tryExtendKeyValue)
    {
        TryExtendKeyValue = tryExtendKeyValue;
    }
    
    public GrpcServerBatcherRequest(GrpcTryExistsKeyValueRequest tryExistsKeyValue)
    {
        TryExistsKeyValue = tryExistsKeyValue;
    }
    
    public GrpcServerBatcherRequest(GrpcTryExecuteTransactionScriptRequest tryExecuteTransactionScript)
    {
        TryExecuteTransactionScript = tryExecuteTransactionScript;
    }
    
    public GrpcServerBatcherRequest(GrpcTryAcquireExclusiveLockRequest tryAcquireExclusiveLock)
    {
        TryAcquireExclusiveLock = tryAcquireExclusiveLock;
    }
    
    public GrpcServerBatcherRequest(GrpcTryAcquireManyExclusiveLocksRequest tryAcquireManyExclusiveLocks)
    {
        TryAcquireManyExclusiveLocks = tryAcquireManyExclusiveLocks;
    }
    
    public GrpcServerBatcherRequest(GrpcTryReleaseExclusiveLockRequest tryReleaseExclusiveLock)
    {
        TryReleaseExclusiveLock = tryReleaseExclusiveLock;
    }
    
    public GrpcServerBatcherRequest(GrpcTryReleaseManyExclusiveLocksRequest tryReleaseManyExclusiveLocks)
    {
        TryReleaseManyExclusiveLocks = tryReleaseManyExclusiveLocks;
    }
    
    public GrpcServerBatcherRequest(GrpcTryPrepareMutationsRequest tryPrepareMutations)
    {
        TryPrepareMutations = tryPrepareMutations;
    }
    
    public GrpcServerBatcherRequest(GrpcTryPrepareManyMutationsRequest tryPrepareManyMutations)
    {
        TryPrepareManyMutations = tryPrepareManyMutations;
    }
    
    public GrpcServerBatcherRequest(GrpcTryCommitMutationsRequest tryCommitMutations)
    {
        TryCommitMutations = tryCommitMutations;
    }
    
    public GrpcServerBatcherRequest(GrpcTryCommitManyMutationsRequest tryCommitManyMutations)
    {
        TryCommitManyMutations = tryCommitManyMutations;
    }
    
    public GrpcServerBatcherRequest(GrpcTryRollbackMutationsRequest tryRollbackMutations)
    {
        TryRollbackMutations = tryRollbackMutations;
    }
    
    public GrpcServerBatcherRequest(GrpcTryRollbackManyMutationsRequest tryRollbackManyMutations)
    {
        TryRollbackManyMutations = tryRollbackManyMutations;
    }
    
    public GrpcServerBatcherRequest(GrpcStartTransactionRequest startTransaction)
    {
        StartTransaction = startTransaction;
    }
    
    public GrpcServerBatcherRequest(GrpcCommitTransactionRequest commitTransaction)
    {
        CommitTransaction = commitTransaction;
    }
    
    public GrpcServerBatcherRequest(GrpcRollbackTransactionRequest rollbackTransaction)
    {
        RollbackTransaction = rollbackTransaction;
    }
}
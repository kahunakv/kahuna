
namespace Kahuna.Server.Communication.Internode.Grpc;

internal sealed class GrpcServerBatcherResponse
{
    public GrpcTrySetKeyValueResponse? TrySetKeyValue { get; }
    
    public GrpcTryGetKeyValueResponse? TryGetKeyValue { get; }
    
    public GrpcTryDeleteKeyValueResponse? TryDeleteKeyValue { get; }
    
    public GrpcTryExtendKeyValueResponse? TryExtendKeyValue { get; }

    public GrpcTryExistsKeyValueResponse? TryExistsKeyValue { get; }
    
    public GrpcTryExecuteTransactionResponse? TryExecuteTransaction { get; }
    
    public GrpcTryAcquireExclusiveLockResponse? TryAcquireExclusiveLock { get; }
    
    public GrpcTryAcquireManyExclusiveLocksResponse? TryAcquireManyExclusiveLocks { get; }
    
    public GrpcTryReleaseExclusiveLockResponse? TryReleaseExclusiveLock { get; }
    
    public GrpcTryReleaseManyExclusiveLocksResponse? TryReleaseManyExclusiveLocks { get; }
    
    public GrpcTryPrepareMutationsResponse? TryPrepareMutations { get; }
    
    public GrpcTryPrepareManyMutationsResponse? TryPrepareManyMutations { get; }
    
    public GrpcTryCommitMutationsResponse? TryCommitMutations { get; }
    
    public GrpcTryCommitManyMutationsResponse? TryCommitManyMutations { get; }

    public GrpcServerBatcherResponse(GrpcTrySetKeyValueResponse trySetKeyValue)
    {
        TrySetKeyValue = trySetKeyValue;
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
    
    public GrpcServerBatcherResponse(GrpcTryExecuteTransactionResponse tryExecuteTransaction)
    {
        TryExecuteTransaction = tryExecuteTransaction;
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
}
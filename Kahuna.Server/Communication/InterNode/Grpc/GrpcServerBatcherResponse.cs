
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
}
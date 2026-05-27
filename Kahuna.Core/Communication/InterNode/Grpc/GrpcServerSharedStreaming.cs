
using Grpc.Core;

namespace Kahuna.Server.Communication.Internode.Grpc;

internal sealed class GrpcServerSharedStreaming
{
    public SemaphoreSlim Semaphore { get; } = new(1, 1);
    
    public AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> LockStreaming { get; }
    
    public AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> KeyValueStreaming { get; }
    
    public GrpcServerSharedStreaming(
        AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> lockStreaming,
        AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> keyValueStreaming
    )
    {
        LockStreaming = lockStreaming;
        KeyValueStreaming = keyValueStreaming;
    }
}
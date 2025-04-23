
using Grpc.Core;

namespace Kahuna.Server.Communication.Internode.Grpc;

internal sealed class GrpcServerSharedStreaming
{
    public SemaphoreSlim Semaphore { get; } = new(1, 1);
    
    public AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> Streaming { get; }
    
    public GrpcServerSharedStreaming(AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> streaming)
    {
        Streaming = streaming;
    }
}
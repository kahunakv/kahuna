
using Grpc.Core;

namespace Kahuna.Server.Communication.Internode.Grpc;

internal sealed class GrpcServerSharedStreaming
{
    public SemaphoreSlim Semaphore { get; } = new(1, 1);
    
    public AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse> Streaming { get; }
    
    public GrpcServerSharedStreaming(AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse> streaming)
    {
        Streaming = streaming;
    }
}
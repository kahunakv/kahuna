
namespace Kahuna.Server.Communication.Internode.Grpc;

internal sealed class GrpcServerBatcherItem
{
    public int RequestId { get; }
    
    /// <summary>
    /// List of logs to store
    /// </summary>
    public GrpcServerBatcherRequest Request { get; }

    /// <summary>
    /// Returns the task completion source of the reply
    /// </summary>
    public TaskCompletionSource<GrpcServerBatcherResponse> Promise { get; }    

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="requestId"></param>
    /// <param name="request"></param>
    /// <param name="promise"></param>
    public GrpcServerBatcherItem(
        int requestId, 
        GrpcServerBatcherRequest request, 
        TaskCompletionSource<GrpcServerBatcherResponse> promise
    )
    {
        RequestId = requestId;
        Request = request;
        Promise = promise;
    }
}

namespace Kahuna.Server.Communication.Internode.Grpc;

/// <summary>
/// Represents an item in the gRPC server batcher, which encapsulates information about a specific request,
/// its type, associated ID, and the task completion source to handle the response.
/// </summary>
internal sealed class GrpcServerBatcherItem
{
    /// <summary>
    /// Batch type
    /// </summary>
    public GrpcServerBatcherItemType Type { get; }
    
    /// <summary>
    /// Unique request id
    /// </summary>
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
    /// <param name="type"></param>
    /// <param name="requestId"></param>
    /// <param name="request"></param>
    /// <param name="promise"></param>
    public GrpcServerBatcherItem(
        GrpcServerBatcherItemType type,
        int requestId, 
        GrpcServerBatcherRequest request, 
        TaskCompletionSource<GrpcServerBatcherResponse> promise
    )
    {
        Type = type;
        RequestId = requestId;
        Request = request;
        Promise = promise;
    }
}
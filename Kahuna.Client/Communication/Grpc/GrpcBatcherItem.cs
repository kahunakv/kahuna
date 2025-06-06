
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Communication;

/// <summary>
/// Represents a single batch item to be processed in a gRPC batching system.
/// This class encapsulates metadata and request/response information
/// for efficient batch processing of different gRPC operation types.
/// </summary>
internal readonly struct GrpcBatcherItem
{
    /// <summary>
    /// Type of batching
    /// </summary>
    public GrpcBatcherItemType Type { get; }
    
    /// <summary>
    /// Unique ID for the request
    /// </summary>
    public int RequestId { get; }
    
    /// <summary>
    /// Request to be sent to the server
    /// </summary>
    public GrpcBatcherRequest Request { get; }

    /// <summary>
    /// Returns the task completion source of the reply
    /// </summary>
    public TaskCompletionSource<GrpcBatcherResponse> Promise { get; }    

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="requestId"></param>
    /// <param name="request"></param>
    /// <param name="promise"></param>
    public GrpcBatcherItem(GrpcBatcherItemType type, int requestId, GrpcBatcherRequest request, TaskCompletionSource<GrpcBatcherResponse> promise)
    {
        Type = type;
        RequestId = requestId;
        Request = request;
        Promise = promise;
    }
}
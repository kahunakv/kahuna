
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Communication;

internal sealed class GrpcBatcherItem
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
    /// List of logs to store
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
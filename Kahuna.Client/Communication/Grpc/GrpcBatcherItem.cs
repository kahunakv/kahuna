
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace Kahuna.Client.Communication;

internal readonly struct GrpcBatcherItem
{
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
    public GrpcBatcherItem(int requestId, GrpcBatcherRequest request, TaskCompletionSource<GrpcBatcherResponse> promise)
    {
        RequestId = requestId;
        Request = request;
        Promise = promise;
    }
}

/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;

namespace Kahuna.Client.Communication;

internal sealed class GrpcSharedStreaming
{
    public SemaphoreSlim Semaphore { get; } = new(1, 1);    
    
    public AsyncDuplexStreamingCall<GrpcBatchClientLockRequest, GrpcBatchClientLockResponse> LockStreaming { get; }
    
    public AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse> KeyValueStreaming { get; }
    
    public GrpcSharedStreaming(
        AsyncDuplexStreamingCall<GrpcBatchClientLockRequest, GrpcBatchClientLockResponse> lockStreaming,
        AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse> keyValueStreaming        
    )
    {
        LockStreaming = lockStreaming;
        KeyValueStreaming = keyValueStreaming;        
    }
}
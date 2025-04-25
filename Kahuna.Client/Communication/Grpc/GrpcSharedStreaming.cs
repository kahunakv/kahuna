
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;

namespace Kahuna.Client.Communication;

/// <summary>
/// The GrpcSharedStreaming class provides functionality to manage gRPC-based shared streaming
/// communication for locking and key-value operations. It encapsulates two duplex streaming calls
/// for handling lock and key-value requests and responses.
/// </summary>
internal sealed class GrpcSharedStreaming
{
    public SemaphoreSlim Semaphore { get; } = new(1, 1);

    /// <summary>
    /// Represents a duplex streaming call for handling lock requests and responses
    /// in a gRPC communication context. It facilitates asynchronous communication
    /// between the client and server for managing distributed locking operations.
    /// </summary>
    public AsyncDuplexStreamingCall<GrpcBatchClientLockRequest, GrpcBatchClientLockResponse> LockStreaming { get; }

    /// <summary>
    /// Represents a duplex streaming call for handling key-value requests and responses
    /// in a gRPC communication context. It enables asynchronous processing of
    /// batched key-value operations between the client and server.
    /// </summary>
    public AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse> KeyValueStreaming { get; }
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="lockStreaming"></param>
    /// <param name="keyValueStreaming"></param>
    public GrpcSharedStreaming(
        AsyncDuplexStreamingCall<GrpcBatchClientLockRequest, GrpcBatchClientLockResponse> lockStreaming,
        AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse> keyValueStreaming        
    )
    {
        LockStreaming = lockStreaming;
        KeyValueStreaming = keyValueStreaming;        
    }
}
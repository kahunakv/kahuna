
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
internal sealed class GrpcSharedStreaming : IDisposable
{
    /// <summary>
    /// Unique identifier for this shared streaming pair.
    /// </summary>
    public long Id { get; }

    /// <summary>
    /// Provides a lightweight synchronization mechanism used to control concurrent access
    /// to shared resources in the context of gRPC-based streaming operations. Ensures that
    /// only one operation writes at a time within the scope of the shared streaming calls.
    /// </summary>
    public SemaphoreSlim Semaphore { get; }

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
    /// <param name="id"></param>
    /// <param name="lockStreaming"></param>
    /// <param name="keyValueStreaming"></param>
    public GrpcSharedStreaming(
        long id,
        AsyncDuplexStreamingCall<GrpcBatchClientLockRequest, GrpcBatchClientLockResponse> lockStreaming,
        AsyncDuplexStreamingCall<GrpcBatchClientKeyValueRequest, GrpcBatchClientKeyValueResponse> keyValueStreaming
    )
    {
        Id = id;
        Semaphore = new(1, 1);
        LockStreaming = lockStreaming;
        KeyValueStreaming = keyValueStreaming;
    }

    /// <summary>
    /// Test-only constructor: injects a pre-configured semaphore without real gRPC connections.
    /// LockStreaming and KeyValueStreaming are null — valid only when the semaphore blocks before
    /// any stream access (e.g. to test cancellation-during-wait behaviour).
    /// </summary>
    internal GrpcSharedStreaming(long id, SemaphoreSlim semaphore)
    {
        Id = id;
        Semaphore = semaphore;
        LockStreaming = null!;
        KeyValueStreaming = null!;
    }

    public void Dispose()
    {
        LockStreaming?.Dispose();
        KeyValueStreaming?.Dispose();
        Semaphore.Dispose();
    }
}

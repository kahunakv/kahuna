
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;
using Kahuna.Shared.Communication.Grpc;

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
    
    // 0 = the node has not said yet, 1 = the node reads request frames, 2 = it does not.
    private int keyValueFrames;

    /// <summary>
    /// Whether the node at the other end of the key-value stream announced that it reads request frames.
    ///
    /// <para>The node says so on the response headers of the stream, which arrive on their own time. This
    /// never waits for them: until they are here the answer is no, and requests travel one per message as
    /// they always did. A node built before frames sends no such header, and it would not answer a frame
    /// at all, so silence must never be read as support. Once the headers have arrived the answer is
    /// final for the life of the stream.</para>
    /// </summary>
    public bool SupportsKeyValueFrames
    {
        get
        {
            int known = Volatile.Read(ref keyValueFrames);

            if (known != 0)
                return known == 1;

            Task<Metadata>? headers = KeyValueStreaming?.ResponseHeadersAsync;

            if (headers is null || !headers.IsCompleted)
                return false;

            bool announced = false;

            // A faulted or cancelled headers task belongs to a call that is going away; reading its
            // exception here keeps it from surfacing later as an unobserved one.
            if (headers.IsCompletedSuccessfully)
                announced = headers.Result.GetValue(ClientBatchFrames.SupportHeader) is not null;
            else
                _ = headers.Exception;

            Volatile.Write(ref keyValueFrames, announced ? 1 : 2);

            return announced;
        }
    }

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

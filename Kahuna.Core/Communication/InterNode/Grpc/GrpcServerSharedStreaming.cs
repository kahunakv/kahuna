using Grpc.Core;

namespace Kahuna.Server.Communication.Internode.Grpc;

/// <summary>
/// One shared duplex-streaming pair (locks + key-values) to a peer, plus the semaphores that
/// serialize writes onto it. Each stream has its own write semaphore: the two streams are
/// separate gRPC calls, so a lock write and a key-value write never touch the same writer and
/// gain nothing from a shared gate — a stalled bulk key-value write must not delay a lock write.
/// Writes are bounded and the pair is disposable so a stream that goes
/// quiet without dying (e.g. the peer was SIGSTOPed and the HTTP/2 session stalled with no error)
/// can be torn down and rebuilt instead of wedging every future forwarded operation.
/// </summary>
internal sealed class GrpcServerSharedStreaming
{
    private int disposed;

    public long Id { get; }

    public SemaphoreSlim LockWriteSemaphore { get; } = new(1, 1);

    public SemaphoreSlim KeyValueWriteSemaphore { get; } = new(1, 1);

    public AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> LockStreaming { get; }

    public AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> KeyValueStreaming { get; }

    public GrpcServerSharedStreaming(
        long id,
        AsyncDuplexStreamingCall<GrpcBatchServerLockRequest, GrpcBatchServerLockResponse> lockStreaming,
        AsyncDuplexStreamingCall<GrpcBatchServerKeyValueRequest, GrpcBatchServerKeyValueResponse> keyValueStreaming
    )
    {
        Id = id;
        LockStreaming = lockStreaming;
        KeyValueStreaming = keyValueStreaming;
    }

    /// <summary>
    /// Cancels and disposes both duplex calls. Idempotent: invalidation can be reached from the
    /// write path (timeout), from either read loop's exit, and from a sibling stream's failure,
    /// all racing each other. Disposal also unblocks an in-flight <c>WriteAsync</c> stuck on a
    /// stalled HTTP/2 window and makes the read loops observe the stream's death.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) == 1)
            return;

        try
        {
            LockStreaming.Dispose();
        }
        catch
        {
            // Disposal of an already-faulted call can throw; the call is unusable either way.
        }

        try
        {
            KeyValueStreaming.Dispose();
        }
        catch
        {
            // Same as above.
        }
    }
}

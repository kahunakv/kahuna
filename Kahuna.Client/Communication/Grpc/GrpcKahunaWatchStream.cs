using System.Runtime.CompilerServices;
using Grpc.Core;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Client.Communication;

/// <summary>
/// One watch on one call of the bidirectional <c>Watch</c> rpc. The call runs on the shared channel to the node;
/// a call is an HTTP/2 stream on that channel, not a connection of its own.
///
/// <para>The rpc can carry many watches; this client opens one watch per call, so a watch's lifetime and its
/// call's lifetime are the same, and a dropped call ends exactly one watch.</para>
/// </summary>
internal sealed class GrpcKahunaWatchStream : IKahunaWatchStream
{
    internal const long WatchId = 1;

    private readonly AsyncDuplexStreamingCall<GrpcWatchRequest, GrpcWatchResponse> call;

    private readonly CancellationTokenSource lifetime;

    // Frames that arrived in the same message as the answer, delivered before the next read.
    private readonly Queue<KeyValueWatchFrame> pending = new();

    private int disposed;

    private GrpcKahunaWatchStream(AsyncDuplexStreamingCall<GrpcWatchRequest, GrpcWatchResponse> call, CancellationTokenSource lifetime)
    {
        this.call = call;
        this.lifetime = lifetime;
    }

    /// <summary>
    /// Opens the watch on <paramref name="client"/> and waits for the node's answer. A transport failure before
    /// the answer is reported as <see cref="KeyValueResponseType.MustRetry"/>: no watch is registered.
    /// </summary>
    public static async Task<KahunaWatchOpening> OpenAsync(KeyValuer.KeyValuerClient client, KeyValueWatchRequest request, CancellationToken cancellationToken)
    {
        CancellationTokenSource lifetime = new();
        GrpcKahunaWatchStream stream = new(client.Watch(cancellationToken: lifetime.Token), lifetime);

        // The caller's token bounds only the opening; the open stream lives until it is disposed.
        CancellationTokenRegistration opening = cancellationToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), lifetime);

        try
        {
            await stream.call.RequestStream.WriteAsync(KeyValueWatchWire.ToGrpcOpen(WatchId, request)).ConfigureAwait(false);

            KeyValueWatchFrame? answer = await stream.ReadAnswerAsync().ConfigureAwait(false);
            if (answer is null)
            {
                await stream.DisposeAsync().ConfigureAwait(false);
                return KahunaWatchOpening.Refused(KeyValueResponseType.MustRetry);
            }

            KahunaWatchOpening result = KahunaWatchOpening.FromAnswer(answer, stream);
            if (result.Stream is null)
                await stream.DisposeAsync().ConfigureAwait(false);

            return result;
        }
        catch (RpcException ex) when (!cancellationToken.IsCancellationRequested && RetryableTransportFailure.IsRetryable(ex))
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            return KahunaWatchOpening.Refused(KeyValueResponseType.MustRetry);
        }
        catch
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            throw;
        }
        finally
        {
            await opening.DisposeAsync().ConfigureAwait(false);
        }
    }

    // The first frame of the watch is its answer. Null when the call ended before it.
    private async Task<KeyValueWatchFrame?> ReadAnswerAsync()
    {
        while (await call.ResponseStream.MoveNext(CancellationToken.None).ConfigureAwait(false))
        {
            KeyValueWatchFrame? answer = null;

            foreach (GrpcWatchFrame wire in call.ResponseStream.Current.Frames)
            {
                if (wire.WatchId != WatchId)
                    continue;

                KeyValueWatchFrame frame = KeyValueWatchWire.FromGrpc(wire);
                if (answer is null)
                    answer = frame;
                else
                    pending.Enqueue(frame);
            }

            if (answer is not null)
                return answer;
        }

        return null;
    }

    public async IAsyncEnumerable<KeyValueWatchFrame> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        while (pending.TryDequeue(out KeyValueWatchFrame? frame))
        {
            yield return frame;
            if (frame.IsTerminal)
                yield break;
        }

        while (true)
        {
            bool moved;

            try
            {
                moved = await call.ResponseStream.MoveNext(cancellationToken).ConfigureAwait(false);
            }
            catch (RpcException) when (Volatile.Read(ref disposed) != 0)
            {
                yield break;
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && cancellationToken.IsCancellationRequested)
            {
                throw new OperationCanceledException(cancellationToken);
            }
            catch (RpcException)
            {
                moved = false;
            }

            if (!moved)
            {
                if (Volatile.Read(ref disposed) == 0)
                    yield return KeyValueWatchFrame.ForLagged(-1, KeyValueWatchLagReason.Disconnected, HLCTimestamp.Zero);

                yield break;
            }

            foreach (GrpcWatchFrame wire in call.ResponseStream.Current.Frames)
            {
                if (wire.WatchId != WatchId)
                    continue;

                KeyValueWatchFrame frame = KeyValueWatchWire.FromGrpc(wire);
                yield return frame;

                if (frame.IsTerminal)
                    yield break;
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;

        // Ending the request stream ends the call on the server, which closes the watch. Cancelling the call
        // covers a server that no longer reads.
        try
        {
            await call.RequestStream.CompleteAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is RpcException or InvalidOperationException or OperationCanceledException or IOException)
        {
        }

        await lifetime.CancelAsync().ConfigureAwait(false);
        call.Dispose();
        lifetime.Dispose();
    }
}

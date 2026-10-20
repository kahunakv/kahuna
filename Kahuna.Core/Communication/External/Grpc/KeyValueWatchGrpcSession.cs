using System.Threading.Channels;

using Grpc.Core;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Watch;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Serves one call of the bidirectional <c>Watch</c> rpc: any number of watches, each named by the watch id the
/// client chose, share the call.
///
/// <para><b>Flow.</b> The request loop opens and cancels watches. Each open watch has a pump that moves its frames
/// into one outbound queue, and a single writer drains the queue into the response stream, several frames per
/// message. The outbound queue is bounded and its producers wait, so a client that reads slowly holds back the
/// pumps; each watch then buffers up to its own capacity and ends with <see cref="KeyValueWatchLagReason.Overflow"/>
/// when the client falls a full capacity behind. The apply path never waits for the network.</para>
///
/// <para><b>Answers.</b> Every open gets exactly one answer before any other frame of that watch: an
/// <see cref="KeyValueWatchFrameType.Opened"/> frame, or a <see cref="KeyValueWatchFrameType.Refused"/> frame
/// with a typed refusal. A failure the shared classifier calls retryable is refused with
/// <see cref="KeyValueResponseType.MustRetry"/> for that watch only; the other watches on the call stay open.</para>
///
/// <para><b>Lifetime.</b> The call ends when the client ends its request stream, when the client goes away
/// (a clean close or an aborted connection), or when a write fails. In every case each watch the call opened is
/// closed and unregistered before the call returns.</para>
/// </summary>
internal sealed class KeyValueWatchGrpcSession
{
    // Frames packed into one response message. Bounds the message size for a burst of small events.
    private const int MaxFramesPerResponse = 256;

    // Frames queued for the writer across every watch of the call.
    private const int OutboundCapacity = 1024;

    private readonly IKahuna kahuna;

    private readonly ILogger logger;

    private readonly Dictionary<long, KeyValueWatch> watches = [];

    private readonly List<Task> pumps = [];

    private readonly Lock sync = new();

    private readonly Channel<GrpcWatchFrame> outbound = Channel.CreateBounded<GrpcWatchFrame>(new BoundedChannelOptions(OutboundCapacity)
    {
        FullMode = BoundedChannelFullMode.Wait,
        SingleReader = true,
        SingleWriter = false
    });

    public KeyValueWatchGrpcSession(IKahuna kahuna, ILogger logger)
    {
        this.kahuna = kahuna;
        this.logger = logger;
    }

    public async Task RunAsync(
        IAsyncStreamReader<GrpcWatchRequest> requests,
        IServerStreamWriter<GrpcWatchResponse> responses,
        CancellationToken callToken)
    {
        using CancellationTokenSource session = CancellationTokenSource.CreateLinkedTokenSource(callToken);
        CancellationToken token = session.Token;

        KeyValueWatchMetrics.RemoteStreams.Add(1, Transport);

        Task writer = WriteLoop(responses, session);

        try
        {
            while (await requests.MoveNext(token).ConfigureAwait(false))
                await Handle(requests.Current, token).ConfigureAwait(false);
        }
        finally
        {
            await session.CancelAsync().ConfigureAwait(false);

            List<KeyValueWatch> open;
            lock (sync)
            {
                open = [.. watches.Values];
                watches.Clear();
            }

            foreach (KeyValueWatch watch in open)
                await watch.DisposeAsync().ConfigureAwait(false);

            outbound.Writer.TryComplete();

            Task[] running;
            lock (sync)
                running = [.. pumps];

            // The pumps and the writer stop on the cancelled session token; their own failures are already
            // logged, and the call is ending either way.
            try
            {
                await Task.WhenAll([.. running, writer]).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is OperationCanceledException or IOException or InvalidOperationException or RpcException)
            {
            }

            KeyValueWatchMetrics.RemoteStreams.Add(-1, Transport);
        }
    }

    private async Task Handle(GrpcWatchRequest request, CancellationToken token)
    {
        if (request.Type == GrpcWatchRequestType.WatchRequestCancel)
        {
            KeyValueWatch? cancelled;
            lock (sync)
                watches.Remove(request.WatchId, out cancelled);

            if (cancelled is not null)
                await cancelled.DisposeAsync().ConfigureAwait(false);

            return;
        }

        long watchId = request.WatchId;

        bool duplicate;
        lock (sync)
            duplicate = watches.ContainsKey(watchId);

        KeyValueWatchScope? scope = KeyValueWatchWire.ScopeFromGrpc(request);

        if (watchId == 0 || duplicate || scope is null)
        {
            await Send(watchId, KeyValueWatchFrame.ForRefused(-1, KeyValueResponseType.InvalidInput), token).ConfigureAwait(false);
            return;
        }

        KeyValueWatchRequest watchRequest = KeyValueWatchWire.RequestFromGrpc(request, scope);

        KeyValueWatchOpening opening;

        try
        {
            opening = await kahuna.WatchKeyValues(watchRequest, token).ConfigureAwait(false);
        }
        catch (Exception ex) when (RetryableFailureClassifier.IsRetryable(ex))
        {
            logger.LogWarning("Mapping retryable {ExceptionType} on Watch to MustRetry: {Message}", ex.GetType().Name, ex.Message);
            await Send(watchId, KeyValueWatchFrame.ForRefused(-1, KeyValueResponseType.MustRetry), token).ConfigureAwait(false);
            return;
        }

        if (opening.Watch is not { } watch)
        {
            await Send(watchId, KeyValueWatchFrame.ForRefused(-1, opening.Type, opening.Routes), token).ConfigureAwait(false);
            return;
        }

        // The answer is queued before the pump starts, so it precedes every frame of the watch.
        try
        {
            await Send(watchId, KeyValueWatchFrame.ForOpened(watch.Partitions), token).ConfigureAwait(false);
        }
        catch
        {
            await watch.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        lock (sync)
        {
            watches[watchId] = watch;

            // A long call opens and closes many watches; keep only the pumps still running.
            pumps.RemoveAll(static pump => pump.IsCompleted);
            pumps.Add(Pump(watchId, watch, token));
        }
    }

    private async Task Pump(long watchId, KeyValueWatch watch, CancellationToken token)
    {
        try
        {
            await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(token).ConfigureAwait(false))
                await Send(watchId, frame, token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is OperationCanceledException or ChannelClosedException)
        {
        }
        finally
        {
            // A watch that ended on its own (a terminal frame) leaves the call; a cancelled one is gone already.
            lock (sync)
            {
                if (watches.TryGetValue(watchId, out KeyValueWatch? current) && ReferenceEquals(current, watch))
                    watches.Remove(watchId);
            }

            await watch.DisposeAsync().ConfigureAwait(false);
        }
    }

    private ValueTask Send(long watchId, KeyValueWatchFrame frame, CancellationToken token) =>
        outbound.Writer.WriteAsync(KeyValueWatchWire.ToGrpc(watchId, frame), token);

    private async Task WriteLoop(IServerStreamWriter<GrpcWatchResponse> responses, CancellationTokenSource session)
    {
        ChannelReader<GrpcWatchFrame> reader = outbound.Reader;
        CancellationToken token = session.Token;

        try
        {
            while (await reader.WaitToReadAsync(token).ConfigureAwait(false))
            {
                GrpcWatchResponse response = new();

                while (response.Frames.Count < MaxFramesPerResponse && reader.TryRead(out GrpcWatchFrame? frame))
                    response.Frames.Add(frame);

                await responses.WriteAsync(response, token).ConfigureAwait(false);
                KeyValueWatchMetrics.RemoteFramesSent.Add(response.Frames.Count, Transport);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            // The client is gone or the transport failed. End the call so the request loop stops and every watch
            // is closed.
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("Watch stream write failed, ending the call: {ExceptionType} {Message}", ex.GetType().Name, ex.Message);

            await session.CancelAsync().ConfigureAwait(false);
        }
    }

    private static readonly KeyValuePair<string, object?> Transport = new("transport", "grpc");
}

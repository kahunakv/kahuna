using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using Microsoft.AspNetCore.Http.Features;

using Kahuna.Server.KeyValues.Watch;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Communication.External.Rest;

/// <summary>
/// Serves <c>POST /v1/kv/watch</c> as a server-sent-events stream: one watch per request. Each frame is one
/// event with a single <c>data</c> line that holds a <see cref="KahunaWatchFrameResponse"/>. The first frame is
/// <c>Opened</c> or <c>Refused</c>; then come the watch's frames, flushed one by one, until a terminal frame or
/// until the client closes the request, which closes and unregisters the watch.
///
/// <para>The watch is opened before the response starts. A retryable failure while it opens therefore reaches the
/// retryable-exception middleware, which answers with the plain JSON MustRetry body, not a stream; a client reads
/// the content type before it reads frames.</para>
///
/// <para>When no frame was written for the keep-alive interval, the handler writes an SSE comment line, which a client
/// skips. A watch with no events otherwise never writes, so a connection whose peer is gone would hold its watch
/// until the node restarts; the write makes such a connection fail, which closes the request and the watch.</para>
/// </summary>
internal static class KeyValueWatchSseHandler
{
    private static readonly KeyValuePair<string, object?> Transport = new("transport", "rest");

    public static async Task Serve(KahunaWatchRequest request, IKahuna keyValues, HttpContext http, TimeSpan keepAlive)
    {
        CancellationToken token = http.RequestAborted;
        KeyValueWatchScope? scope = KeyValueWatchWire.ScopeFromRest(request);

        if (scope is null)
        {
            StartStream(http);
            await WriteFrame(http.Response.BodyWriter, KeyValueWatchFrame.ForRefused(-1, KeyValueResponseType.InvalidInput), token);
            return;
        }

        KeyValueWatchOpening opening = await keyValues.WatchKeyValues(
            KeyValueWatchWire.RequestFromRest(request, scope),
            token);

        StartStream(http);
        PipeWriter writer = http.Response.BodyWriter;

        if (opening.Watch is not { } watch)
        {
            await WriteFrame(writer, KeyValueWatchFrame.ForRefused(-1, opening.Type, opening.Routes), token);
            return;
        }

        KeyValueWatchMetrics.RemoteStreams.Add(1, Transport);

        try
        {
            await using (watch)
            {
                await WriteFrame(writer, KeyValueWatchFrame.ForOpened(watch.Partitions), token);
                await StreamFrames(watch, writer, keepAlive, token);
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException)
        {
            // The client closed the request or the connection dropped; the watch is closed above.
        }
        finally
        {
            KeyValueWatchMetrics.RemoteStreams.Add(-1, Transport);
        }
    }

    private static async Task StreamFrames(KeyValueWatch watch, PipeWriter writer, TimeSpan keepAlive, CancellationToken token)
    {
        IAsyncEnumerator<KeyValueWatchFrame> frames = watch.ReadAllAsync(token).GetAsyncEnumerator(token);

        // A read still in flight when a keep-alive write fails. The enumerator cannot be disposed under it.
        Task<bool>? pending = null;

        try
        {
            while (true)
            {
                ValueTask<bool> next = frames.MoveNextAsync();
                bool more;

                if (next.IsCompleted || keepAlive <= TimeSpan.Zero)
                {
                    more = await next;
                }
                else
                {
                    pending = next.AsTask();

                    while (true)
                    {
                        try
                        {
                            // No token here: the read observes it, so a cancelled request ends the read itself.
                            more = await pending.WaitAsync(keepAlive);
                            break;
                        }
                        catch (TimeoutException)
                        {
                            await WriteKeepAlive(writer, token);
                        }
                    }

                    pending = null;
                }

                if (!more)
                    return;

                await WriteFrame(writer, frames.Current, token);
            }
        }
        finally
        {
            if (pending is not null)
            {
                // Closing the watch completes its channel, so the read in flight returns. Its outcome is of no use
                // once the stream failed: the failure that brought us here is the one the caller sees.
                await watch.DisposeAsync();

                try
                {
                    await pending;
                }
                catch (Exception ex) when (ex is OperationCanceledException or IOException)
                {
                }
            }

            await frames.DisposeAsync();
        }
    }

    private static async ValueTask WriteKeepAlive(PipeWriter writer, CancellationToken token)
    {
        writer.Write(":\n\n"u8);
        await writer.FlushAsync(token);
    }

    private static void StartStream(HttpContext http)
    {
        HttpResponse response = http.Response;
        response.StatusCode = StatusCodes.Status200OK;
        response.ContentType = "text/event-stream";
        response.Headers.CacheControl = "no-cache";

        // Each frame must reach the client when it is flushed, not when a buffer fills.
        http.Features.Get<IHttpResponseBodyFeature>()?.DisableBuffering();
    }

    private static async ValueTask WriteFrame(PipeWriter writer, KeyValueWatchFrame frame, CancellationToken token)
    {
        writer.Write("data: "u8);

        // The serializer writes no line breaks, so the frame stays on one data line.
        using (Utf8JsonWriter json = new(writer))
            JsonSerializer.Serialize(json, KeyValueWatchWire.ToRest(frame), KahunaJsonContext.Default.KahunaWatchFrameResponse);

        writer.Write("\n\n"u8);

        await writer.FlushAsync(token);
        KeyValueWatchMetrics.RemoteFramesSent.Add(1, Transport);
    }
}

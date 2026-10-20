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
/// </summary>
internal static class KeyValueWatchSseHandler
{
    private static readonly KeyValuePair<string, object?> Transport = new("transport", "rest");

    public static async Task Serve(KahunaWatchRequest request, IKahuna keyValues, HttpContext http)
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

                await foreach (KeyValueWatchFrame frame in watch.ReadAllAsync(token))
                    await WriteFrame(writer, frame, token);
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

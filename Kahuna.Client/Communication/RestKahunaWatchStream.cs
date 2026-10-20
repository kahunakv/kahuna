using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Flurl.Http;
using Flurl.Http.Configuration;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Client.Communication;

/// <summary>
/// One watch read from the REST surface's server-sent-events stream (<c>POST /v1/kv/watch</c>). Each event is one
/// <c>data</c> line holding a <see cref="KahunaWatchFrameResponse"/>; blank lines end events and lines that start
/// with <c>':'</c> are comments, both skipped.
/// </summary>
internal sealed class RestKahunaWatchStream : IKahunaWatchStream
{
    private const string EventStreamMediaType = "text/event-stream";

    // A watch lives until it is disposed, so the request must not time out.
    private static readonly Action<FlurlHttpSettings> StreamSettings = settings =>
    {
        KahunaRestJson.Http2Settings(settings);
        settings.Timeout = null;
    };

    private readonly IFlurlResponse response;

    private readonly StreamReader reader;

    private readonly CancellationTokenSource lifetime;

    private int disposed;

    private RestKahunaWatchStream(IFlurlResponse response, StreamReader reader, CancellationTokenSource lifetime)
    {
        this.response = response;
        this.reader = reader;
        this.lifetime = lifetime;
    }

    /// <summary>
    /// Opens the watch on the node at <paramref name="url"/> and waits for its answer. A transport failure before
    /// the answer is reported as <see cref="KeyValueResponseType.MustRetry"/>: no watch is registered.
    /// </summary>
    public static async Task<KahunaWatchOpening> OpenAsync(string url, KeyValueWatchRequest request, CancellationToken cancellationToken)
    {
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(KeyValueWatchWire.ToRest(request), KahunaJsonContext.Default.KahunaWatchRequest);

        CancellationTokenSource lifetime = new();

        // The caller's token bounds only the opening; the open stream lives until it is disposed.
        CancellationTokenRegistration opening = cancellationToken.Register(static state => ((CancellationTokenSource)state!).Cancel(), lifetime);

        IFlurlResponse? response = null;
        RestKahunaWatchStream? stream = null;

        try
        {
            response = await url
                .WithOAuthBearerToken("xxx")
                .AppendPathSegments("v1/kv/watch")
                .WithHeader("Accept", EventStreamMediaType)
                .WithSettings(StreamSettings)
                .PostAsync(new RestCommunication.Utf8JsonContent(payload), HttpCompletionOption.ResponseHeadersRead, lifetime.Token)
                .ConfigureAwait(false);

            Stream body = await response.GetStreamAsync().ConfigureAwait(false);
            stream = new(response, new StreamReader(body, Encoding.UTF8), lifetime);

            // A node that could not start the watch answers with a plain JSON body, not a stream.
            string? mediaType = response.ResponseMessage.Content.Headers.ContentType?.MediaType;
            if (!string.Equals(mediaType, EventStreamMediaType, StringComparison.OrdinalIgnoreCase))
            {
                string text = await stream.reader.ReadToEndAsync(lifetime.Token).ConfigureAwait(false);
                KahunaWatchFrameResponse? refusal = JsonSerializer.Deserialize(text, KahunaJsonContext.Default.KahunaWatchFrameResponse);

                await stream.DisposeAsync().ConfigureAwait(false);
                return KahunaWatchOpening.Refused(refusal?.Type ?? KeyValueResponseType.Errored);
            }

            KeyValueWatchFrame? answer = await stream.ReadFrameAsync(lifetime.Token).ConfigureAwait(false);
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
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested && IsTransportFailure(ex))
        {
            await DisposeOpening(stream, response, lifetime).ConfigureAwait(false);
            return KahunaWatchOpening.Refused(KeyValueResponseType.MustRetry);
        }
        catch
        {
            await DisposeOpening(stream, response, lifetime).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            throw;
        }
        finally
        {
            await opening.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static bool IsTransportFailure(Exception ex) =>
        ex is IOException or HttpRequestException
        || (ex is FlurlHttpException flurl && RestCommunication.IsTransientError(flurl));

    private static async ValueTask DisposeOpening(RestKahunaWatchStream? stream, IFlurlResponse? response, CancellationTokenSource lifetime)
    {
        if (stream is not null)
        {
            await stream.DisposeAsync().ConfigureAwait(false);
            return;
        }

        await lifetime.CancelAsync().ConfigureAwait(false);
        response?.Dispose();
        lifetime.Dispose();
    }

    // The next frame on the stream, or null when the stream ended.
    private async Task<KeyValueWatchFrame?> ReadFrameAsync(CancellationToken cancellationToken)
    {
        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (!line.StartsWith("data:", StringComparison.Ordinal))
                continue;

            KahunaWatchFrameResponse? wire = JsonSerializer.Deserialize(line.AsSpan(5).TrimStart(' '), KahunaJsonContext.Default.KahunaWatchFrameResponse);
            if (wire is not null)
                return KeyValueWatchWire.FromRest(wire);
        }

        return null;
    }

    public async IAsyncEnumerable<KeyValueWatchFrame> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, lifetime.Token);

        while (true)
        {
            KeyValueWatchFrame? frame;

            try
            {
                frame = await ReadFrameAsync(linked.Token).ConfigureAwait(false);
            }
            catch (Exception) when (Volatile.Read(ref disposed) != 0)
            {
                yield break;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex) when (ex is IOException or HttpRequestException or OperationCanceledException)
            {
                frame = null;
            }

            if (frame is null)
            {
                if (Volatile.Read(ref disposed) == 0)
                    yield return KeyValueWatchFrame.ForLagged(-1, KeyValueWatchLagReason.Disconnected, HLCTimestamp.Zero);

                yield break;
            }

            yield return frame;

            if (frame.IsTerminal)
                yield break;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;

        // Aborting the request closes the server's stream, which closes and unregisters the watch.
        await lifetime.CancelAsync().ConfigureAwait(false);
        reader.Dispose();
        response.Dispose();
        lifetime.Dispose();
    }
}

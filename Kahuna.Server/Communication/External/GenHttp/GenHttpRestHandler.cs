
using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

using GenHTTP.Api.Content;
using GenHTTP.Api.Infrastructure;
using GenHTTP.Api.Protocol;

using Kahuna.Communication.External.Rest;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Communication.External.GenHttp;

/// <summary>
/// Serves a subset of the REST surface on the GenHTTP engine: /v1/ping and the key/value try-set,
/// try-get and try-exists routes. The route bodies are the same methods Kestrel's minimal API calls,
/// and the JSON options are the minimal API's own, so the two listeners do the same engine work and
/// write the same bytes; only the HTTP server in front of them differs.
/// </summary>
/// <remarks>
/// The handler dispatches on the raw path bytes rather than through GenHTTP's routing modules, so the
/// comparison measures the engine and not a framework layer that Kahuna would not need.
/// GenHTTP gives a handler no request-aborted signal, so the routes run with
/// <see cref="CancellationToken.None"/>: a client that disconnects does not cancel its operation.
/// </remarks>
internal sealed class GenHttpRestHandler : IHandler
{
    private static readonly byte[] PingBody = "pong"u8.ToArray();

    private static readonly ContentType TextPlain = new("text/plain");

    private readonly IKahuna kahuna;

    private readonly ILogger logger;

    private readonly JsonTypeInfo<KahunaSetKeyValueRequest> setRequestType;

    private readonly JsonTypeInfo<KahunaSetKeyValueResponse> setResponseType;

    private readonly JsonTypeInfo<KahunaGetKeyValueRequest> getRequestType;

    private readonly JsonTypeInfo<KahunaGetKeyValueResponse> getResponseType;

    private readonly JsonTypeInfo<KahunaExistsKeyValueRequest> existsRequestType;

    private readonly JsonTypeInfo<KahunaExistsKeyValueResponse> existsResponseType;

    public GenHttpRestHandler(IKahuna kahuna, JsonSerializerOptions jsonOptions, ILogger logger)
    {
        this.kahuna = kahuna;
        this.logger = logger;

        setRequestType = GetTypeInfo<KahunaSetKeyValueRequest>(jsonOptions);
        setResponseType = GetTypeInfo<KahunaSetKeyValueResponse>(jsonOptions);
        getRequestType = GetTypeInfo<KahunaGetKeyValueRequest>(jsonOptions);
        getResponseType = GetTypeInfo<KahunaGetKeyValueResponse>(jsonOptions);
        existsRequestType = GetTypeInfo<KahunaExistsKeyValueRequest>(jsonOptions);
        existsResponseType = GetTypeInfo<KahunaExistsKeyValueResponse>(jsonOptions);
    }

    public ValueTask PrepareAsync(IServer server) => default;

    public async ValueTask<IResponse?> HandleAsync(IRequest request)
    {
        ReadOnlySpan<byte> path = request.Header.Path.Bytes.Span;

        if (request.Header.Method == RequestMethod.Get && path.SequenceEqual("/v1/ping"u8))
            return Respond(request, ResponseStatus.Ok, PingBody, TextPlain);

        if (request.Header.Method != RequestMethod.Post)
            return Respond(request, ResponseStatus.NotFound, [], TextPlain);

        try
        {
            if (path.SequenceEqual("/v1/kv/try-set"u8))
            {
                KahunaSetKeyValueRequest? body = await ReadBody(request, setRequestType);
                if (body is null)
                    return BadRequest(request);

                return Json(request, await KeyValuesHandlers.TrySet(body, kahuna, CancellationToken.None), setResponseType);
            }

            if (path.SequenceEqual("/v1/kv/try-get"u8))
            {
                KahunaGetKeyValueRequest? body = await ReadBody(request, getRequestType);
                if (body is null)
                    return BadRequest(request);

                return Json(request, await KeyValuesHandlers.TryGet(body, kahuna, CancellationToken.None), getResponseType);
            }

            if (path.SequenceEqual("/v1/kv/try-exists"u8))
            {
                KahunaExistsKeyValueRequest? body = await ReadBody(request, existsRequestType);
                if (body is null)
                    return BadRequest(request);

                return Json(request, await KeyValuesHandlers.TryExists(body, kahuna, CancellationToken.None), existsResponseType);
            }

            return Respond(request, ResponseStatus.NotFound, [], TextPlain);
        }
        catch (JsonException)
        {
            return BadRequest(request);
        }
        catch (Exception ex) when (RetryableExceptionMapping.IsRetryable(ex))
        {
            // Same contract as the Kestrel middleware: every route here is under /v1/kv, so a request
            // that produced no definitive answer gets the key/value surface's MustRetry body.
            if (logger.IsEnabled(LogLevel.Warning))
                logger.LogWarning("Mapping unhandled retryable {ExceptionType} on {Path} to MustRetry: {Message}",
                    ex.GetType().Name, request.Header.Path.ToString(), ex.Message);

            return Json(request, new KahunaSetKeyValueResponse { Type = KeyValueResponseType.MustRetry }, setResponseType);
        }
        catch (Exception ex)
        {
            // An exception that leaves the handler makes GenHTTP drop the connection, so answer 500 here.
            if (logger.IsEnabled(LogLevel.Error))
                logger.LogError(ex, "Unhandled exception on {Path}", request.Header.Path.ToString());

            return Respond(request, ResponseStatus.InternalServerError, [], TextPlain);
        }
    }

    private static async ValueTask<T?> ReadBody<T>(IRequest request, JsonTypeInfo<T> typeInfo)
    {
        IRequestBody? body = request.GetBody();
        if (body is null)
            return default;

        ReadOnlyMemory<byte> bytes = await body.AsMemoryAsync();
        return JsonSerializer.Deserialize(bytes.Span, typeInfo);
    }

    private static IResponse Json<T>(IRequest request, T value, JsonTypeInfo<T> typeInfo) =>
        Respond(request, ResponseStatus.Ok, JsonSerializer.SerializeToUtf8Bytes(value, typeInfo), ContentType.ApplicationJson);

    private static IResponse BadRequest(IRequest request) =>
        Respond(request, ResponseStatus.BadRequest, [], TextPlain);

    private static IResponse Respond(IRequest request, ResponseStatus status, byte[] body, ContentType type) =>
        request.Respond().Status(status).Content(new BytesContent(body, type)).Build();

    private static JsonTypeInfo<T> GetTypeInfo<T>(JsonSerializerOptions options) =>
        (JsonTypeInfo<T>)options.GetTypeInfo(typeof(T));

    /// <summary>A fully serialized body, so the response carries a Content-Length.</summary>
    private sealed class BytesContent(byte[] body, ContentType type) : IResponseContent
    {
        public ulong? Length => (ulong)body.Length;

        public ContentType? Type => type;

        public ReadOnlyMemory<byte>? Encoding => null;

        public ValueTask<ulong?> CalculateChecksumAsync() => new((ulong?)null);

        public ValueTask WriteAsync(IResponseSink sink)
        {
            sink.Writer.Write(body);
            return default;
        }
    }
}


using Grpc.Core;
using Kommander;

namespace Kahuna.Communication.External.Rest;

/// <summary>
/// Last-resort mapping of retryable infrastructure exceptions to a typed MustRetry response on the
/// public REST surface. The locators already map leader-resolution failures, but any remaining
/// escape point — an inter-node gRPC stream dying mid-forward, a Raft path a future change forgets
/// to guard — would otherwise reach the client as an HTTP 500 with a non-JSON body, which no client
/// can classify. Since every mapped condition means "no definitive answer was produced; retrying is
/// how the outcome gets resolved", answering with the surface's own MustRetry code keeps the
/// operation classifiable. Endpoints outside the mapped surfaces (cluster admin, backups) keep
/// their exceptions: those callers are operators, not retry loops.
/// </summary>
public static class RetryableExceptionMapping
{
    /// <summary>
    /// Serialized MustRetry bodies per surface. Responses deserialize into each surface's DTOs
    /// because every response type carries a numeric <c>type</c> field and the remaining fields
    /// default; the values are pinned by tests against the shared enums.
    /// </summary>
    private const string KeyValueMustRetryBody = "{\"type\":101}";
    private const string LockMustRetryBody = "{\"type\":101}";
    private const string SequenceMustRetryBody = "{\"type\":5}";

    public static void UseRetryableExceptionMapping(this WebApplication app)
    {
        app.Use(static async (context, next) =>
        {
            try
            {
                await next(context);
            }
            catch (Exception ex) when (IsRetryable(ex)
                                       && !context.Response.HasStarted
                                       && TryGetMustRetryBody(context.Request.Path) is { } body)
            {
                context.RequestServices.GetRequiredService<ILogger<IKahuna>>().LogWarning(
                    "Mapping unhandled retryable {ExceptionType} on {Path} to MustRetry: {Message}",
                    ex.GetType().Name, context.Request.Path.Value, ex.Message);

                context.Response.StatusCode = StatusCodes.Status200OK;
                context.Response.ContentType = "application/json";
                await context.Response.WriteAsync(body, context.RequestAborted);
            }
        });
    }

    /// <summary>
    /// Retryable = the request never produced a definitive answer: any Raft resolution failure
    /// (missing partition, undecided leader, node not initialized) or an inter-node transport
    /// failure whose status says the remote was unreachable or stopped answering. Cancellation is
    /// deliberately not mapped — the client gave up, there is nobody to answer.
    /// </summary>
    public static bool IsRetryable(Exception ex) => ex switch
    {
        RaftException => true,
        RpcException rpc => rpc.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.Cancelled,
        _ => false,
    };

    /// <summary>Returns the MustRetry response body for the request's surface, or null for
    /// unmapped surfaces (whose exceptions must keep propagating).</summary>
    public static string? TryGetMustRetryBody(PathString path)
    {
        if (path.StartsWithSegments("/v1/kv"))
            return KeyValueMustRetryBody;

        if (path.StartsWithSegments("/v1/locks"))
            return LockMustRetryBody;

        if (path.StartsWithSegments("/v1/sequences"))
            return SequenceMustRetryBody;

        return null;
    }
}

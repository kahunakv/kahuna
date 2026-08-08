
using Grpc.Core;

namespace Kahuna.Client.Communication;

/// <summary>
/// Classifies a gRPC failure as a transport failure the SDK may safely re-drive: the node was
/// unreachable or stopped answering, so no definitive answer was produced.
///
/// <para>Besides the explicit transport statuses, a dead pooled HTTP/2 connection surfaces as
/// <see cref="StatusCode.Internal"/> — a keep-alive ping timeout poisons a pooled connection and the
/// pool keeps handing it out until it is discarded, so these outlive the network fault itself.
/// <see cref="StatusCode.Internal"/> counts only when the failure demonstrably came from the
/// transport layer; a remote application error reports Internal too and must keep propagating, or a
/// genuine server fault would be retried forever.</para>
///
/// <para>The channel's own retry policy cannot express this — service-config retries match on status
/// codes alone, and blanket-retrying Internal is exactly the mistake above. Deliberately a copy of
/// the server-side rule rather than a shared reference: this project depends only on the wire models,
/// and dragging a server assembly into the SDK to share fifteen lines would cost callers far more
/// than the duplication does. A server that answers typed MustRetry never exercises this path, so it
/// only matters against nodes predating that contract.</para>
/// </summary>
internal static class RetryableTransportFailure
{
    public static bool IsRetryable(RpcException ex) =>
        ex.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.Cancelled
        || (ex.StatusCode is StatusCode.Internal && HasTransportCause(ex));

    private static bool HasTransportCause(RpcException ex) =>
        IsTransportException(ex.Status.DebugException) || IsTransportException(ex.InnerException);

    private static bool IsTransportException(Exception? ex) =>
        ex is HttpRequestException or IOException or System.Net.Sockets.SocketException;
}


using Grpc.Core;

namespace Kahuna.Server.Communication.Internode;

/// <summary>
/// Classifies gRPC failures from inter-node forwarding as retryable transport failures — the remote
/// node was unreachable or stopped answering (killed, partitioned away, too slow), so no definitive
/// answer was produced and the caller may safely re-resolve the leader and retry.
///
/// <para>Besides the explicit transport statuses (<see cref="StatusCode.Unavailable"/>,
/// <see cref="StatusCode.DeadlineExceeded"/>, <see cref="StatusCode.Cancelled"/>), a dead HTTP/2
/// connection surfaces as <see cref="StatusCode.Internal"/> — e.g. "Error starting gRPC call" after
/// a keep-alive ping timeout poisons a pooled connection, which the pool keeps handing out until it
/// is discarded, so these can outlive the network fault itself. <see cref="StatusCode.Internal"/> is
/// only retryable when the failure demonstrably came from the transport layer (an
/// <see cref="HttpRequestException"/>, <see cref="IOException"/>, or socket failure underneath);
/// a remote application error also reports Internal and must keep propagating.</para>
/// </summary>
public static class InterNodeTransportFailure
{
    public static bool IsRetryable(RpcException ex) =>
        ex.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.Cancelled
        || (ex.StatusCode is StatusCode.Internal && HasTransportCause(ex));

    /// <summary>True when the Internal status wraps a transport-layer exception rather than a remote
    /// application error. Grpc.Net.Client records the underlying cause as the status's debug
    /// exception (and sometimes as the inner exception), so both are inspected.</summary>
    private static bool HasTransportCause(RpcException ex) =>
        IsTransportException(ex.Status.DebugException) || IsTransportException(ex.InnerException);

    private static bool IsTransportException(Exception? ex) =>
        ex is HttpRequestException or IOException or System.Net.Sockets.SocketException;
}

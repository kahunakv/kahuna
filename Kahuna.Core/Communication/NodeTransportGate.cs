
using Grpc.Core;
using Kommander;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Communication;

/// <summary>
/// Admits a caller to a node-only surface only when it presents a trusted peer certificate.
/// <para>
/// Enforced only in <see cref="RaftNodeAuthenticationMode.MutualTls"/>. In <c>Disabled</c> and
/// <c>SharedSecret</c> mode it admits everyone: there is no certificate to check, and Kahuna's own
/// messages carry no signature.
/// </para>
/// <para>
/// The trust decision is Kommander's <see cref="RaftTransportAuthenticator.ValidatePeerCertificate"/>, the
/// same one <c>RaftService</c> makes, so Raft and Kahuna traffic cannot disagree on who is a peer.
/// The listener policy alone is not enough: every service is mapped on every listener.
/// </para>
/// </summary>
public sealed partial class NodeTransportGate
{
    /// <summary>A gate that admits every caller.</summary>
    public static readonly NodeTransportGate Disabled = new(NullLogger<NodeTransportGate>.Instance);

    private readonly RaftTransportAuthenticator? authenticator;

    private readonly ILogger<NodeTransportGate> logger;

    public NodeTransportGate(RaftTransportAuthenticator authenticator, ILogger<NodeTransportGate> logger)
    {
        this.authenticator = authenticator.Options.NodeAuthenticationMode == RaftNodeAuthenticationMode.MutualTls ? authenticator : null;
        this.logger = logger;
    }

    private NodeTransportGate(ILogger<NodeTransportGate> logger)
    {
        this.logger = logger;
    }

    /// <summary>True when callers must present a trusted peer certificate.</summary>
    public bool IsEnforced => authenticator is not null;

    /// <summary>
    /// Refuses the call with <see cref="StatusCode.Unauthenticated"/> unless the caller is a trusted peer.
    /// Call it before any state is touched. A duplex stream is checked once, before its first read: the
    /// certificate belongs to the connection.
    /// </summary>
    public void RequirePeer(ServerCallContext context)
    {
        if (authenticator is null)
            return;

        HttpContext? httpContext = TryGetHttpContext(context);

        RaftTransportAuthenticationResult result = authenticator.ValidatePeerCertificate(
            httpContext?.Connection.ClientCertificate,
            isSecureTransport: httpContext?.Request.IsHttps ?? false);

        if (result.IsAuthenticated)
            return;

        LogRejected(logger, context.Method, result.Status, httpContext?.Connection.RemoteIpAddress?.ToString() ?? "unknown");

        throw new RpcException(new(StatusCode.Unauthenticated, result.Status.ToString()));
    }

    // No HttpContext means no evidence of TLS or of a certificate, so it fails closed.
    private static HttpContext? TryGetHttpContext(ServerCallContext context)
    {
        try
        {
            return context.GetHttpContext();
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Node-only call {Method} rejected: {Status} (remote {RemoteAddress})")]
    private static partial void LogRejected(ILogger logger, string method, RaftTransportAuthenticationStatus status, string remoteAddress);
}

public static class NodeTransportGateServiceCollectionExtensions
{
    /// <summary>
    /// Registers the gate the Kahuna gRPC services require, built from the registered <see cref="IRaft"/> so
    /// Kahuna and Raft share one trust policy.
    /// </summary>
    public static IServiceCollection AddNodeTransportGate(this IServiceCollection services) =>
        services.AddSingleton(provider => new NodeTransportGate(
            provider.GetRequiredService<IRaft>().Configuration.GetTransportAuthenticator(),
            provider.GetRequiredService<ILogger<NodeTransportGate>>()));
}

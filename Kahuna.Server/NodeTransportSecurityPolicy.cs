
using Kommander;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;

namespace Kahuna.Server;

/// <summary>
/// Builds node-to-node transport security from the command line and enforces the fail-closed
/// startup rules that go with it.
/// <para>
/// Kept out of <c>Program.cs</c> so every rule is unit tested. Each check throws with a message that
/// names the flag that resolves it; nothing warns and continues.
/// </para>
/// </summary>
public static class NodeTransportSecurityPolicy
{
    // These three ports are the standalone port set. scripts/run-standalone.sh, the standalone
    // container image and the kahuna-cli default endpoint all use the same numbers, so a reader who
    // starts a node one way and follows an example written for another way reaches the same node.
    public const int DefaultHttpPort = 8081;

    public const int DefaultHttpsPort = 8082;

    public const int DefaultStandaloneCleartextGrpcPort = 8083;

    /// <summary>
    /// Maps the flags onto the options object that Kommander and Kahuna's inter-node transport share.
    /// Does not load the client certificate; <see cref="Validate"/> does.
    /// </summary>
    public static RaftTransportSecurityOptions Build(KahunaCommandLineOptions opts)
    {
        RaftNodeAuthenticationMode mode = ParseMode(opts.NodeAuthMode);
        bool mutualTls = mode == RaftNodeAuthenticationMode.MutualTls;

        // The HTTPS server certificate doubles as the client certificate unless one is given.
        bool explicitClientCertificate = !string.IsNullOrWhiteSpace(opts.ClientCertificate);

        RaftTransportSecurityOptions security = new()
        {
            NodeAuthenticationMode = mode,
            SharedSecret = string.IsNullOrWhiteSpace(opts.NodeSharedSecret) ? null : opts.NodeSharedSecret,
            RequireTls = opts.NodeRequireTls ?? true,
            AllowInsecureCertificateValidation = opts.RaftAllowInsecureCertificateValidation,
            AllowedClockSkew = TimeSpan.FromSeconds(opts.NodeAuthClockSkew),
            TrustedClientCertificateThumbprints = [.. opts.TrustedClientCertThumbprints ?? []],
            TrustedServerCertificateThumbprints = [.. opts.TrustedServerCertThumbprints ?? []],
            ClientCertificatePath = !mutualTls ? null : explicitClientCertificate ? opts.ClientCertificate : opts.HttpsCertificate,
            ClientCertificatePassword = !mutualTls ? null : explicitClientCertificate ? opts.ClientCertificatePassword : opts.HttpsCertificatePassword
        };

        if (!string.IsNullOrWhiteSpace(opts.NodeAuthHeader))
            security.HeaderName = opts.NodeAuthHeader;

        return security;
    }

    /// <summary>
    /// Refuses configurations that would leave node-to-node traffic unauthenticated while appearing
    /// secured. In <c>MutualTls</c> mode it also loads the client certificate, so a bad path or
    /// password fails here rather than at the first replication.
    /// </summary>
    /// <exception cref="KahunaServerException">A rule is violated.</exception>
    /// <exception cref="RaftException">The client certificate cannot be loaded.</exception>
    public static void Validate(KahunaCommandLineOptions opts, RaftTransportSecurityOptions security)
    {
        if (opts.NodeAuthClockSkew < 0)
            throw new KahunaServerException("--node-auth-clock-skew must be zero or positive");

        if (security.NodeAuthenticationMode == RaftNodeAuthenticationMode.SharedSecret
            && string.IsNullOrWhiteSpace(security.SharedSecret)
            && string.IsNullOrWhiteSpace(opts.RaftHttpAuthBearerToken))
            throw new KahunaServerException("--node-shared-secret must be set when --node-auth-mode is SharedSecret");

        if (security.NodeAuthenticationMode != RaftNodeAuthenticationMode.MutualTls)
            return;

        if (string.IsNullOrWhiteSpace(opts.HttpsCertificate))
            throw new KahunaServerException("--https-certificate must be set when --node-auth-mode is MutualTls: peers need a server certificate to connect to");

        if (opts.RaftAllowInsecureCertificateValidation)
            throw new KahunaServerException(
                "--raft-allow-insecure-certificate-validation cannot be combined with --node-auth-mode MutualTls: " +
                "it disables the peer validation that mutual TLS depends on");

        // The cluster listener accepts any certificate at the TLS layer, so an empty allow-list would trust
        // every certificate that completes a handshake.
        if (!HasUsableThumbprint(security.TrustedClientCertificateThumbprints))
            throw new KahunaServerException(
                "--node-auth-mode MutualTls requires at least one --trusted-client-cert-thumbprint: " +
                "an empty allow-list trusts any self-signed certificate");

        if (!GetHttpsPorts(opts).Contains(opts.RaftPort))
            throw new KahunaServerException(
                $"--raft-port {opts.RaftPort} must be one of the --https-ports when --node-auth-mode is MutualTls: " +
                "a Raft port on a cleartext listener bypasses mutual TLS");

        if (!IsHttpsScheme(opts.RaftGrpcScheme))
            throw new KahunaServerException($"--raft-grpc-scheme must be https:// when --node-auth-mode is MutualTls (got '{opts.RaftGrpcScheme}')");

        if (!IsHttpsScheme(opts.RaftHttpScheme))
            throw new KahunaServerException($"--raft-http-scheme must be https:// when --node-auth-mode is MutualTls (got '{opts.RaftHttpScheme}')");

        // A hint derived from a Raft endpoint sends application clients to the cluster listener, where
        // their handshake fails for lack of a client certificate.
        if (!opts.DisableRoutingHints
            && (string.IsNullOrWhiteSpace(opts.AdvertisedClientEndpoint) || !opts.DisablePeerEndpointAdvertisement))
            throw new KahunaServerException(
                "--node-auth-mode MutualTls with routing hints requires --advertised-client-endpoint (an application " +
                "listener) and --disable-peer-endpoint-advertisement; otherwise hints name the cluster listener. " +
                "Pass --disable-routing-hints to turn hints off instead");

        if (!opts.AllowPlaintextListener && (HasAny(opts.HttpPorts) || HasAny(opts.GrpcCleartextPorts)))
            throw new KahunaServerException(
                "--http-ports and --grpc-cleartext-ports are cleartext listeners; with --node-auth-mode MutualTls " +
                "pass --allow-plaintext-listener to bind them anyway");

        security.GetClientCertificate();
    }

    /// <summary>
    /// Refuses a clustered node whose Raft port sits on a cleartext listener that is not bound because
    /// TLS is configured; that node would silently drop out of the cluster. A Raft port on no local
    /// listener at all is left alone, since it can be an external (NAT) port.
    /// </summary>
    public static void ValidateRaftPortListener(KahunaCommandLineOptions opts, bool httpsConfigured)
    {
        if (ShouldBindPlaintextListeners(httpsConfigured, opts.AllowPlaintextListener))
            return;

        if (GetHttpsPorts(opts).Contains(opts.RaftPort))
            return;

        if (!GetHttpPorts(opts).Contains(opts.RaftPort)
            && !ParsePorts(opts.GrpcCleartextPorts, "--grpc-cleartext-ports").Contains(opts.RaftPort))
            return;

        throw new KahunaServerException(
            $"--raft-port {opts.RaftPort} is on a cleartext listener, which is not bound when an HTTPs certificate " +
            "is configured. Add it to --https-ports, or pass --allow-plaintext-listener");
    }

    /// <summary>
    /// Cleartext listeners are the only transport when no certificate is configured. Beside TLS they are
    /// an explicit opt-in.
    /// </summary>
    public static bool ShouldBindPlaintextListeners(bool httpsConfigured, bool allowPlaintextListener) =>
        !httpsConfigured || allowPlaintextListener;

    /// <summary>True for the listener that must demand a client certificate: the Raft port under mTLS.</summary>
    public static bool IsClusterListener(RaftTransportSecurityOptions security, int port, int raftPort) =>
        security.NodeAuthenticationMode == RaftNodeAuthenticationMode.MutualTls && port == raftPort;

    /// <summary>
    /// The cluster listener asks for the certificate in the initial handshake, which rules out HTTP/3.
    /// </summary>
    public static HttpProtocols GetHttpsProtocols(bool clusterListener) =>
        clusterListener ? HttpProtocols.Http1AndHttp2 : HttpProtocols.Http1AndHttp2AndHttp3;

    /// <summary>
    /// Requires a client certificate on the cluster listener and accepts it at the TLS layer; the trust
    /// decision is the thumbprint allow-list, which Kestrel's chain validation would never reach for
    /// self-signed per-node certificates. Other listeners are left untouched.
    /// </summary>
    public static void ConfigureClientCertificate(HttpsConnectionAdapterOptions httpsOptions, bool clusterListener)
    {
        if (!clusterListener)
            return;

        httpsOptions.ClientCertificateMode = ClientCertificateMode.RequireCertificate;
        httpsOptions.ClientCertificateValidation = static (_, _, _) => true;
    }

    public static IReadOnlyList<int> GetHttpPorts(KahunaCommandLineOptions opts)
    {
        IReadOnlyList<int> ports = ParsePorts(opts.HttpPorts, "--http-ports");
        return ports.Count == 0 ? [DefaultHttpPort] : ports;
    }

    public static IReadOnlyList<int> GetHttpsPorts(KahunaCommandLineOptions opts)
    {
        IReadOnlyList<int> ports = ParsePorts(opts.HttpsPorts, "--https-ports");
        return ports.Count == 0 ? [DefaultHttpsPort] : ports;
    }

    public static IReadOnlyList<int> ParsePorts(IEnumerable<string>? values, string optionName)
    {
        List<int> ports = [];

        foreach (string value in values ?? [])
        {
            if (!int.TryParse(value, out int port) || port is < 0 or > 65535)
                throw new KahunaServerException($"Invalid {optionName} value '{value}': expected a TCP port between 0 and 65535");

            ports.Add(port);
        }

        return ports;
    }

    private static RaftNodeAuthenticationMode ParseMode(string? value)
    {
        // Enum.TryParse also accepts numbers, which would let "7" through as an undefined mode.
        if (string.IsNullOrWhiteSpace(value)
            || !Enum.TryParse(value, ignoreCase: true, out RaftNodeAuthenticationMode mode)
            || !Enum.IsDefined(mode)
            || char.IsAsciiDigit(value.TrimStart()[0]))
            throw new KahunaServerException($"Unknown --node-auth-mode value '{value}': expected Disabled, SharedSecret or MutualTls");

        return mode;
    }

    private static bool IsHttpsScheme(string? scheme) =>
        string.Equals(scheme, "https://", StringComparison.OrdinalIgnoreCase);

    private static bool HasAny(IEnumerable<string>? values) => values is not null && values.Any();

    /// <summary>A value such as ":::" normalizes to nothing, so raw entries are not counted.</summary>
    private static bool HasUsableThumbprint(IReadOnlyCollection<string> thumbprints)
    {
        foreach (string thumbprint in thumbprints)
        {
            foreach (char c in thumbprint)
            {
                if (char.IsAsciiLetterOrDigit(c))
                    return true;
            }
        }

        return false;
    }
}

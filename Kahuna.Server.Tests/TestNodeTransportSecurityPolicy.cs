
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using CommandLine;
using Kommander;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.AspNetCore.Server.Kestrel.Https;

namespace Kahuna.Server.Tests;

/// <summary>
/// The command line reaches the transport-security options, and every fail-closed startup rule refuses
/// what it exists to refuse.
/// </summary>
public sealed class TestNodeTransportSecurityPolicy
{
    private static readonly string[] ClientThumbprints = ["AA", "BB"];

    private static readonly string[] ServerThumbprints = ["CC"];

    private const string Thumbprint = "AB:CD:EF:01:23:45:67:89:AB:CD:EF:01:23:45:67:89:AB:CD:EF:01:23:45:67:89:AB:CD:EF:01:23:45:67:89";

    [Fact]
    public void EveryFlagParsesIntoTheTransportSecurityOptions()
    {
        KahunaCommandLineOptions opts = Parse(
            "--node-auth-mode", "mutualtls",
            "--node-shared-secret", "secret",
            "--node-auth-header", "X-Test-Auth",
            "--node-require-tls", "false",
            "--node-auth-clock-skew", "15",
            "--raft-allow-insecure-certificate-validation",
            "--client-certificate", "/certs/client.pfx",
            "--client-certificate-password", "client-pass",
            "--https-certificate", "/certs/server.pfx",
            "--https-certificate-password", "server-pass",
            "--trusted-client-cert-thumbprint", "AA", "BB",
            "--trusted-server-cert-thumbprint", "CC");

        RaftTransportSecurityOptions security = NodeTransportSecurityPolicy.Build(opts);

        Assert.Equal(RaftNodeAuthenticationMode.MutualTls, security.NodeAuthenticationMode);
        Assert.Equal("secret", security.SharedSecret);
        Assert.Equal("X-Test-Auth", security.HeaderName);
        Assert.False(security.RequireTls);
        Assert.Equal(TimeSpan.FromSeconds(15), security.AllowedClockSkew);
        Assert.True(security.AllowInsecureCertificateValidation);
        Assert.Equal("/certs/client.pfx", security.ClientCertificatePath);
        Assert.Equal("client-pass", security.ClientCertificatePassword);
        Assert.Equal(ClientThumbprints, security.TrustedClientCertificateThumbprints);
        Assert.Equal(ServerThumbprints, security.TrustedServerCertificateThumbprints);
    }

    [Fact]
    public void DefaultsLeaveKommanderDefaultsInPlace()
    {
        RaftTransportSecurityOptions security = NodeTransportSecurityPolicy.Build(Parse());
        RaftTransportSecurityOptions kommanderDefaults = new();

        Assert.Equal(RaftNodeAuthenticationMode.Disabled, security.NodeAuthenticationMode);
        Assert.Null(security.SharedSecret);
        Assert.Equal(kommanderDefaults.HeaderName, security.HeaderName);
        Assert.True(security.RequireTls);
        Assert.Equal(kommanderDefaults.AllowedClockSkew, security.AllowedClockSkew);
        Assert.Empty(security.TrustedClientCertificateThumbprints);
        Assert.Empty(security.TrustedServerCertificateThumbprints);
        Assert.Null(security.ClientCertificatePath);
    }

    [Fact]
    public void FlagsReachTheRaftConfigurationUsedByTheNode()
    {
        KahunaCommandLineOptions opts = Parse("--node-auth-mode", "SharedSecret", "--node-shared-secret", "s3cret");
        RaftTransportSecurityOptions security = NodeTransportSecurityPolicy.Build(opts);

        EmbeddedKahunaOptions embedded = EmbeddedOptionsFactory.CreateEmbeddedOptions(opts, security);

        Assert.Same(security, embedded.TransportSecurity);
    }

    [Fact]
    public void MutualTlsPresentsTheHttpsCertificateWhenNoClientCertificateIsGiven()
    {
        RaftTransportSecurityOptions security = NodeTransportSecurityPolicy.Build(Parse(
            "--node-auth-mode", "MutualTls",
            "--https-certificate", "/certs/server.pfx",
            "--https-certificate-password", "server-pass"));

        Assert.Equal("/certs/server.pfx", security.ClientCertificatePath);
        Assert.Equal("server-pass", security.ClientCertificatePassword);
    }

    [Fact]
    public void OnlyMutualTlsPresentsAClientCertificate()
    {
        RaftTransportSecurityOptions security = NodeTransportSecurityPolicy.Build(Parse(
            "--node-auth-mode", "SharedSecret",
            "--node-shared-secret", "s",
            "--client-certificate", "/certs/client.pfx",
            "--https-certificate", "/certs/server.pfx"));

        Assert.Null(security.ClientCertificatePath);
        Assert.Null(security.ClientCertificatePassword);
    }

    [Theory]
    [InlineData("Kerberos")]
    [InlineData("7")]
    [InlineData("")]
    public void UnknownModeIsRefused(string mode)
    {
        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            NodeTransportSecurityPolicy.Build(new KahunaCommandLineOptions { NodeAuthMode = mode }));

        Assert.Contains("--node-auth-mode", ex.Message);
    }

    [Fact]
    public void ValidMutualTlsConfigurationPassesAndLoadsTheCertificate()
    {
        using TempPfx pfx = TempPfx.Create("valid-pass");
        KahunaCommandLineOptions opts = ValidMutualTls(pfx);
        RaftTransportSecurityOptions security = NodeTransportSecurityPolicy.Build(opts);

        NodeTransportSecurityPolicy.Validate(opts, security);

        X509Certificate2? loaded = security.GetClientCertificate();
        Assert.NotNull(loaded);
        Assert.True(loaded.HasPrivateKey);
    }

    [Fact]
    public void WrongCertificatePasswordFailsAtStartupNamingTheFile()
    {
        using TempPfx pfx = TempPfx.Create("right-pass");
        KahunaCommandLineOptions opts = ValidMutualTls(pfx);
        opts.HttpsCertificatePassword = "wrong-pass";

        RaftException ex = Assert.Throws<RaftException>(() =>
            NodeTransportSecurityPolicy.Validate(opts, NodeTransportSecurityPolicy.Build(opts)));

        Assert.Contains(pfx.Path, ex.Message);
    }

    public static TheoryData<string, string> MutualTlsRefusals() => new()
    {
        { "no-https-certificate", "--https-certificate" },
        { "insecure-validation", "--raft-allow-insecure-certificate-validation" },
        { "no-thumbprint", "--trusted-client-cert-thumbprint" },
        { "separator-only-thumbprint", "--trusted-client-cert-thumbprint" },
        { "raft-port-not-https", "--raft-port" },
        { "raft-port-not-default-https", "--raft-port" },
        { "cleartext-grpc-scheme", "--raft-grpc-scheme" },
        { "cleartext-http-scheme", "--raft-http-scheme" },
        { "hints-without-advertised-endpoint", "--advertised-client-endpoint" },
        { "hints-advertising-peers", "--disable-peer-endpoint-advertisement" },
        { "http-ports-without-opt-in", "--allow-plaintext-listener" },
        { "cleartext-grpc-ports-without-opt-in", "--allow-plaintext-listener" }
    };

    [Theory]
    [MemberData(nameof(MutualTlsRefusals))]
    public void MutualTlsRefusesUnsafeConfigurations(string violation, string namedFlag)
    {
        // The certificate path does not exist: every refusal must fire before the certificate is loaded.
        KahunaCommandLineOptions opts = ValidMutualTls("/nonexistent/certificate.pfx");

        switch (violation)
        {
            case "no-https-certificate": opts.HttpsCertificate = ""; break;
            case "insecure-validation": opts.RaftAllowInsecureCertificateValidation = true; break;
            case "no-thumbprint": opts.TrustedClientCertThumbprints = []; break;
            case "separator-only-thumbprint": opts.TrustedClientCertThumbprints = [":::", " "]; break;
            case "raft-port-not-https": opts.RaftPort = 9999; break;
            case "raft-port-not-default-https": opts.HttpsPorts = []; opts.RaftPort = 8082; break;
            case "cleartext-grpc-scheme": opts.RaftGrpcScheme = "http://"; break;
            case "cleartext-http-scheme": opts.RaftHttpScheme = "http://"; break;
            case "hints-without-advertised-endpoint": opts.AdvertisedClientEndpoint = ""; break;
            case "hints-advertising-peers": opts.DisablePeerEndpointAdvertisement = false; break;
            case "http-ports-without-opt-in": opts.HttpPorts = ["2070"]; break;
            case "cleartext-grpc-ports-without-opt-in": opts.GrpcCleartextPorts = ["2072"]; break;
            default: throw new ArgumentOutOfRangeException(nameof(violation));
        }

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            NodeTransportSecurityPolicy.Validate(opts, NodeTransportSecurityPolicy.Build(opts)));

        Assert.Contains(namedFlag, ex.Message);
    }

    [Fact]
    public void MutualTlsAcceptsCleartextListenersWithTheOptIn()
    {
        using TempPfx pfx = TempPfx.Create("pass");
        KahunaCommandLineOptions opts = ValidMutualTls(pfx);
        opts.HttpPorts = ["2070"];
        opts.GrpcCleartextPorts = ["2072"];
        opts.AllowPlaintextListener = true;

        NodeTransportSecurityPolicy.Validate(opts, NodeTransportSecurityPolicy.Build(opts));
    }

    [Fact]
    public void MutualTlsWithRoutingHintsOffNeedsNoAdvertisedEndpoint()
    {
        using TempPfx pfx = TempPfx.Create("pass");
        KahunaCommandLineOptions opts = ValidMutualTls(pfx);
        opts.AdvertisedClientEndpoint = "";
        opts.DisablePeerEndpointAdvertisement = false;
        opts.DisableRoutingHints = true;

        NodeTransportSecurityPolicy.Validate(opts, NodeTransportSecurityPolicy.Build(opts));
    }

    [Fact]
    public void SharedSecretWithoutASecretIsRefused()
    {
        KahunaCommandLineOptions opts = Parse("--node-auth-mode", "SharedSecret");

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            NodeTransportSecurityPolicy.Validate(opts, NodeTransportSecurityPolicy.Build(opts)));

        Assert.Contains("--node-shared-secret", ex.Message);
    }

    [Fact]
    public void NegativeClockSkewIsRefused()
    {
        KahunaCommandLineOptions opts = new() { NodeAuthClockSkew = -1 };

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            NodeTransportSecurityPolicy.Validate(opts, NodeTransportSecurityPolicy.Build(opts)));

        Assert.Contains("--node-auth-clock-skew", ex.Message);
    }

    [Fact]
    public void DisabledModeNeedsNothing()
    {
        KahunaCommandLineOptions opts = Parse("--http-ports", "2070", "--grpc-cleartext-ports", "2072", "--raft-grpc-scheme", "http://");

        NodeTransportSecurityPolicy.Validate(opts, NodeTransportSecurityPolicy.Build(opts));
    }

    [Theory]
    [InlineData(false, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, true, true)]
    public void CleartextListenersBesideTlsAreAnOptIn(bool httpsConfigured, bool allowPlaintext, bool expected)
    {
        Assert.Equal(expected, NodeTransportSecurityPolicy.ShouldBindPlaintextListeners(httpsConfigured, allowPlaintext));
    }

    [Fact]
    public void RaftPortOnAnUnboundCleartextListenerIsRefused()
    {
        KahunaCommandLineOptions opts = Parse("--https-certificate", "/certs/server.pfx", "--https-ports", "2071", "--raft-port", "2070");

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            NodeTransportSecurityPolicy.ValidateRaftPortListener(opts, httpsConfigured: true));

        Assert.Contains("--allow-plaintext-listener", ex.Message);
    }

    [Fact]
    public void RaftPortListenerCheckAcceptsServedOrExternalPorts()
    {
        // On an HTTPS listener.
        NodeTransportSecurityPolicy.ValidateRaftPortListener(
            Parse("--https-certificate", "/certs/server.pfx", "--https-ports", "2071", "8082", "--raft-port", "8082"), httpsConfigured: true);

        // On the cleartext listener, which the opt-in keeps bound.
        NodeTransportSecurityPolicy.ValidateRaftPortListener(
            Parse("--https-certificate", "/certs/server.pfx", "--allow-plaintext-listener", "--raft-port", "2070"), httpsConfigured: true);

        // On no local listener: an external (NAT) port is not judged.
        NodeTransportSecurityPolicy.ValidateRaftPortListener(
            Parse("--https-certificate", "/certs/server.pfx", "--https-ports", "2071", "--raft-port", "9000"), httpsConfigured: true);
    }

    [Fact]
    public void OnlyTheRaftPortUnderMutualTlsIsTheClusterListener()
    {
        RaftTransportSecurityOptions mutualTls = new() { NodeAuthenticationMode = RaftNodeAuthenticationMode.MutualTls };
        RaftTransportSecurityOptions disabled = new();

        Assert.True(NodeTransportSecurityPolicy.IsClusterListener(mutualTls, 8082, 8082));
        Assert.False(NodeTransportSecurityPolicy.IsClusterListener(mutualTls, 2071, 8082));
        Assert.False(NodeTransportSecurityPolicy.IsClusterListener(disabled, 8082, 8082));
    }

    [Fact]
    public void ClusterListenerRequiresACertificateAndDropsHttp3()
    {
        HttpsConnectionAdapterOptions cluster = new();
        NodeTransportSecurityPolicy.ConfigureClientCertificate(cluster, clusterListener: true);

        Assert.Equal(ClientCertificateMode.RequireCertificate, cluster.ClientCertificateMode);
        Assert.NotNull(cluster.ClientCertificateValidation);
        Assert.Equal(HttpProtocols.Http1AndHttp2, NodeTransportSecurityPolicy.GetHttpsProtocols(clusterListener: true));
    }

    [Fact]
    public void ApplicationListenerIsLeftAsBefore()
    {
        HttpsConnectionAdapterOptions application = new();
        HttpsConnectionAdapterOptions untouched = new();
        NodeTransportSecurityPolicy.ConfigureClientCertificate(application, clusterListener: false);

        Assert.Equal(untouched.ClientCertificateMode, application.ClientCertificateMode);
        Assert.Null(application.ClientCertificateValidation);
        Assert.Equal(HttpProtocols.Http1AndHttp2AndHttp3, NodeTransportSecurityPolicy.GetHttpsProtocols(clusterListener: false));
    }

    [Fact]
    public void InvalidPortIsRefusedNamingTheFlag()
    {
        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            NodeTransportSecurityPolicy.GetHttpsPorts(new KahunaCommandLineOptions { HttpsPorts = ["80a"] }));

        Assert.Contains("--https-ports", ex.Message);
    }

    private static KahunaCommandLineOptions Parse(params string[] args)
    {
        ParserResult<KahunaCommandLineOptions> result = Parser.Default.ParseArguments<KahunaCommandLineOptions>(args);

        Assert.Equal(ParserResultType.Parsed, result.Tag);
        return result.Value;
    }

    private static KahunaCommandLineOptions ValidMutualTls(TempPfx pfx)
    {
        KahunaCommandLineOptions opts = ValidMutualTls(pfx.Path);
        opts.HttpsCertificatePassword = pfx.Password;
        return opts;
    }

    private static KahunaCommandLineOptions ValidMutualTls(string certificatePath) => Parse(
        "--node-auth-mode", "MutualTls",
        "--https-certificate", certificatePath,
        "--https-ports", "2071", "8082",
        "--raft-port", "8082",
        "--trusted-client-cert-thumbprint", Thumbprint,
        "--advertised-client-endpoint", "https://node1:2071",
        "--disable-peer-endpoint-advertisement");

    private sealed class TempPfx : IDisposable
    {
        public string Path { get; }

        public string Password { get; }

        private TempPfx(string path, string password)
        {
            Path = path;
            Password = password;
        }

        public static TempPfx Create(string password)
        {
            using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
            CertificateRequest request = new("CN=kahuna-policy-test", key, HashAlgorithmName.SHA256);
            using X509Certificate2 certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));

            string path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"kahuna-policy-{Guid.NewGuid():N}.pfx");
            File.WriteAllBytes(path, certificate.Export(X509ContentType.Pkcs12, password));
            return new(path, password);
        }

        public void Dispose() => File.Delete(Path);
    }
}

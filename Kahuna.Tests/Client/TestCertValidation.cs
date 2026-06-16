
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Kahuna.Client;
using Kahuna.Client.Communication;
using Xunit;

namespace Kahuna.Tests.Client;

/// <summary>
/// Hermetic unit tests for GrpcBatcher.BuildCertValidationCallback and MakeCacheKey.
/// No network, no Docker, no cluster required.
/// </summary>
public class TestCertValidation
{
    // Convenience: invoke a RemoteCertificateValidationCallback with a cert and no chain/errors.
    private static bool Invoke(RemoteCertificateValidationCallback cb, X509Certificate? cert)
        => cb(null!, cert, null, SslPolicyErrors.None);

    // Create a minimal self-signed cert whose raw bytes we can hash.
    private static X509Certificate2 MakeSelfSignedCert()
    {
        using RSA rsa = RSA.Create(2048);
        CertificateRequest req = new("CN=kahuna-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    }

    private static string Thumbprint(X509Certificate2 cert)
    {
        byte[] hash = SHA256.HashData(cert.RawData);
        return Convert.ToHexString(hash);
    }

    // --- BuildCertValidationCallback ---

    [Fact]
    public void BuildCertValidationCallback_ReturnsNull_ForNullOptions()
    {
        RemoteCertificateValidationCallback? cb = GrpcBatcher.BuildCertValidationCallback(null);
        Assert.Null(cb);
    }

    [Fact]
    public void BuildCertValidationCallback_ReturnsNull_WhenNeitherFlagSet()
    {
        KahunaOptions opts = new(); // AllowInsecure=false, Thumbprints=[]
        RemoteCertificateValidationCallback? cb = GrpcBatcher.BuildCertValidationCallback(opts);
        Assert.Null(cb);
    }

    [Fact]
    public void BuildCertValidationCallback_AcceptsAll_WhenInsecureFlagSet()
    {
        KahunaOptions opts = new() { AllowInsecureCertificateValidation = true };
        RemoteCertificateValidationCallback? cb = GrpcBatcher.BuildCertValidationCallback(opts);

        Assert.NotNull(cb);
        // Must accept any cert — including null — when insecure flag is on.
        Assert.True(Invoke(cb, null));
        using X509Certificate2 cert = MakeSelfSignedCert();
        Assert.True(Invoke(cb, cert));
    }

    [Fact]
    public void BuildCertValidationCallback_AcceptsCert_WhenThumbprintMatches()
    {
        using X509Certificate2 cert = MakeSelfSignedCert();
        string pin = Thumbprint(cert);

        KahunaOptions opts = new() { TrustedServerCertificateThumbprints = [pin] };
        RemoteCertificateValidationCallback? cb = GrpcBatcher.BuildCertValidationCallback(opts);

        Assert.NotNull(cb);
        Assert.True(Invoke(cb, cert));
    }

    [Fact]
    public void BuildCertValidationCallback_AcceptsCert_WhenThumbprintMatchesCaseInsensitive()
    {
        using X509Certificate2 cert = MakeSelfSignedCert();
        string pin = Thumbprint(cert).ToLowerInvariant(); // stored lower-case

        KahunaOptions opts = new() { TrustedServerCertificateThumbprints = [pin] };
        RemoteCertificateValidationCallback? cb = GrpcBatcher.BuildCertValidationCallback(opts);

        Assert.NotNull(cb);
        Assert.True(Invoke(cb, cert));
    }

    [Fact]
    public void BuildCertValidationCallback_RejectsCert_WhenThumbprintDoesNotMatch()
    {
        using X509Certificate2 cert = MakeSelfSignedCert();
        string wrongPin = new string('A', 64); // 32-byte hex that won't match

        KahunaOptions opts = new() { TrustedServerCertificateThumbprints = [wrongPin] };
        RemoteCertificateValidationCallback? cb = GrpcBatcher.BuildCertValidationCallback(opts);

        Assert.NotNull(cb);
        Assert.False(Invoke(cb, cert));
    }

    [Fact]
    public void BuildCertValidationCallback_RejectsNull_WhenThumbprintsConfigured()
    {
        KahunaOptions opts = new() { TrustedServerCertificateThumbprints = ["AABBCC"] };
        RemoteCertificateValidationCallback? cb = GrpcBatcher.BuildCertValidationCallback(opts);

        Assert.NotNull(cb);
        Assert.False(Invoke(cb, null)); // null cert → false, not a NullReferenceException
    }

    // --- MakeCacheKey ---

    [Fact]
    public void MakeCacheKey_ReturnsUrl_ForNullOptions()
    {
        Assert.Equal("https://host:1234", GrpcBatcher.MakeCacheKey("https://host:1234", null));
    }

    [Fact]
    public void MakeCacheKey_ReturnsUrl_WhenNeitherFlagSet()
    {
        KahunaOptions opts = new();
        Assert.Equal("https://host:1234", GrpcBatcher.MakeCacheKey("https://host:1234", opts));
    }

    [Fact]
    public void MakeCacheKey_AppendsInsecureSuffix()
    {
        KahunaOptions opts = new() { AllowInsecureCertificateValidation = true };
        string key = GrpcBatcher.MakeCacheKey("https://host:1234", opts);
        Assert.StartsWith("https://host:1234\0", key);
        Assert.Contains("insecure", key);
    }

    [Fact]
    public void MakeCacheKey_AppendsPinSuffix_AndIsDeterministic()
    {
        KahunaOptions opts = new() { TrustedServerCertificateThumbprints = ["BBBB", "AAAA"] };
        string key1 = GrpcBatcher.MakeCacheKey("https://host:1234", opts);

        // Different option instance, same thumbprints in different order → same key
        KahunaOptions opts2 = new() { TrustedServerCertificateThumbprints = ["AAAA", "BBBB"] };
        string key2 = GrpcBatcher.MakeCacheKey("https://host:1234", opts2);

        Assert.Equal(key1, key2);
        Assert.StartsWith("https://host:1234\0pin:", key1);
    }

    [Fact]
    public void MakeCacheKey_DifferentPolicies_ProduceDifferentKeys()
    {
        string url = "https://host:1234";
        string keyDefault  = GrpcBatcher.MakeCacheKey(url, null);
        string keyInsecure = GrpcBatcher.MakeCacheKey(url, new() { AllowInsecureCertificateValidation = true });
        string keyPin      = GrpcBatcher.MakeCacheKey(url, new() { TrustedServerCertificateThumbprints = ["AABB"] });

        Assert.Equal(3, new HashSet<string> { keyDefault, keyInsecure, keyPin }.Count);
    }

    // --- P2: GrpcChannelPoolSize in cache key ---

    [Fact]
    public void MakeCacheKey_ReturnsUrl_WhenPoolSizeIsDefault()
    {
        // Default opts (GrpcChannelPoolSize=2) produces the bare URL — no suffix.
        KahunaOptions opts = new() { GrpcChannelPoolSize = 2 };
        Assert.Equal("https://host:1234", GrpcBatcher.MakeCacheKey("https://host:1234", opts));
    }

    [Fact]
    public void MakeCacheKey_AppendsPoolSuffix_WhenPoolSizeDiffersFromDefault()
    {
        KahunaOptions opts = new() { GrpcChannelPoolSize = 5 };
        string key = GrpcBatcher.MakeCacheKey("https://host:1234", opts);
        Assert.StartsWith("https://host:1234\0pool:5", key);
    }

    [Fact]
    public void MakeCacheKey_ClampsZeroPoolSizeToOne()
    {
        // GrpcChannelPoolSize=0 is invalid; effective size is 1, which != default 2 → suffix present.
        KahunaOptions optsZero = new() { GrpcChannelPoolSize = 0 };
        KahunaOptions optsOne  = new() { GrpcChannelPoolSize = 1 };
        string keyZero = GrpcBatcher.MakeCacheKey("https://host:1234", optsZero);
        string keyOne  = GrpcBatcher.MakeCacheKey("https://host:1234", optsOne);
        // Both clamp to 1 → same key.
        Assert.Equal(keyOne, keyZero);
        Assert.Contains("pool:1", keyZero);
    }

    [Fact]
    public void MakeCacheKey_CombinesPoolSizeAndTlsPolicy()
    {
        KahunaOptions opts = new() { GrpcChannelPoolSize = 4, AllowInsecureCertificateValidation = true };
        string key = GrpcBatcher.MakeCacheKey("https://host:1234", opts);
        Assert.Contains("pool:4", key);
        Assert.Contains("insecure", key);
    }

    [Fact]
    public void MakeCacheKey_DifferentPoolSizes_ProduceDifferentKeys()
    {
        string url = "https://host:1234";
        string key2 = GrpcBatcher.MakeCacheKey(url, new() { GrpcChannelPoolSize = 2 });
        string key4 = GrpcBatcher.MakeCacheKey(url, new() { GrpcChannelPoolSize = 4 });
        string key8 = GrpcBatcher.MakeCacheKey(url, new() { GrpcChannelPoolSize = 8 });
        Assert.Equal(3, new HashSet<string> { key2, key4, key8 }.Count);
    }
}

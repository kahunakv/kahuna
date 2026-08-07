using Kahuna.Server.Configuration;

namespace Kahuna.Server.Tests;

/// <summary>
/// Covers the startup decisions a node makes before any actor or Raft partition exists: whether to
/// bind an HTTPS listener, and whether the directories a backend is about to open get created.
/// </summary>
public sealed class TestServerStartupConfiguration
{
    [Fact]
    public void NoCertificateBindsHttpOnly()
    {
        Assert.False(ConfigurationValidator.ShouldBindHttps(null, null));
        Assert.False(ConfigurationValidator.ShouldBindHttps("", null));
        Assert.False(ConfigurationValidator.ShouldBindHttps("", []));
    }

    [Fact]
    public void ConfiguredCertificateBindsHttps()
    {
        Assert.True(ConfigurationValidator.ShouldBindHttps("/etc/kahuna/cert.pfx", null));
        Assert.True(ConfigurationValidator.ShouldBindHttps("/etc/kahuna/cert.pfx", ["8082"]));
    }

    [Fact]
    public void HttpsPortsWithoutCertificateIsRejected()
    {
        // Dropping the ports silently would leave the operator believing a port is secured when it
        // is either absent or plaintext.
        Assert.Throws<KahunaServerException>(() => ConfigurationValidator.ShouldBindHttps(null, ["8082"]));
        Assert.Throws<KahunaServerException>(() => ConfigurationValidator.ShouldBindHttps("", ["8082", "8084"]));
    }

    [Fact]
    public void MissingCertificateFileIsAHardStartupError()
    {
        // An explicitly requested certificate that is not there must never degrade to plaintext.
        string missing = Path.Combine(Path.GetTempPath(), $"kahuna-absent-{Guid.NewGuid():N}.pfx");

        Assert.Throws<KahunaServerException>(() => ConfigurationValidator.Validate(new() { HttpsCertificate = missing }));
    }

    [Fact]
    public void ValidateCreatesMissingStorageAndWalDirectories()
    {
        string root = Path.Combine(Path.GetTempPath(), $"kahuna-validate-{Guid.NewGuid():N}");
        string storagePath = Path.Combine(root, "data");
        string walPath = Path.Combine(root, "wal");

        try
        {
            Assert.False(Directory.Exists(storagePath));
            Assert.False(Directory.Exists(walPath));

            ConfigurationValidator.Validate(new() { StoragePath = storagePath }, walPath);

            Assert.True(Directory.Exists(storagePath));
            Assert.True(Directory.Exists(walPath));
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void ValidateLeavesExistingDirectoriesAlone()
    {
        string root = Path.Combine(Path.GetTempPath(), $"kahuna-validate-{Guid.NewGuid():N}");
        string storagePath = Path.Combine(root, "data");
        string marker = Path.Combine(storagePath, "existing.db");

        try
        {
            Directory.CreateDirectory(storagePath);
            File.WriteAllText(marker, "keep me");

            ConfigurationValidator.Validate(new() { StoragePath = storagePath });

            Assert.True(File.Exists(marker));
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }
}

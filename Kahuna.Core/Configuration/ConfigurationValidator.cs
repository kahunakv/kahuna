
using System.Security.Cryptography.X509Certificates;

namespace Kahuna.Server.Configuration;

/// <summary>
/// Provides methods to validate and construct configurations required
/// for the Kahuna server based on command-line options.
/// </summary>
public static class ConfigurationValidator
{
    public static KahunaConfiguration Validate(KahunaConfiguration configuration, string? walPath = null)
    {
        if (!string.IsNullOrEmpty(configuration.HttpsCertificate))
        {
            if (!File.Exists(configuration.HttpsCertificate))
                throw new KahunaServerException("Invalid HTTPS certificate");
    
#pragma warning disable SYSLIB0057
            X509Certificate2 xcertificate = new(configuration.HttpsCertificate, configuration.HttpsCertificatePassword);
#pragma warning restore SYSLIB0057
            configuration.HttpsTrustedThumbprint = xcertificate.Thumbprint;
        }

        if (!string.IsNullOrEmpty(configuration.StoragePath))
        {
            if (Directory.Exists(configuration.StoragePath))
                Directory.CreateDirectory(configuration.StoragePath);
        }

        if (!string.IsNullOrEmpty(walPath))
        {
            if (Directory.Exists(walPath))
                Directory.CreateDirectory(walPath);
        }

        if (configuration.LocksWorkers <= 0)
            configuration.LocksWorkers = Math.Max(256, Environment.ProcessorCount * 4);
        
        if (configuration.KeyValueWorkers <= 0)
            configuration.KeyValueWorkers = Math.Max(256, Environment.ProcessorCount * 4);

        if (configuration.BackgroundWriterWorkers <= 0)
            configuration.BackgroundWriterWorkers = 1;
        
        return configuration;
    }
}

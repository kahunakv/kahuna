
using System.Security.Cryptography.X509Certificates;

namespace Kahuna.Server.Configuration;

public static class ConfigurationValidator
{
    public static KahunaConfiguration Validate(KahunaCommandLineOptions opts)
    {
        // Build Kahuna configuration
        KahunaConfiguration configuration = new()
        {
            HttpsCertificate = opts.HttpsCertificate,
            HttpsCertificatePassword = opts.HttpsCertificatePassword,
            LocksWorkers = opts.LocksWorkers,
            BackgroundWriterWorkers = opts.BackgroundWritersWorkers,
            PersistenceWorkers = opts.PersistenceWorkers,
            Storage = opts.Storage,
            StoragePath = opts.StoragePath,
            StorageRevision = opts.StorageRevision
        };
        
        if (!string.IsNullOrEmpty(configuration.HttpsCertificate))
        {
            if (!File.Exists(configuration.HttpsCertificate))
                throw new KahunaServerException("Invalid HTTPS certificate");
    
#pragma warning disable SYSLIB0057
            X509Certificate2 xcertificate = new(configuration.HttpsCertificate, configuration.HttpsCertificatePassword);
#pragma warning restore SYSLIB0057
            configuration.HttpsTrustedThumbprint = xcertificate.Thumbprint;
        }

        if (!string.IsNullOrEmpty(opts.StoragePath))
        {
            if (Directory.Exists(opts.StoragePath))
                Directory.CreateDirectory(opts.StoragePath);
        }

        if (!string.IsNullOrEmpty(opts.WalPath))
        {
            if (Directory.Exists(opts.WalPath))
                Directory.CreateDirectory(opts.WalPath);
        }

        if (configuration.LocksWorkers <= 0)
            configuration.LocksWorkers = Math.Max(32, Environment.ProcessorCount * 4);
        
        if (configuration.KeyValuesWorkers <= 0)
            configuration.KeyValuesWorkers = Math.Max(32, Environment.ProcessorCount * 4);

        if (configuration.BackgroundWriterWorkers <= 0)
            configuration.BackgroundWriterWorkers = 1;

        if (configuration.PersistenceWorkers <= 0)
            configuration.PersistenceWorkers = 8;
        
        return configuration;
    }
}

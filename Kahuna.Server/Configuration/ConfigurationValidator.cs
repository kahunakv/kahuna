
using System.Security.Cryptography.X509Certificates;

namespace Kahuna.Server.Configuration;

/// <summary>
/// Provides methods to validate and construct configurations required
/// for the Kahuna server based on command-line options.
/// </summary>
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
            KeyValueWorkers = opts.KeyValueWorkers,
            BackgroundWriterWorkers = opts.BackgroundWritersWorkers,
            Storage = opts.Storage,
            StoragePath = opts.StoragePath,
            StorageRevision = opts.StorageRevision,
            DefaultTransactionTimeout = opts.DefaultTransactionTimeout,
            ScriptCacheExpiration = TimeSpan.FromSeconds(opts.ScriptCacheExpiration),
            CacheEntryTtl = TimeSpan.FromSeconds(opts.CacheEntryTtl),
            CacheEntriesToRemove = opts.CacheEntriesToRemove,
            DirtyObjectsWriterDelay = opts.DirtyObjectsWriterDelay
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
            configuration.LocksWorkers = Math.Max(256, Environment.ProcessorCount * 4);
        
        if (configuration.KeyValueWorkers <= 0)
            configuration.KeyValueWorkers = Math.Max(256, Environment.ProcessorCount * 4);

        if (configuration.BackgroundWriterWorkers <= 0)
            configuration.BackgroundWriterWorkers = 1;
        
        return configuration;
    }
}

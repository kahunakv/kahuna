
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

        if (configuration.CollectionInterval <= TimeSpan.Zero)
            configuration.CollectionInterval = TimeSpan.FromSeconds(60);

        if (configuration.CollectBatchMax <= 0)
            configuration.CollectBatchMax = configuration.CacheEntriesToRemove > 0 ? configuration.CacheEntriesToRemove : 1_000;

        if (configuration.CacheEntriesToRemove <= 0)
            configuration.CacheEntriesToRemove = configuration.CollectBatchMax;

        if (configuration.MaxEntriesPerActor <= 0)
            configuration.MaxEntriesPerActor = 50_000;

        if (configuration.MaxBytesPerActor <= 0)
            configuration.MaxBytesPerActor = 256L * 1024 * 1024;

        if (configuration.RevisionRetention <= 0)
            configuration.RevisionRetention = configuration.RevisionsToKeepCached > 0 ? configuration.RevisionsToKeepCached : 16;

        if (configuration.LruSampleSize <= 0)
            configuration.LruSampleSize = 5;

        if (configuration.LruSampleScanMax <= 0)
            configuration.LruSampleScanMax = 256;

        if (configuration.MetadataTrimInterval < 0)
            configuration.MetadataTrimInterval = 4;
        
        return configuration;
    }
}

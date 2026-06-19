
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

        ValidatePersistentRevisionRetention(configuration);
        ValidatePitr(configuration);

        return configuration;
    }

    private static void ValidatePersistentRevisionRetention(KahunaConfiguration configuration)
    {
        if (configuration.PersistentRevisionRetentionCount < 0)
            configuration.PersistentRevisionRetentionCount = 0;

        if (configuration.PersistentRevisionRetentionAge < TimeSpan.Zero)
            configuration.PersistentRevisionRetentionAge = TimeSpan.Zero;

        if (configuration.PersistentRevisionRetentionCount != 0
            && configuration.PersistentRevisionRetentionCount < 1)
            configuration.PersistentRevisionRetentionCount = 1;

        if (configuration.PersistentRevisionCleanupBatchSize < 1)
            configuration.PersistentRevisionCleanupBatchSize = 1;

        if (!IsPersistentRevisionRetentionEnabled(configuration))
            return;

        if (configuration.PersistentRevisionCleanupInterval <= TimeSpan.Zero)
            configuration.PersistentRevisionCleanupInterval = TimeSpan.FromMinutes(5);
    }

    internal static bool IsPersistentRevisionRetentionEnabled(KahunaConfiguration configuration) =>
        configuration.PersistentRevisionRetentionCount != 0
        || configuration.PersistentRevisionRetentionAge > TimeSpan.Zero;

    private static void ValidatePitr(KahunaConfiguration configuration)
    {
        if (configuration.PitrWindow <= TimeSpan.Zero)
            configuration.PitrWindow = TimeSpan.FromHours(1);

        if (configuration.PitrWindow > TimeSpan.FromHours(6))
            configuration.PitrWindow = TimeSpan.FromHours(6);

        if (configuration.BaseSnapshotInterval <= TimeSpan.Zero)
            configuration.BaseSnapshotInterval = TimeSpan.FromMinutes(30);

        if (configuration.BaseSnapshotInterval > configuration.PitrWindow)
            configuration.BaseSnapshotInterval = configuration.PitrWindow;
    }
}

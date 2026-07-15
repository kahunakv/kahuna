
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
            configuration.LocksWorkers = Math.Max(32, Environment.ProcessorCount * 4);
        
        if (configuration.KeyValueWorkers <= 0)
            configuration.KeyValueWorkers = Math.Max(32, Environment.ProcessorCount * 4);

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

        if (configuration.DefaultTransactionTimeout <= 0)
            throw new KahunaServerException(
                $"DefaultTransactionTimeout ({configuration.DefaultTransactionTimeout} ms) must be greater than zero; " +
                "a non-positive value resolves every unconstrained session request to zero, which fires the cancellation token immediately.");

        if (configuration.MaxTransactionTimeout <= 0)
            throw new KahunaServerException(
                $"MaxTransactionTimeout ({configuration.MaxTransactionTimeout} ms) must be greater than zero.");

        if (configuration.MaxTransactionTimeout < configuration.DefaultTransactionTimeout)
            throw new KahunaServerException(
                $"MaxTransactionTimeout ({configuration.MaxTransactionTimeout} ms) must be >= DefaultTransactionTimeout ({configuration.DefaultTransactionTimeout} ms); " +
                "a maximum below the default would clamp every default-length session.");

        ValidatePersistentRevisionRetention(configuration);
        ValidatePitr(configuration);

        return configuration;
    }

    /// <summary>
    /// Validates that <see cref="KahunaConfiguration.RangeSplitSettleWindow"/> is at least as long
    /// as <paramref name="minLeaderStabilityMs"/> (the Raft leader-stability gate). A shorter settle
    /// window allows re-splitting before the new partition's leader has stabilised, which defeats
    /// the cooldown entirely. Call this after both <see cref="KahunaConfiguration"/> and
    /// <c>RaftConfiguration</c> are constructed, passing <c>RaftConfiguration.MinLeaderStabilityMs</c>.
    /// No-op when either value is zero (feature disabled).
    /// </summary>
    /// <exception cref="KahunaServerException">Thrown when the constraint is violated.</exception>
    public static void ValidateSettleWindow(KahunaConfiguration configuration, long minLeaderStabilityMs)
    {
        if (minLeaderStabilityMs <= 0 || configuration.RangeSplitSettleWindow <= TimeSpan.Zero)
            return;

        if (configuration.RangeSplitSettleWindow.TotalMilliseconds < minLeaderStabilityMs)
            throw new KahunaServerException(
                $"RangeSplitSettleWindow ({configuration.RangeSplitSettleWindow.TotalMilliseconds:F0} ms) " +
                $"must be at least MinLeaderStabilityMs ({minLeaderStabilityMs} ms); " +
                "a shorter window allows re-splitting before the new partition's leader has stabilised.");
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

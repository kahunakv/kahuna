
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

        // ── Partition write aggregator ──────────────────────────────────────────────────────────────
        // Linger may be zero (immediate dispatch) but never negative.
        if (configuration.KeyValueWriteLingerMs < 0)
            configuration.KeyValueWriteLingerMs = 0;

        if (configuration.KeyValueWriteMaxBatchItems <= 0)
            configuration.KeyValueWriteMaxBatchItems = 512;

        if (configuration.KeyValueWriteMaxBatchBytes <= 0)
            configuration.KeyValueWriteMaxBatchBytes = 4 * 1024 * 1024;

        if (configuration.KeyValueWriteMaxQueuedItemsPerPartition <= 0)
            configuration.KeyValueWriteMaxQueuedItemsPerPartition = 8_192;

        if (configuration.KeyValueWriteMaxQueuedBytesPerPartition <= 0)
            configuration.KeyValueWriteMaxQueuedBytesPerPartition = 32L * 1024 * 1024;

        if (configuration.KeyValueWriteMaxQueueDelayMs <= 0)
            configuration.KeyValueWriteMaxQueueDelayMs = 1_000;

        // Linger is the coalescing wait before a buffer flushes; it must never exceed the queue-age deadline,
        // or an item could pass its release deadline before its buffer's linger timer ever fires. Clamp down.
        if (configuration.KeyValueWriteLingerMs > configuration.KeyValueWriteMaxQueueDelayMs)
            configuration.KeyValueWriteLingerMs = configuration.KeyValueWriteMaxQueueDelayMs;

        // A batch can never select more than a partition is allowed to hold.
        if (configuration.KeyValueWriteMaxBatchItems > configuration.KeyValueWriteMaxQueuedItemsPerPartition)
            configuration.KeyValueWriteMaxBatchItems = configuration.KeyValueWriteMaxQueuedItemsPerPartition;

        if (configuration.KeyValueWriteMaxBatchBytes > configuration.KeyValueWriteMaxQueuedBytesPerPartition)
            configuration.KeyValueWriteMaxBatchBytes = (int)Math.Min(configuration.KeyValueWriteMaxBatchBytes, configuration.KeyValueWriteMaxQueuedBytesPerPartition);

        // Terminal reserves are additive headroom; a negative value would shrink the base budget, not add to it.
        if (configuration.KeyValueWriteTerminalReserveItemsPerPartition < 0)
            configuration.KeyValueWriteTerminalReserveItemsPerPartition = 0;
        if (configuration.KeyValueWriteTerminalReserveBytesPerPartition < 0)
            configuration.KeyValueWriteTerminalReserveBytesPerPartition = 0;
        if (configuration.KeyValueWriteTerminalReserveItemsGlobal < 0)
            configuration.KeyValueWriteTerminalReserveItemsGlobal = 0;
        if (configuration.KeyValueWriteTerminalReserveBytesGlobal < 0)
            configuration.KeyValueWriteTerminalReserveBytesGlobal = 0;

        // The hard per-operation ceiling must admit at least a full batch's worth of bytes, or a legitimately
        // large single value below the batch target could never be admitted. A value <= 0 disables the ceiling.
        if (configuration.KeyValueWriteMaxOperationBytes > 0
            && configuration.KeyValueWriteMaxOperationBytes < configuration.KeyValueWriteMaxBatchBytes)
            configuration.KeyValueWriteMaxOperationBytes = configuration.KeyValueWriteMaxBatchBytes;

        // The pre-dispatch residence bound must leave real headroom below the write-intent lease for the wake
        // scheduling plus the Raft round trip, so a queue-age-released write can never be proposed after its
        // intent could already have expired.
        const int writeIntentLeaseMs = 10_000;
        const int schedulingHeadroomMs = 2_000;
        int maxAllowedQueueDelayMs = writeIntentLeaseMs - schedulingHeadroomMs;
        if (configuration.KeyValueWriteMaxQueueDelayMs > maxAllowedQueueDelayMs)
            throw new KahunaServerException(
                $"KeyValueWriteMaxQueueDelayMs ({configuration.KeyValueWriteMaxQueueDelayMs} ms) must be <= {maxAllowedQueueDelayMs} ms — the {writeIntentLeaseMs} ms write-intent lease minus {schedulingHeadroomMs} ms of scheduling/round-trip headroom — so a queue-age-released write cannot be proposed late.");

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

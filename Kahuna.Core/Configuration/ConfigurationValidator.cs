
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Configuration;

/// <summary>
/// Provides methods to validate and construct configurations required
/// for the Kahuna server based on command-line options.
/// </summary>
public static class ConfigurationValidator
{
    /// <summary>
    /// Whether the Kestrel HTTPS listener should be created at all.
    ///
    /// Binding HTTPS without a certificate is not merely useless: Kestrel resolves an empty
    /// certificate path against the current working directory and aborts startup, so a node launched
    /// with no flags could never start. With no certificate the node binds HTTP only.
    ///
    /// Ports explicitly requested as HTTPS without a certificate are a contradiction and throw —
    /// silently serving plaintext on a port the operator asked to be secure is the one outcome that
    /// must not happen.
    /// </summary>
    public static bool ShouldBindHttps(string? httpsCertificate, IEnumerable<string>? httpsPorts)
    {
        if (!string.IsNullOrEmpty(httpsCertificate))
            return true;

        if (httpsPorts is not null && httpsPorts.Any())
            throw new KahunaServerException("--https-ports requires --https-certificate; refusing to serve plaintext on a port requested as HTTPS");

        return false;
    }

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

        // Create the directories a backend is about to open. RocksDB creates its own, but SQLite does
        // not, and a node must not fail to start merely because its data directory does not exist yet.
        if (!string.IsNullOrEmpty(configuration.StoragePath))
        {
            if (!Directory.Exists(configuration.StoragePath))
                Directory.CreateDirectory(configuration.StoragePath);
        }

        if (!string.IsNullOrEmpty(walPath))
        {
            if (!Directory.Exists(walPath))
                Directory.CreateDirectory(walPath);
        }

        if (string.IsNullOrEmpty(configuration.InterNodeGrpcScheme))
            configuration.InterNodeGrpcScheme = "https://";

        if (configuration.LocksWorkers <= 0)
            configuration.LocksWorkers = Math.Max(32, Environment.ProcessorCount * 4);
        
        if (configuration.KeyValueWorkers <= 0)
            configuration.KeyValueWorkers = Math.Max(32, Environment.ProcessorCount * 4);

        if (configuration.BackgroundWriterWorkers <= 0)
            configuration.BackgroundWriterWorkers = 1;

        if (configuration.SequencerWorkers <= 0)
            configuration.SequencerWorkers = Math.Max(8, Environment.ProcessorCount);

        // A block of one value is the gap-free mode (one commit per value); zero or negative would
        // reserve nothing and never make progress.
        if (configuration.SequencerBlockSize <= 0)
            configuration.SequencerBlockSize = 1;

        if (configuration.SequencerIdempotencyRetentionTtl < TimeSpan.Zero)
            configuration.SequencerIdempotencyRetentionTtl = TimeSpan.Zero;

        if (configuration.SequencerBlockLease < TimeSpan.Zero)
            configuration.SequencerBlockLease = TimeSpan.Zero;

        if (configuration.CollectionInterval <= TimeSpan.Zero)
            configuration.CollectionInterval = TimeSpan.FromSeconds(60);

        // A non-positive checkpoint interval makes every dirty partition due on every flush tick, so each tick
        // rewrites the receipt/record/intent snapshots that gate the checkpoint — throughput collapses for no
        // retention benefit.
        if (configuration.CheckpointInterval <= TimeSpan.Zero)
            configuration.CheckpointInterval = TimeSpan.FromSeconds(30);

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

        if (configuration.DefaultAdmissionWaitMs <= 0)
            throw new KahunaServerException(
                $"DefaultAdmissionWaitMs ({configuration.DefaultAdmissionWaitMs} ms) must be greater than zero; " +
                "a non-positive value resolves every unconstrained request to zero, which refuses admission before the caller can ever be queued.");

        if (configuration.MaxAdmissionWaitMs <= 0)
            throw new KahunaServerException(
                $"MaxAdmissionWaitMs ({configuration.MaxAdmissionWaitMs} ms) must be greater than zero.");

        if (configuration.MaxAdmissionWaitMs < configuration.DefaultAdmissionWaitMs)
            throw new KahunaServerException(
                $"MaxAdmissionWaitMs ({configuration.MaxAdmissionWaitMs} ms) must be >= DefaultAdmissionWaitMs ({configuration.DefaultAdmissionWaitMs} ms); " +
                "a maximum below the default would clamp every caller that did not ask for a specific budget.");

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
            // Both names are given: the property for an embedded consumer, the flag for an operator
            // who set this from the command line and has no way to map one to the other.
            throw new KahunaServerException(
                $"RangeSplitSettleWindow / --range-split-settle-window ({configuration.RangeSplitSettleWindow.TotalMilliseconds:F0} ms) " +
                $"must be at least MinLeaderStabilityMs / --raft-min-leader-stability-ms ({minLeaderStabilityMs} ms); " +
                "a shorter window allows re-splitting before the new partition's leader has stabilised.");
    }

    /// <summary>
    /// Refuses a collection interval short enough to undermine what else rides on it.
    /// <para>
    /// The interval is not only the split/merge sampling cadence: session range locks are leased for
    /// <b>twice</b> it and renewed on its tick, with the renewal sweep bounded by one interval. Lower
    /// it far enough and the sweep cannot finish a fan-out of inter-node re-acquires before its own
    /// deadline, so locks lapse under load — while the operator believes they only made splitting more
    /// eager. The floor is the phase-two commit timeout: giving an entire renewal sweep less time than
    /// a single commit round trip is allowed cannot be defended.
    /// </para>
    /// <para>
    /// Deliberately <b>not</b> part of <see cref="Validate"/>: the embedded API is used by tests that
    /// drive lease expiry directly at sub-second intervals, and this is a guard on the operator-facing
    /// flag, not on the library. Call it from the server's startup path.
    /// </para>
    /// </summary>
    /// <exception cref="KahunaServerException">Thrown when the interval is below the floor.</exception>
    public static void ValidateCollectionInterval(KahunaConfiguration configuration)
    {
        TimeSpan floor = TimeSpan.FromMilliseconds(Math.Max(configuration.Phase2CommitTimeout, 1_000));

        if (configuration.CollectionInterval < floor)
            throw new KahunaServerException(
                $"--range-collection-interval ({configuration.CollectionInterval.TotalSeconds:F0}s) must be at least " +
                $"{floor.TotalSeconds:F0}s (the phase-two commit timeout). It also sets the session range-lock lease " +
                "(twice the interval) and bounds the renewal sweep, so a shorter interval risks locks lapsing under load.");
    }

    /// <summary>
    /// Validates the replica-placement (replication factor) settings at startup. A negative factor
    /// is meaningless and refused; questionable-but-workable settings only warn, because Kommander
    /// degrades gracefully — a cluster with fewer live nodes than the factor simply runs every
    /// range at the available node count until more nodes join.
    /// </summary>
    /// <param name="replicationFactor">Desired voter replicas per range; 0 = full replication.</param>
    /// <param name="seedNodeCount">Number of seed nodes known at startup, or null when the caller
    /// cannot know it (an embedded node handed an externally built discovery).</param>
    /// <param name="logger">Sink for the non-fatal warnings; null suppresses them.</param>
    /// <exception cref="KahunaServerException">Thrown when the factor is negative.</exception>
    public static void ValidateReplicaPlacement(int replicationFactor, int? seedNodeCount, ILogger? logger)
    {
        if (replicationFactor < 0)
            throw new KahunaServerException(
                $"Replication factor ({replicationFactor}) must be zero or positive; " +
                "0 means full replication (every voter hosts every range).");

        if (replicationFactor == 0)
            return;

        // An even factor tolerates no more failures than the next odd factor down (quorum of 4 is 3,
        // same single-failure tolerance as quorum of 3 at 3 replicas) while paying for an extra copy.
        if (replicationFactor % 2 == 0)
            logger?.LogWarning(
                "Replication factor {ReplicationFactor} is even; it has the same failure tolerance as {OddFactor} " +
                "at the cost of an extra replica per range. Prefer odd values.",
                replicationFactor, replicationFactor - 1);

        if (seedNodeCount is > 0 && replicationFactor > seedNodeCount)
            logger?.LogWarning(
                "Replication factor {ReplicationFactor} exceeds the {SeedNodeCount} seed node(s); " +
                "ranges will hold one replica per available node until enough nodes join.",
                replicationFactor, seedNodeCount);
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

        ValidateBackupTarget(configuration);
    }

    /// <summary>
    /// Refuses a backup target that cannot work, at startup rather than at the first backup. Both failures
    /// here are silent-data-loss shaped: a node that starts happily and only discovers at backup time that
    /// it has nowhere to write, or nowhere to stage, has been advertising a backup capability it never had.
    /// </summary>
    private static void ValidateBackupTarget(KahunaConfiguration configuration)
    {
        if (string.IsNullOrWhiteSpace(configuration.BackupDir))
            return; // backups disabled entirely

        if (string.IsNullOrWhiteSpace(configuration.BackupTarget))
            configuration.BackupTarget = "local";

        bool isLocal = string.Equals(configuration.BackupTarget, "local", StringComparison.OrdinalIgnoreCase);
        if (isLocal)
            return;

        if (configuration.BackupStorageProvider is null)
            throw new KahunaServerException(
                $"Backup target '{configuration.BackupTarget}' has no storage provider registered. " +
                "Object-storage targets ship as separate packages and the host must install one by setting " +
                "KahunaConfiguration.BackupStorageProvider; only 'local' is built in.");

        // A remote target always stages checkpoints locally, because a persistence backend can only write
        // one through the filesystem. Requiring the path up front means the operator sizes the volume for
        // it deliberately instead of discovering the requirement when a full backup fills the disk.
        if (string.IsNullOrWhiteSpace(configuration.BackupScratchDir))
            throw new KahunaServerException(
                $"Backup target '{configuration.BackupTarget}' requires a local scratch directory " +
                "(--backup-scratch-dir): a checkpoint is written through the filesystem before upload. " +
                "Size it for one full backup, which transits it in its entirety.");
    }
}

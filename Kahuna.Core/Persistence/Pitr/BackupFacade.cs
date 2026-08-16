
using Nixie;
using Kommander;
using Kommander.Time;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// The node's backup and point-in-time-recovery entry point: it wires the <see cref="BackupService"/>
/// to the rest of the node (flush barrier, MVCC snapshot holds, WAL retention holds, applied-index
/// probe), and translates every internal failure into the public, sanitized
/// <see cref="KahunaBackupException"/> callers classify by outcome.
///
/// <para>
/// A node without a configured backup directory gets a <see cref="Disabled"/> instance: it answers
/// <see cref="IsConfigured"/> false and refuses every operation, so callers never have to test for a
/// null service.
/// </para>
/// </summary>
internal sealed class BackupFacade : IDisposable
{
    /// <summary>
    /// Lease for the MVCC snapshot-history hold a full backup pins at its cut. The backup driver
    /// renews it at roughly a third of this interval for the whole checkpoint/hash/verify/publish
    /// window, so the hold outlives a long backup as long as renewal keeps succeeding.
    /// </summary>
    private const int SnapshotBackupHoldLeaseMs = 600_000;

    private readonly BackupService? service;

    private BackupFacade(BackupService? service) => this.service = service;

    internal bool IsConfigured => service is not null;

    /// <summary>A node with no backup directory configured: every operation refuses.</summary>
    internal static BackupFacade Disabled() => new(null);

    /// <summary>
    /// Builds the backup service for a node that has a backup directory configured, and starts the
    /// periodic GC reaper when one is enabled.
    /// </summary>
    /// <param name="flushPersistenceAsync">Flush barrier run before a checkpoint is captured.</param>
    /// <param name="appliedHlcProbe">
    /// Per-partition max-enqueued commit HLC of the background writer. The driver's applied-index
    /// barrier blocks on it until the writer has caught up with the committed writes being captured,
    /// so a probe that always answers <see cref="HLCTimestamp.Zero"/> does not silently truncate the
    /// checkpoint — it fails the backup closed once the barrier times out.
    /// </param>
    internal static BackupFacade Create(
        ActorSystem actorSystem,
        IRaft raft,
        IPersistenceBackend persistenceBackend,
        KahunaConfiguration configuration,
        KeyValuesManager keyValues,
        Func<Task> flushPersistenceAsync,
        Func<int, HLCTimestamp> appliedHlcProbe,
        ILogger<IKahuna> logger)
    {
        BackupStoragePair backupStorage = CreateBackupStorage(configuration);

        BackupService service = new(
            raft,
            persistenceBackend,
            configuration.BackupDir,
            backupStorage.Manifests,
            backupStorage.Artifacts,
            configuration.Storage,
            configuration.StorageRevision,
            flushPersistenceAsync,
            keyValues.GetSafeTimestampAsync,
            logger,
            clusterId: configuration.BackupClusterId,
            macKeyFile: configuration.BackupMacKeyFile,
            liveStoragePath: configuration.StoragePath,
            restoreRoot: configuration.RestoreRoot,
            // Composable WAL retention hold (Kommander) — keeps a captured incremental prefix from
            // being compacted mid-read; composes with the periodic horizon floor via minimum.
            acquireRetentionHold: raft.AcquireRetentionHold,
            // Pin MVCC history at the cut for the checkpoint window; fail closed if the snapshot
            // floor already passed the cut or the hold cannot be acquired.
            acquireSnapshotHold: async (cut, holdCt) =>
            {
                (KeyValueResponseType floorType, HLCTimestamp floor, _) = await keyValues.GetSnapshotFloor(holdCt);
                // An unconfirmed floor answer (no node with confirmed meta leadership) cannot
                // prove the floor has not already passed the cut — fail the backup closed.
                if (floorType != KeyValueResponseType.Get || floor.CompareTo(cut) > 0)
                    return null;

                (KeyValueResponseType type, string holdId, _) = await keyValues.AcquireSnapshotHold(
                    "pitr-backup-" + Guid.NewGuid().ToString("N")[..8], cut, SnapshotBackupHoldLeaseMs, holdCt);
                return type == KeyValueResponseType.Set && !string.IsNullOrEmpty(holdId) ? holdId : null;
            },
            releaseSnapshotHold: async (holdId, holdCt) =>
            {
                await keyValues.ReleaseSnapshotHold(holdId, holdCt);
            },
            // Extend the same lease for the whole backup; a lost renewal (expiry, leadership
            // change, or an un-committable mutation) fails the backup closed rather than letting
            // pruning reclaim the pinned history mid-run.
            renewSnapshotHold: async (holdId, holdCt) =>
            {
                (KeyValueResponseType type, _) = await keyValues.RenewSnapshotHold(holdId, SnapshotBackupHoldLeaseMs, holdCt);
                return type == KeyValueResponseType.Set;
            },
            snapshotHoldLeaseMs: SnapshotBackupHoldLeaseMs,
            // Applied-index barrier probe: the backup waits until committed writes are applied and
            // queued for persistence before flushing, so a checkpoint never misses committed data.
            appliedHlcProbe: appliedHlcProbe,
            // Chain-aware retention bounds; unset dimensions are unbounded, and all-unset (the
            // default) disables retention entirely so backups are only reclaimed when opted in.
            retentionPolicy: new BackupRetentionPolicy(
                MaxChains: configuration.BackupRetentionMaxChains > 0 ? configuration.BackupRetentionMaxChains : null,
                MaxAge: configuration.BackupRetentionMaxAge > TimeSpan.Zero ? configuration.BackupRetentionMaxAge : null,
                MaxTotalBytes: configuration.BackupRetentionMaxBytes > 0 ? configuration.BackupRetentionMaxBytes : null),
            // Restore checkpoint-copy throughput budget (bytes/sec; 0 = unlimited).
            copyThrottleBytesPerSec: configuration.BackupRestoreThrottleBytesPerSec);

        // Periodic backup GC: sweeps crash-orphaned/leftover artifacts (always) and enforces
        // retention (when configured), including a startup sweep on its first tick. Disabled when
        // the interval is non-positive; GC then runs only inline after each backup.
        if (configuration.BackupGcInterval > TimeSpan.Zero)
            actorSystem.Spawn<BackupGcReaperActor, BackupGcReaperRequest>(
                "backup-gc-reaper",
                service,
                configuration,
                logger);

        return new BackupFacade(service);
    }

    /// <summary>
    /// Builds the backup storage pair. A host that registered a
    /// <see cref="KahunaConfiguration.BackupStorageProvider"/> gets whatever it returns; otherwise the
    /// local-directory implementations are used, so an existing deployment behaves exactly as before.
    /// <para>
    /// A non-local target with no provider registered is a configuration error rather than a silent
    /// fallback to local disk: writing backups to the wrong place is not a failure an operator would
    /// notice until they needed a restore. Startup validation catches it first; this is the backstop for
    /// a host that bypassed validation.
    /// </para>
    /// </summary>
    private static BackupStoragePair CreateBackupStorage(KahunaConfiguration configuration)
    {
        string target = string.IsNullOrWhiteSpace(configuration.BackupTarget) ? "local" : configuration.BackupTarget;
        string? scratch = string.IsNullOrWhiteSpace(configuration.BackupScratchDir) ? null : configuration.BackupScratchDir;

        // Backup bytes sit on local disk in cleartext while they transit scratch, whatever the eventual
        // target is, so the scratch root gets the same treatment as a local backup root: refuse it if it
        // is symlinked or group/world-writable, then create it owner-only. Doing this centrally means a
        // storage provider cannot forget it — and cannot weaken it, since a target that declares no POSIX
        // hardening of its own still stages here.
        if (scratch is not null)
        {
            BackupFilePermissions.EnsureRootSecure(scratch);
            BackupFilePermissions.CreateDirectory(scratch);
        }

        if (configuration.BackupStorageProvider is not null)
        {
            BackupStoragePair pair = configuration.BackupStorageProvider(
                new BackupStorageContext(target, configuration.BackupDir, scratch));

            if (pair.Artifacts.Capabilities.RequiresLocalScratch && scratch is null)
                throw new KahunaServerException(
                    $"The configured backup target '{target}' stages backups through a local directory, " +
                    "but no backup scratch directory is configured.");

            return pair;
        }

        if (!string.Equals(target, "local", StringComparison.OrdinalIgnoreCase))
            throw new KahunaServerException(
                $"Backup target '{target}' requires a backup storage provider to be registered by the host " +
                "(object-storage targets ship as separate packages); only 'local' is built in.");

        return new BackupStoragePair(
            new LocalDirectoryStorageTarget(configuration.BackupDir),
            new LocalDirectoryArtifactStore(configuration.BackupDir));
    }

    internal async Task<KahunaBackupInfo> TakeFullAsync(CancellationToken ct = default)
    {
        BackupService svc = RequireBackupService();
        try { return await svc.TakeFullAsync(ct: ct); }
        catch (Exception ex) when (ShouldMap(ex)) { throw MapAndLog(svc, ex); }
    }

    internal async Task<KahunaBackupInfo> TakeIncrementalAsync(Guid parentBackupId, CancellationToken ct = default)
    {
        BackupService svc = RequireBackupService();
        try { return await svc.TakeIncrementalAsync(parentBackupId, ct: ct); }
        catch (Exception ex) when (ShouldMap(ex)) { throw MapAndLog(svc, ex); }
    }

    internal async Task<KahunaBackupInfo> TakeCoordinatedAsync(CancellationToken ct = default)
    {
        BackupService svc = RequireBackupService();
        try { return await svc.TakeCoordinatedBackupAsync(ct); }
        catch (Exception ex) when (ShouldMap(ex)) { throw MapAndLog(svc, ex); }
    }

    internal async Task<IReadOnlyList<KahunaBackupInfo>> ListAsync(CancellationToken ct = default)
    {
        BackupService svc = RequireBackupService();
        // Public listing runs the cheap structural check only; full artifact verification (hashing
        // every file of every backup) is opt-in at the service layer, not on the default network path.
        try { return await svc.ListBackupsAsync(verifyArtifacts: false, ct); }
        catch (Exception ex) when (ShouldMap(ex)) { throw MapAndLog(svc, ex); }
    }

    internal async Task<IReadOnlyList<KahunaBackupInfo>> GetChainAsync(Guid leafBackupId, CancellationToken ct = default)
    {
        BackupService svc = RequireBackupService();
        try { return await svc.ResolveAndValidateAsync(leafBackupId, ct); }
        catch (Exception ex) when (ShouldMap(ex)) { throw MapAndLog(svc, ex); }
    }

    internal async Task<KahunaRestoreResponse> RestoreToAsync(
        Guid leafBackupId,
        string targetDir,
        long targetTimeMs,
        CancellationToken ct = default)
    {
        BackupService svc = RequireBackupService();
        // Shared resolver (identical to the bootstrap path): a millisecond target restores to the
        // inclusive END of that millisecond, so a same-millisecond commit with counter > 0 is included
        // rather than dropped. Zero (targetTimeMs <= 0) means "chain max".
        HLCTimestamp targetTime = PitrTargetResolver.FromUnixMilliseconds(targetTimeMs);
        try { return await svc.RestoreToAsync(leafBackupId, targetDir, targetTime, ct: ct); }
        catch (Exception ex) when (ShouldMap(ex)) { throw MapAndLog(svc, ex); }
    }

    internal async Task<KahunaBackupGcResult> RunGarbageCollectionAsync(bool dryRun, CancellationToken ct = default)
    {
        BackupService svc = RequireBackupService();
        try
        {
            BackupGcInventory inventory = dryRun
                ? await svc.PlanGarbageCollectionAsync(ct)
                : await svc.RunGarbageCollectionAsync(ct);

            long bytes = 0;
            KahunaBackupGcResult result = new() { Applied = !dryRun };
            foreach (BackupGcCandidate c in inventory.RetentionDeletions)
            {
                bytes += c.Bytes;
                result.RetentionDeletions.Add(new KahunaBackupGcDeletion
                {
                    BackupId = c.BackupId,
                    Type = c.Type.ToString(),
                    CreatedAtUtc = c.CreatedAtUtc,
                    Bytes = c.Bytes,
                    Reason = c.Reason
                });
            }
            // The plan already carries display names only, never absolute server paths.
            foreach (OrphanSweepCandidate o in inventory.OrphanReclamations)
                result.OrphanReclamations.Add(new KahunaBackupGcOrphan
                {
                    Name = o.Name,
                    IsDirectory = o.IsDirectory,
                    Reason = o.Reason
                });
            result.BytesReclaimed = bytes;
            return result;
        }
        catch (Exception ex) when (ShouldMap(ex)) { throw MapAndLog(svc, ex); }
    }

    private BackupService RequireBackupService() =>
        service ?? throw new InvalidOperationException(
            "Backup is not configured on this node. Set BackupDir in configuration or --pitr-backup-dir.");

    /// <summary>
    /// True for internal backup failures that should be surfaced to API callers as a typed
    /// <see cref="KahunaBackupException"/>. Cancellation and already-typed exceptions pass through
    /// unchanged.
    /// </summary>
    private static bool ShouldMap(Exception ex) =>
        ex is not OperationCanceledException and not KahunaBackupException;

    /// <summary>
    /// Logs the full failure detail against a fresh correlation id and returns the sanitized, typed
    /// outcome with that id appended, so a caller sees only "&lt;stable message&gt; (operation abc123…)" while
    /// an operator can grep the server log for the same id to see the paths/exception behind it.
    /// </summary>
    private static KahunaBackupException MapAndLog(BackupService svc, Exception ex)
    {
        string operationId = Guid.NewGuid().ToString("N")[..12];
        svc.LogOperationFailure(operationId, ex);
        KahunaBackupException mapped = MapBackupException(ex);
        return new KahunaBackupException(mapped.Outcome, $"{mapped.Message} (operation {operationId})");
    }

    /// <summary>
    /// Maps an internal backup exception to a public, sanitized <see cref="KahunaBackupException"/>
    /// so callers classify failures by <see cref="KahunaBackupOutcome"/> rather than by internal
    /// exception type or message. Messages intentionally omit absolute paths and raw backend text.
    /// </summary>
    private static KahunaBackupException MapBackupException(Exception ex) => ex switch
    {
        BackupDriverException d when d.NeedsFullBackup =>
            new(KahunaBackupOutcome.NeedsFull,
                "An incremental backup could not be produced because the WAL compaction floor " +
                "advanced past the parent backup; take a full backup instead."),
        BackupDriverException d when d.ParentMissing =>
            new(KahunaBackupOutcome.ParentMissing, "The requested parent backup was not found."),
        BackupDriverException d when d.TargetConflict =>
            new(KahunaBackupOutcome.TargetConflict,
                "The restore destination already exists or overlaps a protected path."),
        BackupDriverException d when d.TargetOutsideCoverage =>
            new(KahunaBackupOutcome.TargetOutsideCoverage,
                "The requested restore time is outside the recoverable coverage of this backup chain."),
        BackupDriverException d when d.ExactCheckpointUnavailable =>
            new(KahunaBackupOutcome.ExactCheckpointUnavailable,
                "This node's storage backend cannot produce an exact as-of backup image."),
        BackupDriverException d when d.TopologyChanged =>
            new(KahunaBackupOutcome.TopologyChanged,
                "The cluster topology changed during the backup; nothing was published. Retry once stable."),
        BackupDriverException d when d.RetryableLeadershipLoss =>
            new(KahunaBackupOutcome.RetryableLeadershipLoss,
                "Meta-partition leadership was lost during the backup; nothing was published. Retry against the leader."),
        // The full message passes through: it names only partition ids (no paths or backend text),
        // and the caller needs them to know which partitions the chain is missing.
        BackupDriverException d when d.RestrictedCoverage =>
            new(KahunaBackupOutcome.RestrictedCoverage, d.Message),
        BackupInsecureRootException =>
            new(KahunaBackupOutcome.InsecureRoot,
                "The configured backup or restore directory is unsafe (a symlink, or group/world-writable). " +
                "Restrict it to the server's user and retry."),
        ExactCheckpointUnavailableException =>
            new(KahunaBackupOutcome.ExactCheckpointUnavailable,
                "An exact as-of backup image cannot be produced because a historyless (SetNoRevision) " +
                "key was modified after the backup cut."),
        BackupUnsupportedFormatException =>
            new(KahunaBackupOutcome.UnsupportedFormat,
                "The backup is in a legacy or unsupported format and cannot be restored by this version."),
        BackupArtifactException =>
            new(KahunaBackupOutcome.CorruptArtifact,
                "A backup artifact is missing, altered, or failed integrity verification."),
        BackupChainException =>
            new(KahunaBackupOutcome.CorruptChain, "The backup chain is structurally invalid."),
        BackupDriverException =>
            new(KahunaBackupOutcome.IoError, "The backup operation could not be completed."),
        IOException =>
            new(KahunaBackupOutcome.IoError, "The backup operation failed due to an I/O error."),
        _ => new(KahunaBackupOutcome.IoError, "The backup operation failed."),
    };

    public void Dispose() => service?.Dispose();
}

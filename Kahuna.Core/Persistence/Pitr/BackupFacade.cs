
using Nixie;
using Kommander;
using Kommander.Time;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;

using Kahuna.Server.Persistence.Providers;

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

    // How many times a full backup asks for its snapshot hold while the answer is the retryable MustRetry, and
    // the base delay between asks (it grows with each attempt).
    private const int SnapshotHoldAttempts = 5;

    private const int SnapshotHoldRetryDelayMs = 50;

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
        IInterNodeCommunication interNodeCommunication,
        ILogger<IKahuna> logger)
    {
        BackupStoragePair backupStorage = CreateBackupStorage(configuration, configuration.BackupDir, logger);

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
            storageProviders: configuration.StorageProviders,
            // Composable WAL retention hold (Kommander) — keeps a captured incremental prefix from
            // being compacted mid-read; composes with the periodic horizon floor via minimum.
            acquireRetentionHold: raft.AcquireRetentionHold,
            // Pin MVCC history at the cut for the checkpoint window; fail closed if the snapshot
            // floor already passed the cut or the hold cannot be acquired.
            acquireSnapshotHold: async (cut, holdCt) =>
            {
                // One holder id for every attempt: an acquire is idempotent by (holder, timestamp), so a retry
                // after a lost answer finds the hold the earlier attempt committed instead of adding another.
                string holderId = "pitr-backup-" + Guid.NewGuid().ToString("N")[..8];

                for (int attempt = 1; ; attempt++)
                {
                    (KeyValueResponseType floorType, HLCTimestamp floor, _) = await keyValues.GetSnapshotFloor(holdCt);

                    // A confirmed floor above the cut is final: the history the cut needs may already be gone.
                    if (floorType == KeyValueResponseType.Get && floor.CompareTo(cut) > 0)
                        return null;

                    KeyValueResponseType answer = floorType;
                    if (floorType == KeyValueResponseType.Get)
                    {
                        (KeyValueResponseType type, string holdId, _) = await keyValues.AcquireSnapshotHold(
                            holderId, cut, SnapshotBackupHoldLeaseMs, holdCt);
                        if (type == KeyValueResponseType.Set && !string.IsNullOrEmpty(holdId))
                            return holdId;
                        answer = type;
                    }

                    // An unconfirmed floor answer (no node with confirmed meta leadership) cannot prove the floor
                    // has not already passed the cut, and a refused acquire protects nothing — both fail the backup
                    // closed. Only the retryable answer is tried again: a meta-leader change, or a prune that
                    // overlapped the acquire, which several shares of one cluster-wide backup make more likely.
                    if (answer != KeyValueResponseType.MustRetry || attempt >= SnapshotHoldAttempts)
                        return null;

                    await Task.Delay(SnapshotHoldRetryDelayMs * attempt, holdCt);
                }
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
            copyThrottleBytesPerSec: configuration.BackupRestoreThrottleBytesPerSec,
            // The coordinated backup verifies its cut after the capture against the durable intents this node
            // held from before the cut was chosen until then.
            beginCommitObservation: keyValues.BeginCommitObservation,
            // A full backup records the durable intents live at its range ends, which the restore of every chain
            // built on it needs to expand a commit settled after the range.
            walkLiveIntents: keyValues.DurablePreparedIntentStore.WalkLiveIntents,
            // A cluster-wide backup under a replication factor asks the nodes that host the partitions this
            // node does not host for their shares, and captures its own share with the same filter.
            interNodeCommunication: interNodeCommunication,
            keyOwnership: partitions =>
            {
                RangeMap map = keyValues.RangeMapStore.Current;
                int hashPoolSize = raft.Configuration.InitialPartitions;
                return key => partitions.Contains(PartitionDataEnumerator.OwnerOfKey(map, key, hashPoolSize));
            },
            partScratchRoot: configuration.BackupScratchDir,
            // Capture I/O budget (bytes/sec; 0 = unlimited): paces the hash and verify reads of a capture.
            captureThrottleBytesPerSec: configuration.BackupCaptureThrottleBytesPerSec,
            // How long a capture waits for the committed writes of its partitions to be applied before it reports
            // them as not caught up; a non-positive value keeps the driver's default.
            applyBarrierTimeoutMs: (int)Math.Clamp(configuration.BackupApplyBarrierTimeout.TotalMilliseconds, 0, int.MaxValue));

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
    /// Seeds a node that is about to join a running cluster from the backup chain ending at
    /// <paramref name="leafBackupId"/>: replays the chain into <paramref name="persistenceBackend"/> and
    /// writes a committed checkpoint per partition into <paramref name="walAdapter"/>, so the node presents
    /// to the leader at the restored index instead of at index 0. Runs before the node joins, so it cannot
    /// use the node's <see cref="BackupService"/>, which may not exist (no backup directory configured) and
    /// whose catalog is bound to the configured directory rather than <paramref name="backupDir"/>.
    ///
    /// <para>It reads the chain exactly as a restore does: through the same storage target and artifact
    /// store (a registered <see cref="KahunaConfiguration.BackupStorageProvider"/> included), from a root
    /// that passes the same permission check, with every manifest authenticated against the configured
    /// <see cref="KahunaConfiguration.BackupMacKeyFile"/>. It also refuses a chain stamped with another
    /// <see cref="KahunaConfiguration.BackupClusterId"/>; see <see cref="BootstrapHelper"/> for why a seed
    /// is stricter than a restore there.</para>
    ///
    /// <para>Failures surface as a typed <see cref="KahunaBackupException"/>, as every other backup
    /// operation does. The full detail is logged against the operation id the message carries.</para>
    /// </summary>
    internal static async Task BootstrapAsync(
        KahunaConfiguration configuration,
        string backupDir,
        Guid leafBackupId,
        HLCTimestamp targetTime,
        IPersistenceBackend persistenceBackend,
        Kommander.WAL.IWAL walAdapter,
        TimeSpan pitrWindow,
        TimeSpan baseSnapshotInterval,
        Func<Task> flushPersistenceAsync,
        ILogger<IKahuna> logger,
        Action<int, long>? recordSeedBoundary = null)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(backupDir))
                throw new BackupDriverException("A PITR bootstrap needs the backup directory that holds the chain.");

            BackupFilePermissions.EnsureRootSecure(backupDir);

            BackupStoragePair storage = CreateBackupStorage(configuration, backupDir);
            byte[]? macKey = BackupService.LoadMacKey(configuration.BackupMacKeyFile);

            BackupCatalog catalog = new(storage.Manifests);
            IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(leafBackupId).ConfigureAwait(false);

            // Every check that can refuse the chain runs inside BootstrapNodeAsync before the backend or
            // the WAL is touched, so a refusal leaves the node exactly as it was.
            await flushPersistenceAsync().ConfigureAwait(false);
            await BootstrapHelper.BootstrapNodeAsync(
                chain, storage.Artifacts, targetTime, persistenceBackend, walAdapter, pitrWindow, DateTime.UtcNow,
                baseSnapshotInterval, macKey, configuration.BackupClusterId, recordSeedBoundary).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldMap(ex))
        {
            throw MapAndLog((operationId, e) => logger.LogError(e, "PITR bootstrap {OperationId} failed", operationId), ex);
        }
    }

    /// <summary>
    /// The first step of a PITR bootstrap, run before the persistence backend opens: puts the full backup's
    /// checkpoint into the node's storage path, verified, unless the store already holds data or a completed
    /// seed. See <see cref="BootstrapBaseImage"/> for the layout, the publish protocol and the restart rules.
    /// The returned plan tells the caller whether to run <see cref="BootstrapAsync"/> before the join.
    ///
    /// <para>The chain is read through the same storage target and artifact store as every other backup
    /// operation (a registered <see cref="KahunaConfiguration.BackupStorageProvider"/> included), so a bucket
    /// target needs no tool outside the server. Every refusal leaves no backend data in the storage path.</para>
    /// </summary>
    /// <param name="leafBackupId">The leaf to seed from, or null for the newest valid chain of this cluster.</param>
    /// <param name="fallbackToJoin">
    /// When true, a chain outside the retention window (or no chain at all, when <paramref name="leafBackupId"/>
    /// is null) gives a fallback plan with an empty store instead of a failure.
    /// </param>
    internal static async Task<PitrBootstrapPlan> PrepareBootstrapAsync(
        KahunaConfiguration configuration,
        string backupDir,
        Guid? leafBackupId,
        HLCTimestamp targetTime,
        TimeSpan pitrWindow,
        TimeSpan baseSnapshotInterval,
        bool fallbackToJoin,
        ILogger logger,
        CancellationToken ct = default)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(backupDir))
                throw new BackupDriverException("A PITR bootstrap needs the backup directory that holds the chain.");

            BackupFilePermissions.EnsureRootSecure(backupDir);

            return await BootstrapBaseImage.PrepareAsync(new BootstrapBaseImageRequest(
                configuration.Storage,
                configuration.StoragePath,
                configuration.StorageRevision,
                CreateBackupStorage(configuration, backupDir),
                leafBackupId,
                targetTime,
                pitrWindow,
                baseSnapshotInterval,
                DateTime.UtcNow,
                BackupService.LoadMacKey(configuration.BackupMacKeyFile),
                configuration.BackupClusterId,
                fallbackToJoin,
                configuration.BackupRestoreThrottleBytesPerSec,
                configuration.StorageProviders), ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldMap(ex))
        {
            throw MapAndLog((operationId, e) => logger.LogError(e, "PITR bootstrap {OperationId} failed", operationId), ex);
        }
    }

    /// <summary>
    /// Records in the storage path that the seed of a plan from <see cref="PrepareBootstrapAsync"/> completed,
    /// so a later start skips the seed. Call it after <see cref="BootstrapAsync"/> returned successfully.
    /// </summary>
    internal static void CompleteBootstrap(PitrBootstrapPlan plan, ILogger logger)
    {
        try
        {
            BootstrapBaseImage.MarkSeeded(plan.StoragePath);
        }
        catch (Exception ex) when (ShouldMap(ex))
        {
            throw MapAndLog((operationId, e) => logger.LogError(e, "PITR bootstrap {OperationId} failed", operationId), ex);
        }
    }

    /// <summary>
    /// Builds the backup storage pair rooted at <paramref name="backupDir"/>. A host that registered a
    /// <see cref="KahunaConfiguration.BackupStorageProvider"/> gets whatever it returns; otherwise the
    /// local-directory implementations are used, so an existing deployment behaves exactly as before.
    /// <para>
    /// A non-local target with no provider registered is a configuration error rather than a silent
    /// fallback to local disk: writing backups to the wrong place is not a failure an operator would
    /// notice until they needed a restore. Startup validation catches it first; this is the backstop for
    /// a host that bypassed validation.
    /// </para>
    /// </summary>
    private static BackupStoragePair CreateBackupStorage(KahunaConfiguration configuration, string backupDir, ILogger? logger = null)
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
                new BackupStorageContext(target, backupDir, scratch));

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
            new LocalDirectoryStorageTarget(backupDir),
            new LocalDirectoryArtifactStore(backupDir, CreateLiveCheckpointStaging(configuration, logger)));
    }

    /// <summary>
    /// Staging on the store's filesystem for a local target, used when the backup directory is on another one.
    /// Only an engine whose checkpoint links the live files benefits (RocksDB hard-links them where the
    /// filesystem allows it); one that writes a full copy wherever the checkpoint goes would only move that
    /// write onto the disk the commits use. The provider says which (<see cref="IPersistenceBackendProvider.StagesLiveCheckpointsInStore"/>).
    /// <para>The staging root is under the storage path, which belongs to this one store. A directory that
    /// several nodes can share, such as the scratch directory, is not used: each node's leftover sweep would
    /// see the others' staging trees as debris.</para>
    /// </summary>
    private static LiveCheckpointStaging? CreateLiveCheckpointStaging(KahunaConfiguration configuration, ILogger? logger)
    {
        if (configuration.StorageProviders.FindBackend(configuration.Storage) is not { StagesLiveCheckpointsInStore: true } provider ||
            string.IsNullOrWhiteSpace(configuration.StoragePath))
            return null;

        return new LiveCheckpointStaging(
            configuration.StoragePath,
            provider.DataDirectory(configuration.StoragePath, configuration.StorageRevision),
            [Path.Combine(configuration.StoragePath, LiveCheckpointStagingDirectoryName)],
            Math.Max(0, configuration.BackupCaptureThrottleBytesPerSec),
            logger);
    }

    internal const string LiveCheckpointStagingDirectoryName = "backup-staging";

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

    /// <summary>
    /// This node's share of a cluster-wide backup another node composes. A node with no backup directory
    /// refuses, so the composing node can name it; every other refusal comes from the service.
    /// </summary>
    internal Task<BackupPartResult> CaptureBackupPartAsync(BackupPartRequest request, IBackupPartSink sink, CancellationToken ct = default) =>
        service is null
            ? Task.FromResult(BackupPartResult.Refused(BackupPartOutcome.NotConfigured, "backup is not configured on that node"))
            : service.CaptureBackupPartAsync(request, sink, ct);

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
    private static KahunaBackupException MapAndLog(BackupService svc, Exception ex) =>
        MapAndLog(svc.LogOperationFailure, ex);

    private static KahunaBackupException MapAndLog(Action<string, Exception> logFailure, Exception ex)
    {
        string operationId = Guid.NewGuid().ToString("N")[..12];
        logFailure(operationId, ex);
        KahunaBackupException mapped = MapBackupException(ex);
        return new KahunaBackupException(mapped.Outcome, $"{mapped.Message} (operation {operationId})");
    }

    /// <summary>
    /// Maps an internal backup exception to a public, sanitized <see cref="KahunaBackupException"/>
    /// so callers classify failures by <see cref="KahunaBackupOutcome"/> rather than by internal
    /// exception type or message. Messages intentionally omit absolute paths and raw backend text.
    /// </summary>
    internal static KahunaBackupException MapBackupException(Exception ex) => ex switch
    {
        BackupDriverException d when d.NeedsFullBackup =>
            new(KahunaBackupOutcome.NeedsFull,
                d.TopologyDivergedFromParent
                    ? "An incremental backup could not be produced because the cluster topology changed " +
                      "since the parent chain was taken; take a full backup instead."
                    : "An incremental backup could not be produced because the WAL compaction floor " +
                      "advanced past the parent backup; take a full backup instead."),
        BackupDriverException d when d.ParentMissing =>
            new(KahunaBackupOutcome.ParentMissing, "The requested parent backup was not found."),
        BackupDriverException d when d.TargetConflict =>
            new(KahunaBackupOutcome.TargetConflict,
                "The restore destination already exists or overlaps a protected path."),
        BackupDriverException d when d.OutsidePitrWindow =>
            new(KahunaBackupOutcome.TargetOutsideCoverage,
                "The backup's restore point is older than the PITR retention window, so the leader would send a " +
                "full snapshot anyway; use a more recent backup, or join without the bootstrap."),
        BackupDriverException d when d.TargetOutsideCoverage =>
            new(KahunaBackupOutcome.TargetOutsideCoverage,
                "The requested restore time is outside the recoverable coverage of this backup chain."),
        BackupDriverException d when d.ExactCheckpointUnavailable =>
            new(KahunaBackupOutcome.ExactCheckpointUnavailable,
                "This node's storage backend cannot produce an exact as-of backup image."),
        // A replica that has not applied its partitions to the cut is a transient condition, so it shares the retryable
        // outcome of a share that could not be captured. The message passes through: it names only partition ids,
        // timestamps and node endpoints, and the caller needs the partitions to tell a stalled node from a slow one.
        BackupDriverException d when d.NotCaughtUp =>
            new(KahunaBackupOutcome.PartitionCaptureFailed, d.Message),
        BackupDriverException d when d.TopologyChanged =>
            new(KahunaBackupOutcome.TopologyChanged,
                "The cluster topology changed during the backup; nothing was published. Retry once stable."),
        BackupDriverException d when d.CutUnverified =>
            new(KahunaBackupOutcome.CutUnverified,
                "A transaction in flight during the capture could not be proven whole in the backup or in its chain's restore; nothing was published. Retry."),
        BackupDriverException d when d.RetryableLeadershipLoss =>
            new(KahunaBackupOutcome.RetryableLeadershipLoss,
                "Meta-partition leadership was lost during the backup; nothing was published. Retry against the leader."),
        // The full message passes through: it names only partition ids (no paths or backend text),
        // and the caller needs them to know which partitions the chain is missing.
        BackupDriverException d when d.RestrictedCoverage =>
            new(KahunaBackupOutcome.RestrictedCoverage, d.Message),
        // Passes through for the same reason: it names only partition ids and the node's endpoint.
        BackupDriverException d when d.PartitionCaptureFailed =>
            new(KahunaBackupOutcome.PartitionCaptureFailed, d.Message),
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

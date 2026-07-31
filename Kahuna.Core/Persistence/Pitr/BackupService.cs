using System.Buffers;
using System.Diagnostics;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.Communication.Rest;
using Kommander;
using Kommander.System;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Owns <see cref="BackupDriver"/> and <see cref="BackupCatalog"/> for a node.
/// Exposes full/incremental/coordinated backup, catalog listing, chain resolution,
/// and offline restore-to-directory operations.
/// </summary>
internal sealed class BackupService : IDisposable
{
    private readonly BackupDriver _driver;
    private readonly BackupCatalog _catalog;
    private readonly IRaft _raft;
    private readonly string _backupDir;
    private readonly string _storageType;
    private readonly string _storageRevision;
    private readonly string? _liveStoragePath;
    private readonly string? _restoreRoot;
    private readonly Func<int, long, IDisposable>? _acquireRetentionHold;
    private readonly ILogger? _logger;
    private readonly Func<Task<HLCTimestamp>> _queryMinInFlight;
    private readonly BackupRetentionPolicy _retentionPolicy;

    // Restore checkpoint-copy throughput budget in bytes/second; 0 = unlimited. Caps the bulk copy so a
    // restore does not saturate the disk and starve foreground traffic.
    private readonly long _copyThrottleBytesPerSec;

    // Serializes backup creation with garbage collection so a sweep never observes — and never
    // reclaims — an artifact directory that a backup is midway through writing. Backups are heavy and
    // infrequent, so serializing them (and GC) against each other is cheap and removes the race
    // entirely, without needing to track per-directory in-flight reservations.
    private readonly SemaphoreSlim _gcGate = new(1, 1);

    public BackupService(
        IRaft raft,
        IPersistenceBackend persistenceBackend,
        string backupDir,
        string storageType,
        string storageRevision,
        Func<Task> flushBeforeCheckpoint,
        Func<Task<HLCTimestamp>> queryMinInFlight,
        ILogger? logger = null,
        string? liveStoragePath = null,
        string? restoreRoot = null,
        Func<int, long, IDisposable>? acquireRetentionHold = null,
        BackupDriver.AcquireSnapshotHoldDelegate? acquireSnapshotHold = null,
        BackupDriver.ReleaseSnapshotHoldDelegate? releaseSnapshotHold = null,
        BackupDriver.RenewSnapshotHoldDelegate? renewSnapshotHold = null,
        int snapshotHoldLeaseMs = BackupDriver.DefaultSnapshotHoldLeaseMs,
        Func<int, HLCTimestamp>? appliedHlcProbe = null,
        BackupRetentionPolicy retentionPolicy = default,
        long copyThrottleBytesPerSec = 0)
    {
        _raft = raft;
        _backupDir = backupDir;
        _storageType = storageType;
        _storageRevision = storageRevision;
        _liveStoragePath = liveStoragePath;
        _restoreRoot = string.IsNullOrWhiteSpace(restoreRoot) ? null : Path.GetFullPath(restoreRoot);
        _acquireRetentionHold = acquireRetentionHold;
        _logger = logger;
        _driver = new BackupDriver(raft, persistenceBackend, flushBeforeCheckpoint,
            acquireSnapshotHold, releaseSnapshotHold, renewSnapshotHold, snapshotHoldLeaseMs, appliedHlcProbe);
        _catalog = new BackupCatalog(new LocalDirectoryStorageTarget(backupDir));
        _queryMinInFlight = queryMinInFlight;
        _retentionPolicy = retentionPolicy;
        _copyThrottleBytesPerSec = copyThrottleBytesPerSec;
    }

    public void Dispose()
    {
        _gcGate.Dispose();
    }

    public async Task<KahunaBackupInfo> TakeFullAsync(HLCTimestamp? snapshotT = null, CancellationToken ct = default)
    {
        await _gcGate.WaitAsync(ct).ConfigureAwait(false);
        long start = Stopwatch.GetTimestamp();
        try
        {
            BackupManifest manifest = await _driver.TakeFullBackupAsync(_backupDir, _catalog, snapshotT, ct);
            RecordBackupSuccess(manifest, start);
            KahunaBackupInfo dto = ToDto(manifest);
            TryRunGcLocked(ct);
            return dto;
        }
        catch { BackupIoMetrics.BackupFailures.Add(1); throw; }
        finally { _gcGate.Release(); }
    }

    public async Task<KahunaBackupInfo> TakeIncrementalAsync(
        Guid parentBackupId, HLCTimestamp? snapshotT = null, CancellationToken ct = default)
    {
        await _gcGate.WaitAsync(ct).ConfigureAwait(false);
        long start = Stopwatch.GetTimestamp();
        try
        {
            KahunaBackupInfo dto;
            try
            {
                BackupManifest manifest = _driver.TakeIncrementalBackup(
                    parentBackupId, _backupDir, _catalog, snapshotT, _acquireRetentionHold, ct);
                RecordBackupSuccess(manifest, start);
                dto = ToDto(manifest);
                dto.RequestedKind = BackupType.Incremental.ToString();
            }
            catch (BackupDriverException ex) when (ex.NeedsFullBackup)
            {
                _logger?.LogWarning(
                    "Incremental backup from {ParentId} not possible — WAL compaction floor advanced past " +
                    "parent range. Falling back to full backup. Reason: {Message}",
                    parentBackupId, ex.Message);

                BackupManifest full = await _driver.TakeFullBackupAsync(_backupDir, _catalog, snapshotT, ct);
                RecordBackupSuccess(full, start);
                dto = ToDto(full);
                // Make the substitution observable: the caller asked for an incremental and got a full.
                dto.RequestedKind = BackupType.Incremental.ToString();
                dto.SubstitutionReason =
                    "The WAL compaction floor advanced past the parent backup's range, so an incremental " +
                    "could not be produced; a full backup was taken instead.";
            }

            TryRunGcLocked(ct);
            return dto;
        }
        catch { BackupIoMetrics.BackupFailures.Add(1); throw; }
        finally { _gcGate.Release(); }
    }

    public async Task<KahunaBackupInfo> TakeCoordinatedBackupAsync(CancellationToken ct = default)
    {
        await _gcGate.WaitAsync(ct).ConfigureAwait(false);
        long start = Stopwatch.GetTimestamp();
        try
        {
            HLCTimestamp snapshotT = await SnapshotCoordinator.ComputeSafeSnapshotTimeAsync(
                _queryMinInFlight, _raft.WalAdapter, _raft.GetPartitionMap(), ct);
            BackupManifest manifest = await _driver.TakeFullBackupAsync(_backupDir, _catalog, snapshotT, ct);
            RecordBackupSuccess(manifest, start);
            KahunaBackupInfo dto = ToDto(manifest);
            TryRunGcLocked(ct);
            return dto;
        }
        catch { BackupIoMetrics.BackupFailures.Add(1); throw; }
        finally { _gcGate.Release(); }
    }

    private static void RecordBackupSuccess(BackupManifest manifest, long startTimestamp)
    {
        long bytes = 0;
        foreach (long size in manifest.Sizes.Values)
            bytes += size;
        BackupIoMetrics.BackupOperations.Add(1);
        BackupIoMetrics.BackupBytes.Add(bytes);
        BackupIoMetrics.BackupDurationMs.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalMilliseconds);
    }

    public async Task<HLCTimestamp> ComputeSafeSnapshotTimeAsync(CancellationToken ct = default) =>
        await SnapshotCoordinator.ComputeSafeSnapshotTimeAsync(
            _queryMinInFlight, _raft.WalAdapter, _raft.GetPartitionMap(), ct);

    /// <summary>
    /// The set of ids the orphan sweep must never reclaim: every backup that has a manifest file,
    /// including ones whose manifest is corrupt, structurally invalid, or transiently unreadable. GC
    /// serializes with backup creation via <c>_gcGate</c>, so no in-flight backup needs separate
    /// reservation — the danger is the reverse: a directory whose manifest failed to parse is absent
    /// from the valid-id set and would otherwise be swept as an orphan, destroying the very artifacts an
    /// operator needs to diagnose or repair. Deriving protection from manifest <i>presence</i> (a
    /// filename scan that never reads contents) fails closed regardless of manifest health.
    /// </summary>
    private HashSet<Guid> ProtectedManifestIds(CancellationToken ct) =>
        _catalog.ListManifestIds(ct).ToHashSet();

    /// <summary>
    /// Dry-run inventory: what a garbage-collection pass would reclaim right now — orphaned/leftover
    /// artifacts and, when a retention policy is configured, whole chains beyond its bounds — each with
    /// a reason. Read-only; deletes nothing.
    /// </summary>
    internal BackupGcInventory PlanGarbageCollection(CancellationToken ct = default)
    {
        IReadOnlyList<BackupManifest> manifests = _catalog.List(ct);
        HashSet<Guid> validIds = manifests.Select(m => m.BackupId).ToHashSet();
        IReadOnlyList<OrphanSweepCandidate> orphans =
            BackupRetention.PlanOrphanSweep(_backupDir, validIds, ProtectedManifestIds(ct), ct);
        IReadOnlyList<BackupGcCandidate> retention = _retentionPolicy.IsEnabled
            ? BackupRetention.PlanRetention(manifests, _retentionPolicy, DateTime.UtcNow)
            : [];
        return new BackupGcInventory(retention, orphans);
    }

    /// <summary>
    /// Reclaims orphaned/leftover artifacts and enforces the retention policy, serialized against backup
    /// creation. Returns what was reclaimed. Safe to call on startup and on a periodic tick.
    /// </summary>
    internal async Task<BackupGcInventory> RunGarbageCollectionAsync(CancellationToken ct = default)
    {
        await _gcGate.WaitAsync(ct).ConfigureAwait(false);
        try { return RunGarbageCollectionLocked(ct); }
        finally { _gcGate.Release(); }
    }

    // Must be called while _gcGate is held. Sweeps orphans first (always), then applies retention (only
    // when a policy is configured), deleting whole chains descendants-first.
    private BackupGcInventory RunGarbageCollectionLocked(CancellationToken ct)
    {
        IReadOnlyList<BackupManifest> manifests = _catalog.List(ct);
        HashSet<Guid> validIds = manifests.Select(m => m.BackupId).ToHashSet();

        IReadOnlyList<OrphanSweepCandidate> orphans =
            BackupRetention.PlanOrphanSweep(_backupDir, validIds, ProtectedManifestIds(ct), ct);
        BackupRetention.ApplyOrphanSweep(orphans, ct);

        IReadOnlyList<BackupGcCandidate> retention = _retentionPolicy.IsEnabled
            ? BackupRetention.PlanRetention(manifests, _retentionPolicy, DateTime.UtcNow)
            : [];
        BackupRetention.ApplyRetention(retention, _catalog, _backupDir, ct);

        long bytesReclaimed = 0;
        foreach (BackupGcCandidate c in retention)
            bytesReclaimed += c.Bytes;

        BackupGcMetrics.Runs.Add(1);
        if (orphans.Count > 0)
            BackupGcMetrics.OrphansReclaimed.Add(orphans.Count);
        if (retention.Count > 0)
        {
            BackupGcMetrics.RetentionDeletions.Add(retention.Count);
            BackupGcMetrics.BytesReclaimed.Add(bytesReclaimed);
        }

        if ((orphans.Count > 0 || retention.Count > 0) && _logger is not null && _logger.IsEnabled(LogLevel.Information))
            _logger.LogInformation(
                "Backup GC reclaimed {Orphans} orphan/leftover artifact(s) and deleted {Retained} out-of-retention backup(s) ({Bytes} bytes) under {Dir}.",
                orphans.Count, retention.Count, bytesReclaimed, _backupDir);

        return new BackupGcInventory(retention, orphans);
    }

    // Best-effort GC after a successful backup (gate already held). A GC failure must never fail the
    // backup that just succeeded, so everything is swallowed and logged.
    private void TryRunGcLocked(CancellationToken ct)
    {
        try { RunGarbageCollectionLocked(ct); }
        catch (Exception ex) { _logger?.LogWarning(ex, "Post-backup GC pass failed; the backup itself succeeded."); }
    }

    /// <summary>
    /// Lists every backup, marking invalid ones without hiding them. Each parsed manifest gets a cheap,
    /// manifest-only structural check (format version, schema, checksum/size keysets); when
    /// <paramref name="verifyArtifacts"/> is set, a structurally-valid entry is additionally verified
    /// against its on-disk files and digests (the expensive path — cancellable via <paramref name="ct"/>).
    /// A single malformed entry never fails the whole list; manifests that fail to deserialize are
    /// surfaced as explicit invalid entries.
    /// </summary>
    public IReadOnlyList<KahunaBackupInfo> ListBackups(bool verifyArtifacts = false, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        List<KahunaBackupInfo> result = [];

        foreach (BackupManifest manifest in _catalog.List(ct))
        {
            ct.ThrowIfCancellationRequested();

            KahunaBackupInfo dto = ToDto(manifest);
            try
            {
                string? structuralError = BackupArtifactVerifier.GetStructuralError(manifest);
                if (structuralError is not null)
                {
                    dto.IsInvalid = true;
                    dto.InvalidReason = structuralError;
                }
                else if (verifyArtifacts)
                {
                    try
                    {
                        BackupArtifactVerifier.Verify(manifest, _backupDir, ct);
                    }
                    catch (Exception ex) when (ex is BackupArtifactException or BackupUnsupportedFormatException)
                    {
                        dto.IsInvalid = true;
                        dto.InvalidReason = ex.Message;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // A single unexpected error must never fail the whole list — mark this entry invalid.
                dto.IsInvalid = true;
                dto.InvalidReason = $"Structural validation error: {ex.Message}";
            }

            result.Add(dto);
        }

        // Surface corrupt/unreadable (non-deserializable) manifests as explicit invalid entries too.
        foreach ((Guid backupId, string reason) in _catalog.ListCorrupt(ct))
            result.Add(new KahunaBackupInfo { BackupId = backupId, IsInvalid = true, InvalidReason = reason });

        return result;
    }

    public IReadOnlyList<KahunaBackupInfo> ResolveAndValidate(Guid leafBackupId, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        IReadOnlyList<BackupManifest> chain = _catalog.ResolveAndValidate(leafBackupId, ct);
        (HLCTimestamp? min, HLCTimestamp max) = BackupChainCoverage.Compute(chain);

        List<KahunaBackupInfo> dtos = chain.Select(ToDto).ToList();

        // Expose the chain's recoverable window on the head (Full) entry so callers can validate a
        // restore target against exact bounds instead of a wall-clock heuristic. The lower bound is
        // left null when the base cut is unknown (legacy full) — never advertised as zero.
        if (dtos.Count > 0)
        {
            dtos[0].MinRecoverablePhysicalMs = min?.L;
            dtos[0].MaxRecoverablePhysicalMs = max.L;
        }

        return dtos;
    }

    public IReadOnlyList<KahunaBackupInfo> ValidateChain(Guid leafBackupId, CancellationToken ct = default) =>
        ResolveAndValidate(leafBackupId, ct);

    /// <summary>
    /// Offline restore: produces a populated storage-engine directory at <paramref name="targetDir"/>
    /// by copying the Full backup's checkpoint and replaying incremental WAL segments up to
    /// <paramref name="targetTime"/>. The operator can then start a fresh standalone node with
    /// <c>--storage-path=targetDir --storage-revision={revision}</c> (omit revision for memory).
    /// No WAL seeding is performed; reads fall back to the persistence backend for durability=Persistent keys.
    /// </summary>
    public async Task<KahunaRestoreResponse> RestoreToAsync(
        Guid leafBackupId,
        string targetDir,
        HLCTimestamp targetTime,
        TimeSpan? pitrWindow = null,
        CancellationToken ct = default)
    {
        long start = Stopwatch.GetTimestamp();
        try
        {
        ct.ThrowIfCancellationRequested();

        IReadOnlyList<BackupManifest> chain = _catalog.ResolveAndValidate(leafBackupId, ct);

        // Verify every artifact (size + digest, safe paths, no missing/extra/symlinked files) up
        // front — before copying anything into targetDir — so a corrupt or legacy chain fails closed
        // without producing a partial or misleading restore target. Always invoked (empty checksums
        // are handled by the verifier; they are not a reason to skip).
        foreach (BackupManifest m in chain)
        {
            ct.ThrowIfCancellationRequested();
            BackupArtifactVerifier.Verify(m, _backupDir, ct);
        }

        // Full backup is always chain[0]; its checkpoint is the base image.
        BackupManifest fullBackup = chain[0];
        string checkpointSrc = Path.Combine(_backupDir, fullBackup.BackupId.ToString("N"), "checkpoint");

        if (!Directory.Exists(checkpointSrc))
            throw new BackupDriverException(
                $"Checkpoint directory not found for full backup {fullBackup.BackupId:N}: {checkpointSrc}");

        // Validate the target against the chain's exact coverage (fails closed on an unknown lower
        // bound or an out-of-range target), resolving Zero to the natural end. Wall-clock age is
        // irrelevant. Then recover the bounds for the response.
        targetTime = BackupChainCoverage.Resolve(chain, targetTime);
        (HLCTimestamp? minOpt, HLCTimestamp maxCover) = BackupChainCoverage.Compute(chain);
        HLCTimestamp min = minOpt!.Value; // Resolve guaranteed a non-null lower bound

        // Resolve and confine the destination, then build the whole restore in a private staging
        // sibling and publish it with a single atomic rename. A crash/cancel/validation failure
        // mid-restore therefore never leaves a partial or misleading directory at the final path.
        string finalDir = Path.GetFullPath(targetDir);
        EnsureRestoreDestinationSafe(finalDir);

        // Collision-safe staging sibling: never delete a pre-existing name (it could be unrelated) —
        // pick a fresh, non-existent path.
        string staging = NewStagingPath(finalDir);

        RestoreResult result;
        try
        {
            ct.ThrowIfCancellationRequested();

            // RocksDB opens at {dir}/{revision}/ (revision is a subdir). SQLite embeds the revision
            // in the filename, so files live directly in {dir}/. Copy the checkpoint accordingly.
            string checkpointDest = _storageType == "rocksdb" && !string.IsNullOrEmpty(_storageRevision)
                ? Path.Combine(staging, _storageRevision)
                : staging;

            Directory.CreateDirectory(checkpointDest);
            await CopyDirectoryAsync(checkpointSrc, checkpointDest, _copyThrottleBytesPerSec, ct).ConfigureAwait(false);

            // Verify the bytes actually staged (and about to be opened) against the manifest — closes
            // the verify-then-use gap for the base image even if the source changed after the check.
            BackupArtifactVerifier.VerifyCheckpointCopy(fullBackup, checkpointDest, ct);

            IPersistenceBackend targetBackend = OpenBackendAt(staging);
            try
            {
                // alreadyVerified:false → RestoreEngine re-verifies each incremental immediately
                // before replaying it (point-of-use), minimizing the verify-then-use window on WAL
                // segments read from the source artifacts.
                result = await RestoreEngine.RestoreAsync(
                    chain, _backupDir, targetTime, targetBackend, pitrWindow, nowUtc: null, alreadyVerified: false, ct: ct).ConfigureAwait(false);

                // For memory backends, StoreKeyValues only updates the in-memory object; the files in
                // staging are still the Full backup's state. Flush the merged result back to disk so a
                // subsequent OpenCheckpoint(staging) sees all applied entries.
                if (_storageType is "memory" or "" && result.EntriesApplied > 0)
                {
                    string mergeTmp = staging + ".merge_" + Guid.NewGuid().ToString("N")[..8];
                    targetBackend.CreateCheckpoint(mergeTmp, result.LastAppliedIndex, result.LastAppliedTime);
                    TryDeleteDirectory(staging);
                    Directory.Move(mergeTmp, staging);
                }
            }
            finally
            {
                // Release engine file handles (RocksDB/SQLite) before renaming the staging tree.
                (targetBackend as IDisposable)?.Dispose();
            }

            ct.ThrowIfCancellationRequested();

            // Revalidate at publish: an ancestor could have been swapped for a symlink, or the
            // destination created, between the initial check and now (raced-ancestor / TOCTOU).
            EnsureRestoreDestinationSafe(finalDir);

            // Atomic publish: refuse to overwrite an existing tree.
            if (Directory.Exists(finalDir))
                throw new BackupDriverException(
                    $"Restore destination already exists: {finalDir}") { TargetConflict = true };

            Directory.Move(staging, finalDir);
        }
        catch
        {
            TryDeleteOrQuarantine(staging);
            throw;
        }

        long restoredBytes = 0;
        foreach (BackupManifest m in chain)
            foreach (long size in m.Sizes.Values)
                restoredBytes += size;
        BackupIoMetrics.RestoreOperations.Add(1);
        BackupIoMetrics.RestoreBytes.Add(restoredBytes);
        BackupIoMetrics.RestoreEntriesApplied.Add(result.EntriesApplied);
        BackupIoMetrics.RestoreDurationMs.Record(Stopwatch.GetElapsedTime(start).TotalMilliseconds);

        return new KahunaRestoreResponse
        {
            TargetDir = finalDir,
            PartitionsRestored = result.PartitionsRestored,
            EntriesApplied = result.EntriesApplied,
            LastAppliedPhysicalMs = result.LastAppliedTime.L,
            Chain = chain.Select(ToDto).ToList(),
            Outcome = KahunaBackupOutcome.Ok,
            MinRecoverablePhysicalMs = min.L,
            MaxRecoverablePhysicalMs = maxCover.L
        };
        }
        catch
        {
            BackupIoMetrics.RestoreFailures.Add(1);
            throw;
        }
    }

    /// <summary>
    /// Rejects a restore destination that is unsafe to create or publish into: a symlink on the
    /// destination or ANY existing ancestor (which could redirect the write/rename elsewhere), an
    /// already-populated directory (never overwrite), a path outside a configured server-owned restore
    /// root, or a path overlapping the backup root / live storage path.
    /// </summary>
    private void EnsureRestoreDestinationSafe(string finalDir)
    {
        // A symlinked destination leaf could redirect the atomic rename elsewhere.
        if (Directory.Exists(finalDir) &&
            (new DirectoryInfo(finalDir).Attributes & FileAttributes.ReparsePoint) != 0)
            throw new BackupDriverException(
                $"Restore destination is a symlink, which is not allowed: {finalDir}") { TargetConflict = true };

        if (Directory.Exists(finalDir) && Directory.EnumerateFileSystemEntries(finalDir).Any())
            throw new BackupDriverException(
                $"Restore destination already exists and is not empty: {finalDir}") { TargetConflict = true };

        // When a server-owned restore root is configured, the destination must be canonically
        // contained within it, and no ancestor BELOW the root may be a symlink — an attacker with
        // write access under the root could otherwise plant a symlinked ancestor that redirects the
        // write outside the root. Ancestors at/above the root are the trusted server-owned path
        // (e.g. legitimate system symlinks like /var → /private/var) and are not walked.
        if (_restoreRoot is not null)
        {
            if (!IsWithin(finalDir, _restoreRoot))
                throw new BackupDriverException(
                    "Restore destination is outside the configured server-owned restore root; " +
                    "choose a path under it.") { TargetConflict = true };

            string rootNorm = Path.TrimEndingDirectorySeparator(Path.GetFullPath(_restoreRoot));
            for (string? p = finalDir; !string.IsNullOrEmpty(p); p = Path.GetDirectoryName(p))
            {
                string pn = Path.TrimEndingDirectorySeparator(Path.GetFullPath(p));
                // Case-sensitive (ordinal) stop condition: on a case-sensitive volume a case-different
                // sibling is a distinct tree, not the trusted root — it must NOT stop the walk (so it
                // is still checked for redirection). Being ordinal is fail-closed on every volume.
                if (string.Equals(pn, rootNorm, StringComparison.Ordinal))
                    break; // reached the trusted root

                if (Directory.Exists(p) && (new DirectoryInfo(p).Attributes & FileAttributes.ReparsePoint) != 0)
                    throw new BackupDriverException(
                        $"Restore destination path passes through a symlink under the restore root ('{p}'), " +
                        "which is not allowed.") { TargetConflict = true };
            }
        }

        foreach (string? forbidden in new[] { _backupDir, _liveStoragePath })
        {
            if (string.IsNullOrEmpty(forbidden))
                continue;

            if (PathsOverlap(finalDir, Path.GetFullPath(forbidden)))
                throw new BackupDriverException(
                    "Restore destination overlaps a protected path (the backup root or the live " +
                    "storage path); choose a separate directory.") { TargetConflict = true };
        }
    }

    /// <summary>Returns a fresh, non-existent staging sibling of <paramref name="finalDir"/>.</summary>
    private static string NewStagingPath(string finalDir)
    {
        for (int i = 0; i < 100; i++)
        {
            string candidate = finalDir + ".staging_" + Guid.NewGuid().ToString("N")[..8];
            if (!Directory.Exists(candidate) && !File.Exists(candidate))
                return candidate;
        }
        throw new BackupDriverException("Could not allocate a unique restore staging directory.")
            { TargetConflict = true };
    }

    /// <summary>
    /// True when <paramref name="path"/> is the same as or nested under <paramref name="root"/>.
    /// Comparison is case-SENSITIVE (ordinal): on a case-sensitive volume a case-different sibling
    /// (e.g. <c>/srv/restore/out</c> vs root <c>/srv/Restore</c>) is a distinct tree and must never be
    /// accepted as "within". Ordinal is fail-closed on every volume — on a case-insensitive volume it
    /// only over-rejects a case-variant of an otherwise-contained path, never accepts an escape.
    /// </summary>
    private static bool IsWithin(string path, string root)
    {
        string np = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
        string nr = Path.TrimEndingDirectorySeparator(Path.GetFullPath(root));
        return string.Equals(np, nr, StringComparison.Ordinal)
            || np.StartsWith(nr + Path.DirectorySeparatorChar, StringComparison.Ordinal);
    }

    /// <summary>True when <paramref name="a"/> and <paramref name="b"/> are the same directory or
    /// one is nested inside the other. Comparison is case-INSENSITIVE — the conservative direction for
    /// an overlap check: on a case-insensitive volume case-variant paths ARE the same tree, so
    /// treating them as overlapping catches a restore that would land in a protected path. On a
    /// case-sensitive volume this only over-detects overlap (rejects a case-variant sibling), never
    /// misses one.</summary>
    private static bool PathsOverlap(string a, string b)
    {
        static string Norm(string p) =>
            Path.TrimEndingDirectorySeparator(Path.GetFullPath(p));

        string na = Norm(a);
        string nb = Norm(b);
        char sep = Path.DirectorySeparatorChar;

        return string.Equals(na, nb, StringComparison.OrdinalIgnoreCase)
            || na.StartsWith(nb + sep, StringComparison.OrdinalIgnoreCase)
            || nb.StartsWith(na + sep, StringComparison.OrdinalIgnoreCase);
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // best-effort
        }
    }

    /// <summary>Deletes the staging tree; if deletion fails, renames it to a quarantine name so it
    /// can never be mistaken for a completed restore.</summary>
    private void TryDeleteOrQuarantine(string staging)
    {
        try
        {
            if (Directory.Exists(staging))
                Directory.Delete(staging, recursive: true);
        }
        catch
        {
            try
            {
                if (Directory.Exists(staging))
                    Directory.Move(staging, staging + ".quarantine_" + Guid.NewGuid().ToString("N")[..8]);
            }
            catch
            {
                _logger?.LogWarning("Failed to clean up restore staging directory {Staging}", staging);
            }
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────────

    private IPersistenceBackend OpenBackendAt(string path) => _storageType switch
    {
        "rocksdb" => new RocksDbPersistenceBackend(path, _storageRevision),
        "sqlite"  => new SqlitePersistenceBackend(path, _storageRevision, _logger),
        _         => MemoryPersistenceBackend.OpenCheckpoint(path)
    };

    private const int CopyBufferSize = 1 << 20; // 1 MiB streamed copy chunks — bounded memory per file

    private static async Task CopyDirectoryAsync(
        string source, string destination, long throttleBytesPerSec, CancellationToken ct = default)
    {
        Directory.CreateDirectory(destination);
        foreach (string file in Directory.GetFiles(source))
        {
            ct.ThrowIfCancellationRequested();
            // Never follow a symlink out of the source tree — a reparse point could redirect the
            // read to arbitrary bytes outside the verified artifact.
            if ((File.GetAttributes(file) & FileAttributes.ReparsePoint) != 0)
                throw new BackupArtifactException($"Backup artifact contains a symlink/reparse point: '{file}'.");
            await CopyFileAsync(file, Path.Combine(destination, Path.GetFileName(file)), throttleBytesPerSec, ct).ConfigureAwait(false);
        }
        foreach (string subDir in Directory.GetDirectories(source))
        {
            if ((File.GetAttributes(subDir) & FileAttributes.ReparsePoint) != 0)
                throw new BackupArtifactException($"Backup artifact contains a symlinked directory: '{subDir}'.");
            await CopyDirectoryAsync(subDir, Path.Combine(destination, Path.GetFileName(subDir)), throttleBytesPerSec, ct).ConfigureAwait(false);
        }
    }

    // Streamed async file copy — the checkpoint copy is the largest single I/O of a restore; streaming
    // it asynchronously keeps a fixed 1 MiB buffer resident and does not block the calling thread. When
    // a throughput budget is set, the copy is paced to stay at or under it so it does not saturate the
    // disk and starve foreground traffic.
    private static async Task CopyFileAsync(string source, string destination, long throttleBytesPerSec, CancellationToken ct)
    {
        await using FileStream src = new(
            source, FileMode.Open, FileAccess.Read, FileShare.Read, CopyBufferSize, useAsync: true);
        await using FileStream dst = new(
            destination, FileMode.Create, FileAccess.Write, FileShare.None, CopyBufferSize, useAsync: true);

        if (throttleBytesPerSec <= 0)
        {
            await src.CopyToAsync(dst, CopyBufferSize, ct).ConfigureAwait(false);
            return;
        }

        byte[] buffer = ArrayPool<byte>.Shared.Rent(CopyBufferSize);
        try
        {
            long copied = 0;
            long start = Stopwatch.GetTimestamp();
            int n;
            while ((n = await src.ReadAsync(buffer.AsMemory(0, CopyBufferSize), ct).ConfigureAwait(false)) > 0)
            {
                await dst.WriteAsync(buffer.AsMemory(0, n), ct).ConfigureAwait(false);
                copied += n;
                double sleep = ThrottleDelaySeconds(copied, throttleBytesPerSec, Stopwatch.GetElapsedTime(start).TotalSeconds);
                if (sleep > 0.001)
                    await Task.Delay(TimeSpan.FromSeconds(sleep), ct).ConfigureAwait(false);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>
    /// How long (seconds) to pause so that <paramref name="bytesCopied"/> bytes over the copy so far have
    /// taken at least <c>bytesCopied / throttleBytesPerSec</c> seconds — i.e. throughput stays at or under
    /// the budget. Returns 0 when the copy is at or behind the target rate (no pause needed) or when the
    /// budget is non-positive (unlimited). Pure — no clock access — so it is unit-testable.
    /// </summary>
    internal static double ThrottleDelaySeconds(long bytesCopied, long throttleBytesPerSec, double elapsedSeconds)
    {
        if (throttleBytesPerSec <= 0)
            return 0;
        double targetSeconds = (double)bytesCopied / throttleBytesPerSec;
        double delay = targetSeconds - elapsedSeconds;
        return delay > 0 ? delay : 0;
    }

    private static KahunaBackupInfo ToDto(BackupManifest m) => new()
    {
        BackupId = m.BackupId,
        FormatVersion = m.FormatVersion,
        Type = m.Type.ToString(),
        CreatedAtUtc = m.CreatedAtUtc,
        ParentBackupId = m.ParentBackupId,
        PartitionCount = m.PartitionRanges.Count,
        ClusterSnapshotNode = m.ClusterSnapshotNode,
        ClusterSnapshotPhysical = m.ClusterSnapshotPhysical,
        ClusterSnapshotCounter = m.ClusterSnapshotCounter,
        RequestedKind = m.Type.ToString(),
        ActualKind = m.Type.ToString()
    };
}

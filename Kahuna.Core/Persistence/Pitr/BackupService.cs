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
internal sealed class BackupService
{
    private readonly BackupDriver _driver;
    private readonly BackupCatalog _catalog;
    private readonly IRaft _raft;
    private readonly string _backupDir;
    private readonly string _storageType;
    private readonly string _storageRevision;
    private readonly string? _liveStoragePath;
    private readonly string? _restoreRoot;
    private readonly ILogger? _logger;
    private readonly Func<Task<HLCTimestamp>> _queryMinInFlight;

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
        string? restoreRoot = null)
    {
        _raft = raft;
        _backupDir = backupDir;
        _storageType = storageType;
        _storageRevision = storageRevision;
        _liveStoragePath = liveStoragePath;
        _restoreRoot = string.IsNullOrWhiteSpace(restoreRoot) ? null : Path.GetFullPath(restoreRoot);
        _logger = logger;
        _driver = new BackupDriver(raft, persistenceBackend, flushBeforeCheckpoint);
        _catalog = new BackupCatalog(new LocalDirectoryStorageTarget(backupDir));
        _queryMinInFlight = queryMinInFlight;
    }

    public async Task<KahunaBackupInfo> TakeFullAsync(HLCTimestamp? snapshotT = null, CancellationToken ct = default)
    {
        BackupManifest manifest = await _driver.TakeFullBackupAsync(_backupDir, _catalog, snapshotT, ct);
        return ToDto(manifest);
    }

    public async Task<KahunaBackupInfo> TakeIncrementalAsync(
        Guid parentBackupId, HLCTimestamp? snapshotT = null, CancellationToken ct = default)
    {
        try
        {
            BackupManifest manifest = _driver.TakeIncrementalBackup(parentBackupId, _backupDir, _catalog, snapshotT, ct);
            KahunaBackupInfo dto = ToDto(manifest);
            dto.RequestedKind = BackupType.Incremental.ToString();
            return dto;
        }
        catch (BackupDriverException ex) when (ex.NeedsFullBackup)
        {
            _logger?.LogWarning(
                "Incremental backup from {ParentId} not possible — WAL compaction floor advanced past " +
                "parent range. Falling back to full backup. Reason: {Message}",
                parentBackupId, ex.Message);

            BackupManifest full = await _driver.TakeFullBackupAsync(_backupDir, _catalog, snapshotT, ct);
            KahunaBackupInfo dto = ToDto(full);
            // Make the substitution observable: the caller asked for an incremental and got a full.
            dto.RequestedKind = BackupType.Incremental.ToString();
            dto.SubstitutionReason =
                "The WAL compaction floor advanced past the parent backup's range, so an incremental " +
                "could not be produced; a full backup was taken instead.";
            return dto;
        }
    }

    public async Task<KahunaBackupInfo> TakeCoordinatedBackupAsync(CancellationToken ct = default)
    {
        HLCTimestamp snapshotT = await SnapshotCoordinator.ComputeSafeSnapshotTimeAsync(
            _queryMinInFlight, _raft.WalAdapter, _raft.GetPartitionMap(), ct);
        BackupManifest manifest = await _driver.TakeFullBackupAsync(_backupDir, _catalog, snapshotT, ct);
        return ToDto(manifest);
    }

    public async Task<HLCTimestamp> ComputeSafeSnapshotTimeAsync(CancellationToken ct = default) =>
        await SnapshotCoordinator.ComputeSafeSnapshotTimeAsync(
            _queryMinInFlight, _raft.WalAdapter, _raft.GetPartitionMap(), ct);

    public IReadOnlyList<KahunaBackupInfo> ListBackups(CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        List<KahunaBackupInfo> result = _catalog.List(ct).Select(ToDto).ToList();

        // Surface corrupt/unreadable manifests as explicit invalid entries rather than hiding them.
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
    public KahunaRestoreResponse RestoreTo(
        Guid leafBackupId,
        string targetDir,
        HLCTimestamp targetTime,
        TimeSpan? pitrWindow = null,
        CancellationToken ct = default)
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
            CopyDirectory(checkpointSrc, checkpointDest, ct);

            // Verify the bytes actually staged (and about to be opened) against the manifest — closes
            // the verify-then-use gap for the base image even if the source changed after the check.
            BackupArtifactVerifier.VerifyCheckpointCopy(fullBackup, checkpointDest, ct);

            IPersistenceBackend targetBackend = OpenBackendAt(staging);
            try
            {
                // alreadyVerified:false → RestoreEngine re-verifies each incremental immediately
                // before replaying it (point-of-use), minimizing the verify-then-use window on WAL
                // segments read from the source artifacts.
                result = RestoreEngine.Restore(
                    chain, _backupDir, targetTime, targetBackend, pitrWindow, nowUtc: null, ct, alreadyVerified: false);

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
                if (string.Equals(pn, rootNorm, StringComparison.OrdinalIgnoreCase))
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

    /// <summary>True when <paramref name="path"/> is the same as or nested under <paramref name="root"/>.</summary>
    private static bool IsWithin(string path, string root)
    {
        string np = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
        string nr = Path.TrimEndingDirectorySeparator(Path.GetFullPath(root));
        return string.Equals(np, nr, StringComparison.OrdinalIgnoreCase)
            || np.StartsWith(nr + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>True when <paramref name="a"/> and <paramref name="b"/> are the same directory or
    /// one is nested inside the other. Comparison is case-insensitive to be safe on case-insensitive
    /// filesystems (macOS/Windows).</summary>
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

    private static void CopyDirectory(string source, string destination, CancellationToken ct = default)
    {
        Directory.CreateDirectory(destination);
        foreach (string file in Directory.GetFiles(source))
        {
            ct.ThrowIfCancellationRequested();
            // Never follow a symlink out of the source tree — a reparse point could redirect the
            // read to arbitrary bytes outside the verified artifact.
            if ((File.GetAttributes(file) & FileAttributes.ReparsePoint) != 0)
                throw new BackupArtifactException($"Backup artifact contains a symlink/reparse point: '{file}'.");
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)), overwrite: true);
        }
        foreach (string subDir in Directory.GetDirectories(source))
        {
            if ((File.GetAttributes(subDir) & FileAttributes.ReparsePoint) != 0)
                throw new BackupArtifactException($"Backup artifact contains a symlinked directory: '{subDir}'.");
            CopyDirectory(subDir, Path.Combine(destination, Path.GetFileName(subDir)), ct);
        }
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

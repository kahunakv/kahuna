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
        ILogger? logger = null)
    {
        _raft = raft;
        _backupDir = backupDir;
        _storageType = storageType;
        _storageRevision = storageRevision;
        _logger = logger;
        _driver = new BackupDriver(raft, persistenceBackend, flushBeforeCheckpoint);
        _catalog = new BackupCatalog(new LocalDirectoryStorageTarget(backupDir));
        _queryMinInFlight = queryMinInFlight;
    }

    public async Task<KahunaBackupInfo> TakeFullAsync(HLCTimestamp? snapshotT = null)
    {
        BackupManifest manifest = await _driver.TakeFullBackupAsync(_backupDir, _catalog, snapshotT);
        return ToDto(manifest);
    }

    public async Task<KahunaBackupInfo> TakeIncrementalAsync(Guid parentBackupId, HLCTimestamp? snapshotT = null)
    {
        try
        {
            BackupManifest manifest = _driver.TakeIncrementalBackup(parentBackupId, _backupDir, _catalog, snapshotT);
            return ToDto(manifest);
        }
        catch (BackupDriverException ex) when (ex.NeedsFullBackup)
        {
            _logger?.LogWarning(
                "Incremental backup from {ParentId} not possible — WAL compaction floor advanced past " +
                "parent range. Falling back to full backup. Reason: {Message}",
                parentBackupId, ex.Message);

            BackupManifest full = await _driver.TakeFullBackupAsync(_backupDir, _catalog, snapshotT);
            return ToDto(full);
        }
    }

    public async Task<KahunaBackupInfo> TakeCoordinatedBackupAsync()
    {
        HLCTimestamp snapshotT = await SnapshotCoordinator.ComputeSafeSnapshotTimeAsync(
            _queryMinInFlight, _raft.WalAdapter, _raft.GetPartitionMap());
        BackupManifest manifest = await _driver.TakeFullBackupAsync(_backupDir, _catalog, snapshotT);
        return ToDto(manifest);
    }

    public async Task<HLCTimestamp> ComputeSafeSnapshotTimeAsync() =>
        await SnapshotCoordinator.ComputeSafeSnapshotTimeAsync(
            _queryMinInFlight, _raft.WalAdapter, _raft.GetPartitionMap());

    public IReadOnlyList<KahunaBackupInfo> ListBackups() =>
        _catalog.List().Select(ToDto).ToList();

    public IReadOnlyList<KahunaBackupInfo> ResolveAndValidate(Guid leafBackupId) =>
        _catalog.ResolveAndValidate(leafBackupId).Select(ToDto).ToList();

    public IReadOnlyList<KahunaBackupInfo> ValidateChain(Guid leafBackupId) =>
        _catalog.ResolveAndValidate(leafBackupId).Select(ToDto).ToList();

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
        TimeSpan? pitrWindow = null)
    {
        IReadOnlyList<BackupManifest> chain = _catalog.ResolveAndValidate(leafBackupId);

        // Full backup is always chain[0]; its checkpoint is the base image.
        BackupManifest fullBackup = chain[0];
        string checkpointSrc = Path.Combine(_backupDir, fullBackup.BackupId.ToString("N"), "checkpoint");

        if (!Directory.Exists(checkpointSrc))
            throw new BackupDriverException(
                $"Checkpoint directory not found for full backup {fullBackup.BackupId:N}: {checkpointSrc}");

        // Zero means "restore to the chain's natural end" — mirror the bootstrap path.
        if (targetTime == HLCTimestamp.Zero)
        {
            targetTime = chain
                .SelectMany(m => m.PartitionRanges)
                .Select(r => r.ToHlc)
                .Aggregate(HLCTimestamp.Zero, (acc, hlc) => hlc.CompareTo(acc) > 0 ? hlc : acc);
        }

        // RocksDB opens at {targetDir}/{revision}/ (revision is a subdir).
        // SQLite embeds the revision in the filename, so files live directly in {targetDir}/.
        // Copy the checkpoint into the right location so OpenBackendAt can open it.
        string checkpointDest = _storageType == "rocksdb" && !string.IsNullOrEmpty(_storageRevision)
            ? Path.Combine(targetDir, _storageRevision)
            : targetDir;

        Directory.CreateDirectory(checkpointDest);
        CopyDirectory(checkpointSrc, checkpointDest);

        IPersistenceBackend targetBackend = OpenBackendAt(targetDir);
        RestoreResult result = RestoreEngine.Restore(chain, _backupDir, targetTime, targetBackend, pitrWindow);

        // For memory backends, StoreKeyValues only updates the in-memory object; the checkpoint
        // files in targetDir are still the Full backup's state.  Flush the merged result back
        // to disk so a subsequent OpenCheckpoint(targetDir) sees all applied entries.
        if (_storageType is "memory" or "" && result.EntriesApplied > 0)
        {
            string tempPath = targetDir + ".merge_" + Guid.NewGuid().ToString("N")[..8];
            targetBackend.CreateCheckpoint(tempPath, result.LastAppliedIndex, result.LastAppliedTime);
            Directory.Delete(targetDir, recursive: true);
            Directory.Move(tempPath, targetDir);
        }

        return new KahunaRestoreResponse
        {
            TargetDir = targetDir,
            PartitionsRestored = result.PartitionsRestored,
            EntriesApplied = result.EntriesApplied,
            LastAppliedPhysicalMs = result.LastAppliedTime.L,
            Chain = chain.Select(ToDto).ToList()
        };
    }

    // ── helpers ───────────────────────────────────────────────────────────────────────────────

    private IPersistenceBackend OpenBackendAt(string path) => _storageType switch
    {
        "rocksdb" => new RocksDbPersistenceBackend(path, _storageRevision),
        "sqlite"  => new SqlitePersistenceBackend(path, _storageRevision, _logger),
        _         => MemoryPersistenceBackend.OpenCheckpoint(path)
    };

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (string file in Directory.GetFiles(source))
            File.Copy(file, Path.Combine(destination, Path.GetFileName(file)), overwrite: true);
        foreach (string subDir in Directory.GetDirectories(source))
            CopyDirectory(subDir, Path.Combine(destination, Path.GetFileName(subDir)));
    }

    private static KahunaBackupInfo ToDto(BackupManifest m) => new()
    {
        BackupId = m.BackupId,
        Type = m.Type.ToString(),
        CreatedAtUtc = m.CreatedAtUtc,
        ParentBackupId = m.ParentBackupId,
        PartitionCount = m.PartitionRanges.Count,
        ClusterSnapshotNode = m.ClusterSnapshotNode,
        ClusterSnapshotPhysical = m.ClusterSnapshotPhysical,
        ClusterSnapshotCounter = m.ClusterSnapshotCounter
    };
}

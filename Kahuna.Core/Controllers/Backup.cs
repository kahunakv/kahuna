
using Kommander.Time;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Shared.Communication.Rest;

namespace Kahuna;

/// <summary>
/// Backup and point-in-time-recovery surface. The operations delegate to the node's
/// <see cref="BackupFacade"/>, which owns the backup service and the mapping of internal failures
/// onto the public, sanitized outcome type.
/// </summary>
public sealed partial class KahunaManager
{
    /// <inheritdoc/>
    public async Task BootstrapFromPitrBackupAsync(
        string backupDir,
        Guid leafBackupId,
        HLCTimestamp targetTime,
        Kommander.WAL.IWAL walAdapter,
        TimeSpan pitrWindow,
        TimeSpan baseSnapshotInterval)
    {
        BackupCatalog catalog = new(new LocalDirectoryStorageTarget(backupDir));
        LocalDirectoryArtifactStore artifacts = new(backupDir);
        IReadOnlyList<BackupManifest> chain = await catalog.ResolveAndValidateAsync(leafBackupId);

        // Coverage validation (Zero → natural end, fail closed on out-of-range / unknown lower bound)
        // is centralized in BootstrapHelper.BootstrapNode so it happens before any backend/WAL mutation.
        await FlushPersistenceAsync();
        await BootstrapHelper.BootstrapNodeAsync(chain, artifacts, targetTime, persistenceBackend, walAdapter, pitrWindow, DateTime.UtcNow, baseSnapshotInterval);
    }

    // ── Backup / PITR ──────────────────────────────────────────────────────────────────────

    public bool IsBackupConfigured => backups.IsConfigured;

    public bool IsRemoteRestoreAllowed => remoteRestoreAllowed;

    public Task<KahunaBackupInfo> TakeFullBackupAsync(CancellationToken ct = default) =>
        backups.TakeFullAsync(ct);

    public Task<KahunaBackupInfo> TakeIncrementalBackupAsync(Guid parentBackupId, CancellationToken ct = default) =>
        backups.TakeIncrementalAsync(parentBackupId, ct);

    public Task<KahunaBackupInfo> TakeCoordinatedBackupAsync(CancellationToken ct = default) =>
        backups.TakeCoordinatedAsync(ct);

    public Task<IReadOnlyList<KahunaBackupInfo>> ListBackupsAsync(CancellationToken ct = default) =>
        backups.ListAsync(ct);

    public Task<IReadOnlyList<KahunaBackupInfo>> GetBackupChainAsync(Guid leafBackupId, CancellationToken ct = default) =>
        backups.GetChainAsync(leafBackupId, ct);

    public Task<KahunaRestoreResponse> RestoreToAsync(
        Guid leafBackupId,
        string targetDir,
        long targetTimeMs,
        CancellationToken ct = default) =>
        backups.RestoreToAsync(leafBackupId, targetDir, targetTimeMs, ct);

    public Task<KahunaBackupGcResult> RunBackupGarbageCollectionAsync(bool dryRun, CancellationToken ct = default) =>
        backups.RunGarbageCollectionAsync(dryRun, ct);
}

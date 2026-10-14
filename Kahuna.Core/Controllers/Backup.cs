
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
    public Task BootstrapFromPitrBackupAsync(
        string backupDir,
        Guid leafBackupId,
        HLCTimestamp targetTime,
        Kommander.WAL.IWAL walAdapter,
        TimeSpan pitrWindow,
        TimeSpan baseSnapshotInterval) =>
        // Coverage, window, authentication and cluster checks all run before any backend/WAL mutation;
        // see BackupFacade.BootstrapAsync and BootstrapHelper.BootstrapNodeAsync.
        BackupFacade.BootstrapAsync(
            configuration, backupDir, leafBackupId, targetTime, persistenceBackend, walAdapter,
            pitrWindow, baseSnapshotInterval, FlushPersistenceAsync, logger);

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

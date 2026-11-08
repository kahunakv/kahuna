using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence.Providers;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// The <c>"local"</c> backup target: manifests and artifacts in the backup directory on this host. Kahuna
/// registers it in every <see cref="StorageProviders"/>, and it is the default target.
/// </summary>
public sealed class LocalBackupStorageProvider : IBackupStorageProvider
{
    /// <summary>The target name, the default value of <c>--pitr-backup-target</c>.</summary>
    public const string Name = "local";

    /// <summary>
    /// The directory under the storage path where a checkpoint of the live store is staged when the backup
    /// directory is on another filesystem.
    /// </summary>
    internal const string LiveCheckpointStagingDirectoryName = "backup-staging";

    public string TargetName => Name;

    /// <summary>False: the backend writes a checkpoint straight into the backup directory.</summary>
    public bool RequiresLocalScratch => false;

    public IBackupStorage Open(BackupStorageContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        return new LocalBackupStorage(context.BackupDir, context.Configuration, context.Logger);
    }

    /// <summary>
    /// Staging on the store's filesystem, used when the backup directory is on another one. Only an engine
    /// whose checkpoint links the live files benefits (RocksDB hard-links them where the filesystem allows
    /// it); one that writes a full copy wherever the checkpoint goes would only move that write onto the disk
    /// the commits use. The engine says which (<see cref="IPersistenceBackendProvider.StagesLiveCheckpointsInStore"/>).
    /// <para>The staging root is under the storage path, which belongs to this one store. A directory that
    /// several nodes can share, such as the scratch directory, is not used: each node's leftover sweep would
    /// see the others' staging trees as debris.</para>
    /// </summary>
    private static LiveCheckpointStaging? CreateLiveCheckpointStaging(KahunaConfiguration configuration, ILogger logger)
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

    private sealed class LocalBackupStorage(string backupDir, KahunaConfiguration configuration, ILogger logger) : IBackupStorage
    {
        private IBackupStorageTarget? manifests;

        private IBackupArtifactStore? artifacts;

        public IBackupStorageTarget Manifests =>
            manifests ?? throw new InvalidOperationException("The local backup storage is not started.");

        public IBackupArtifactStore Artifacts =>
            artifacts ?? throw new InvalidOperationException("The local backup storage is not started.");

        /// <summary>
        /// Builds the two stores. Each refuses an unsafe root (a symlink, or group/world-writable) before it
        /// creates the directory owner-only, so a start never repairs an exposed root without a refusal.
        /// </summary>
        public Task StartAsync(CancellationToken ct)
        {
            ct.ThrowIfCancellationRequested();

            if (manifests is not null)
                return Task.CompletedTask;

            LocalDirectoryStorageTarget manifestStore = new(backupDir);
            LocalDirectoryArtifactStore artifactStore = new(backupDir, CreateLiveCheckpointStaging(configuration, logger));

            artifacts = artifactStore;
            manifests = manifestStore;
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}

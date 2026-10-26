#if !KAHUNA_THREAD_FREE

using Kahuna.Server.Persistence.Backend;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Persistence.Providers;

/// <summary>
/// The RocksDB backend, <c>"rocksdb"</c>: one database in <c>{storage path}/{revision}</c>, whose checkpoint
/// links its files, so a live checkpoint is staged inside the store's directory.
/// </summary>
internal sealed class RocksDbPersistenceBackendProvider : IPersistenceBackendProvider
{
    public string StorageType => "rocksdb";

    public bool KeepsDataOnDisk => true;

    public bool StagesLiveCheckpointsInStore => true;

    public IPersistenceBackend Open(PersistenceBackendContext context) =>
        new RocksDbPersistenceBackend(
            context.StoragePath,
            context.StorageRevision,
            context.SharedResources,
            context.Configuration.RocksDbDirectReads,
            context.Configuration.RocksDbStatistics,
            context.Logger,
            context.Configuration.BackupCaptureThrottleBytesPerSec,
            context.Configuration.StorageCompactionRateBytesPerSec,
            context.Configuration.StorageMaxBackgroundCompactions);

    public IPersistenceBackend OpenAt(string directory, string revision, ILogger logger) => new RocksDbPersistenceBackend(directory, revision);

    // An empty revision opens the root itself.
    public string DataDirectory(string storageRoot, string revision) =>
        string.IsNullOrEmpty(revision) ? storageRoot : Path.Combine(storageRoot, revision);

    public bool HasData(string storageRoot, string revision)
    {
        string dbPath = Path.Combine(storageRoot, revision);
        return File.Exists(dbPath) || (Directory.Exists(dbPath) && Directory.EnumerateFileSystemEntries(dbPath).Any());
    }

    public void MergeCheckpointInto(string targetDirectory, string targetRevision, string sourceDirectory, CancellationToken ct) =>
        RocksDbPersistenceBackend.MergeCheckpointInto(targetDirectory, sourceDirectory, ct);
}
#endif

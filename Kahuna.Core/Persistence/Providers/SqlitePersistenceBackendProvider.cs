#if !KAHUNA_THREAD_FREE

using Kahuna.Server.Persistence.Backend;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Persistence.Providers;

/// <summary>
/// The SQLite backend, <c>"sqlite"</c>: shard files <c>kahuna{shard}_{revision}.db</c> directly in the
/// storage path, so the files of a checkpoint from another node are renamed to the local revision.
/// </summary>
internal sealed class SqlitePersistenceBackendProvider : IPersistenceBackendProvider
{
    public string StorageType => "sqlite";

    public bool KeepsDataOnDisk => true;

    public IPersistenceBackend Open(PersistenceBackendContext context) =>
        new SqlitePersistenceBackend(context.StoragePath, context.StorageRevision, context.Logger);

    public IPersistenceBackend OpenAt(string directory, string revision, ILogger logger) => new SqlitePersistenceBackend(directory, revision, logger);

    public string DataDirectory(string storageRoot, string revision) => storageRoot;

    // The pattern also matches the -wal and -shm files.
    public bool HasData(string storageRoot, string revision) =>
        Directory.EnumerateFiles(storageRoot, $"kahuna*_{revision}.db*").Any();

    public void MergeCheckpointInto(string targetDirectory, string targetRevision, string sourceDirectory, CancellationToken ct) =>
        SqlitePersistenceBackend.MergeCheckpointInto(targetDirectory, targetRevision, sourceDirectory, ct);

    public void AdoptRevision(string checkpointDirectory, string revision)
    {
        foreach (string file in Directory.GetFiles(checkpointDirectory, "kahuna*_*.db"))
        {
            string name = Path.GetFileName(file);
            string renamed = name[..(name.IndexOf('_') + 1)] + revision + ".db";
            if (!string.Equals(name, renamed, StringComparison.Ordinal))
                File.Move(file, Path.Combine(checkpointDirectory, renamed));
        }
    }
}
#endif

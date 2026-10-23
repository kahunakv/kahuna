
using Kahuna.Server.Persistence.Backend;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Persistence.Providers;

/// <summary>The in-memory backend, <c>"memory"</c>: nothing on disk; a checkpoint is a JSON dump that a restore reloads.</summary>
internal sealed class MemoryPersistenceBackendProvider : IPersistenceBackendProvider
{
    /// <summary>The type name.</summary>
    public const string TypeName = "memory";

    public string StorageType => TypeName;

    public bool KeepsDataOnDisk => false;

    public IPersistenceBackend Open(PersistenceBackendContext context) => new MemoryPersistenceBackend();

    public IPersistenceBackend OpenAt(string directory, string revision, ILogger logger) => MemoryPersistenceBackend.OpenCheckpoint(directory);

    public string DataDirectory(string storageRoot, string revision) => storageRoot;

    public bool HasData(string storageRoot, string revision) => false;

    public void MergeCheckpointInto(string targetDirectory, string targetRevision, string sourceDirectory, CancellationToken ct) =>
        MemoryPersistenceBackend.MergeCheckpointInto(targetDirectory, sourceDirectory, ct);
}

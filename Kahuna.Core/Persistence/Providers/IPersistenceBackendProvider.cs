
using Kahuna.Server.Persistence.Backend;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Persistence.Providers;

/// <summary>
/// A storage engine for a node's key-value and lock data, chosen by its <see cref="StorageType"/> name
/// (the <c>--storage</c> option). It opens the node's backend, and tells the backup, restore and
/// bootstrap paths what they must know about the engine's files. Registered in
/// <see cref="StorageProviders"/>; a host registers its own before the node starts.
/// </summary>
public interface IPersistenceBackendProvider
{
    /// <summary>The storage type name this provider answers to, such as <c>"rocksdb"</c>. Compared ordinally.</summary>
    string StorageType { get; }

    /// <summary>
    /// True when the backend keeps its data in files under the storage path: the node creates the path,
    /// puts its snapshot staging under it, and can install a PITR base image into it. False for a backend
    /// that holds everything in memory, whose checkpoints are written out by the backup itself.
    /// </summary>
    bool KeepsDataOnDisk { get; }

    /// <summary>Opens the node's backend at <see cref="PersistenceBackendContext.StoragePath"/>.</summary>
    IPersistenceBackend Open(PersistenceBackendContext context);

    /// <summary>
    /// Opens a backend over the files at <paramref name="directory"/>: a restore's staging copy of a
    /// checkpoint, laid out as <see cref="DataDirectory"/> says, under the storage <paramref name="revision"/>.
    /// </summary>
    IPersistenceBackend OpenAt(string directory, string revision, ILogger logger);

    /// <summary>
    /// The directory that holds the store's files when the storage path is <paramref name="storageRoot"/>:
    /// the root itself for an engine that names its files after the revision, or a directory under it.
    /// A checkpoint is copied there to be opened, and a base image is published there.
    /// </summary>
    string DataDirectory(string storageRoot, string revision);

    /// <summary>True when the files this provider would open under <paramref name="storageRoot"/> exist already.</summary>
    bool HasData(string storageRoot, string revision);

    /// <summary>
    /// Adds every row of the checkpoint at <paramref name="sourceDirectory"/> to the checkpoint at
    /// <paramref name="targetDirectory"/>. Both were written by this engine; the shares of one
    /// cluster-wide backup hold disjoint keys. The checkpoint manifests are the caller's.
    /// </summary>
    void MergeCheckpointInto(string targetDirectory, string targetRevision, string sourceDirectory, CancellationToken ct);

    /// <summary>
    /// Renames the files of a checkpoint written by another node so they carry <paramref name="revision"/>,
    /// for an engine whose file names hold the revision. The default does nothing.
    /// </summary>
    void AdoptRevision(string checkpointDirectory, string revision)
    {
    }

    /// <summary>
    /// True when a live checkpoint should be staged inside the store's own directory: an engine that
    /// links its files into a checkpoint needs the checkpoint on the same file system. The default is
    /// false, for an engine that writes a full copy wherever the checkpoint goes.
    /// </summary>
    bool StagesLiveCheckpointsInStore => false;
}

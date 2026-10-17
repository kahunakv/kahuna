using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Merges key-filtered as-of checkpoints into one image. Each source is one node's share of a cluster-wide
/// backup: the partitions that node captured, at the same cut as every other share, with every row of
/// another partition's keys removed (see the key-filtered <c>IPersistenceBackend.CreateCheckpointAsOf</c>).
/// The shares therefore hold disjoint key sets, and their union is the cluster's state at the cut.
/// <para>
/// The merge is backend-specific because a checkpoint is a backend's own on-disk format: every share of one
/// backup comes from the same storage engine, which the caller checks before merging.
/// </para>
/// </summary>
internal static class CheckpointComposer
{
    /// <summary>
    /// Adds every row of the checkpoint at <paramref name="sourceDir"/> to the checkpoint at
    /// <paramref name="targetDir"/>, and raises the target's sidecar index to the source's when it is higher
    /// (the sidecar index is the largest committed index the image holds; both sides share the cut).
    /// </summary>
    /// <param name="storageType">The storage engine both checkpoints were written by.</param>
    /// <param name="targetRevision">The storage revision the target's files are named with (SQLite).</param>
    internal static void MergeInto(
        string storageType, string targetRevision, string targetDir, string sourceDir, CancellationToken ct)
    {
        switch (storageType)
        {
#if !KAHUNA_THREAD_FREE
            // The thread-free (browser) build has no RocksDB or SQLite backend.
            case "rocksdb":
                RocksDbPersistenceBackend.MergeCheckpointInto(targetDir, sourceDir, ct);
                break;

            case "sqlite":
                SqlitePersistenceBackend.MergeCheckpointInto(targetDir, targetRevision, sourceDir, ct);
                break;
#endif
            case "memory" or "":
                MemoryPersistenceBackend.MergeCheckpointInto(targetDir, sourceDir, ct);
                break;

            default:
                throw new BackupDriverException(
                    $"Checkpoints of the '{storageType}' storage engine cannot be merged into one cluster-wide image.");
        }

        CheckpointManifest target = CheckpointManifest.ReadFrom(targetDir);
        CheckpointManifest source = CheckpointManifest.ReadFrom(sourceDir);
        if (source.AppliedIndex > target.AppliedIndex)
            CheckpointManifest.From(source.AppliedIndex, target.AppliedTime).WriteTo(targetDir);
    }

    /// <summary>
    /// Renames a checkpoint written by another node so its files carry <paramref name="revision"/>. Only SQLite
    /// names its files after the storage revision; the other engines keep it out of the checkpoint entirely.
    /// </summary>
    internal static void AdoptRevision(string storageType, string revision, string checkpointDir)
    {
        if (storageType != "sqlite")
            return;

        foreach (string file in Directory.GetFiles(checkpointDir, "kahuna*_*.db"))
        {
            string name = Path.GetFileName(file);
            string renamed = name[..(name.IndexOf('_') + 1)] + revision + ".db";
            if (!string.Equals(name, renamed, StringComparison.Ordinal))
                File.Move(file, Path.Combine(checkpointDir, renamed));
        }
    }
}

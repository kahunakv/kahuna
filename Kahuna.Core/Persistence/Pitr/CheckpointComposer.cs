using Kahuna.Server.Persistence.Providers;

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
    /// <param name="storageType">The storage engine both checkpoints were written by, one of Kahuna's own.</param>
    /// <param name="targetRevision">The storage revision the target's files are named with (SQLite).</param>
    internal static void MergeInto(
        string storageType, string targetRevision, string targetDir, string sourceDir, CancellationToken ct) =>
        MergeInto(StorageProviders.CreateDefault().FindBackend(storageType), storageType, targetRevision, targetDir, sourceDir, ct);

    /// <summary>
    /// The form of <see cref="MergeInto(string, string, string, string, CancellationToken)"/> for the engine
    /// <paramref name="provider"/>, or null when <paramref name="storageType"/> is not registered.
    /// </summary>
    internal static void MergeInto(
        IPersistenceBackendProvider? provider, string storageType, string targetRevision, string targetDir, string sourceDir, CancellationToken ct)
    {
        if (provider is null)
            throw new BackupDriverException(
                $"Checkpoints of the '{storageType}' storage engine cannot be merged into one cluster-wide image: no such engine is registered.");

        provider.MergeCheckpointInto(targetDir, targetRevision, sourceDir, ct);

        CheckpointManifest target = CheckpointManifest.ReadFrom(targetDir);
        CheckpointManifest source = CheckpointManifest.ReadFrom(sourceDir);
        if (source.AppliedIndex > target.AppliedIndex)
            CheckpointManifest.From(source.AppliedIndex, target.AppliedTime).WriteTo(targetDir);
    }

    /// <summary>
    /// Renames a checkpoint written by another node so its files carry <paramref name="revision"/>, for an engine
    /// that names its files after the storage revision (SQLite); the others keep it out of the checkpoint.
    /// </summary>
    internal static void AdoptRevision(IPersistenceBackendProvider? provider, string revision, string checkpointDir) =>
        provider?.AdoptRevision(checkpointDir, revision);
}

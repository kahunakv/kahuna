
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Catalog operations over an <see cref="IBackupStorageTarget"/>: resolve chains and
/// validate their structural integrity.
/// </summary>
internal sealed class BackupCatalog
{
    private readonly IBackupStorageTarget _target;

    public BackupCatalog(IBackupStorageTarget target)
    {
        _target = target;
    }

    public void Put(BackupManifest manifest) => _target.Put(manifest);

    public BackupManifest? Get(Guid backupId) => _target.Get(backupId);

    public IReadOnlyList<BackupManifest> List() => _target.List();

    /// <summary>
    /// Resolves the backup chain ending at <paramref name="leafBackupId"/> by walking
    /// <see cref="BackupManifest.ParentBackupId"/> links back to the root Full backup.
    /// Returns the chain in chronological order: Full first, leaf last.
    /// </summary>
    /// <exception cref="BackupChainException">
    /// Thrown when a manifest in the chain is missing from the catalog.
    /// </exception>
    public IReadOnlyList<BackupManifest> ResolveChain(Guid leafBackupId)
    {
        List<BackupManifest> reversed = [];
        HashSet<Guid> seen = [];
        Guid? current = leafBackupId;

        while (current.HasValue)
        {
            if (!seen.Add(current.Value))
                throw new BackupChainException(
                    $"Cycle detected in backup chain at {current.Value:N} while resolving from {leafBackupId:N}.");

            BackupManifest? manifest = _target.Get(current.Value);
            if (manifest is null)
                throw new BackupChainException(
                    $"Manifest {current.Value:N} not found in catalog while resolving chain from {leafBackupId:N}.");

            reversed.Add(manifest);
            current = manifest.ParentBackupId;
        }

        reversed.Reverse();
        return reversed;
    }

    /// <summary>
    /// Resolves the chain and immediately validates it.
    /// Equivalent to <c>Validate(ResolveChain(leafBackupId))</c>.
    /// </summary>
    public IReadOnlyList<BackupManifest> ResolveAndValidate(Guid leafBackupId)
    {
        IReadOnlyList<BackupManifest> chain = ResolveChain(leafBackupId);
        Validate(chain);
        return chain;
    }

    /// <summary>
    /// Validates that <paramref name="chain"/> is a well-formed backup chain:
    /// <list type="bullet">
    ///   <item>Must be non-empty and start with a Full backup.</item>
    ///   <item>Every subsequent entry must be Incremental.</item>
    ///   <item>Parent-ID links must be unbroken.</item>
    ///   <item>For each partition, index ranges must be contiguous:
    ///     <c>chain[i+1].FromIndex == chain[i].ToIndex + 1</c>.</item>
    /// </list>
    /// </summary>
    /// <exception cref="BackupChainException">Thrown on any violation with a descriptive message.</exception>
    public static void Validate(IReadOnlyList<BackupManifest> chain)
    {
        if (chain.Count == 0)
            throw new BackupChainException("Backup chain is empty.");

        if (chain[0].Type != BackupType.Full)
            throw new BackupChainException(
                $"Chain must start with a Full backup; found {chain[0].Type} ({chain[0].BackupId:N}).");

        for (int i = 1; i < chain.Count; i++)
        {
            BackupManifest prev = chain[i - 1];
            BackupManifest curr = chain[i];

            if (curr.Type != BackupType.Incremental)
                throw new BackupChainException(
                    $"Entry {i} ({curr.BackupId:N}) in the chain must be Incremental; found {curr.Type}.");

            if (curr.ParentBackupId != prev.BackupId)
                throw new BackupChainException(
                    $"Broken parent link at position {i}: expected parent {prev.BackupId:N}, " +
                    $"got {curr.ParentBackupId?.ToString("N") ?? "null"}.");

            // Build a lookup for the previous entry's partition ranges.
            Dictionary<int, PartitionBackupRange> prevRanges =
                prev.PartitionRanges.ToDictionary(r => r.PartitionId);

            foreach (PartitionBackupRange currRange in curr.PartitionRanges)
            {
                if (!prevRanges.TryGetValue(currRange.PartitionId, out PartitionBackupRange? prevRange))
                    continue; // partition first appears in this incremental — acceptable

                long expectedFrom = prevRange.ToIndex + 1;
                if (currRange.FromIndex != expectedFrom)
                    throw new BackupChainException(
                        $"Index gap on partition {currRange.PartitionId} between backup {prev.BackupId:N} " +
                        $"(ToIndex={prevRange.ToIndex}) and {curr.BackupId:N} " +
                        $"(FromIndex={currRange.FromIndex}); expected {expectedFrom}.");
            }
        }
    }
}

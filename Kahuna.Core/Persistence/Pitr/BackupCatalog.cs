
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

    /// <summary>
    /// Removes a backup: the manifest first (tombstone), then its artifact directory
    /// <c>{backupDir}/{id:N}/</c>. Manifest-first ordering means a crash between the two steps leaves at
    /// worst an orphan artifact directory with no manifest — which the orphan sweep reclaims — never a
    /// manifest resolving to absent artifacts. The artifact directory is removed without following a
    /// symlink: if the directory itself is a reparse point only the link is unlinked, so a swapped-in
    /// symlink can never redirect the delete outside the backup directory. Idempotent.
    /// </summary>
    public void Delete(Guid backupId, string backupDir)
    {
        _target.Delete(backupId);
        RemoveArtifactDirectory(Path.Combine(backupDir, backupId.ToString("N")));
    }

    /// <summary>
    /// Removes a leftover artifact directory (or staging/quarantine remnant) at <paramref name="path"/>
    /// without following a top-level symlink. Idempotent — a no-op when the path does not exist.
    /// </summary>
    internal static void RemoveArtifactDirectory(string path)
    {
        if (!Directory.Exists(path))
            return;

        // A reparse point (symlink/junction) as the directory itself must be unlinked, not recursed
        // into — a recursive delete through it would reclaim the link target's contents outside the
        // backup directory. Recursive delete of a real directory does not descend into reparse-point
        // subdirectories on this runtime, so inner links are removed as links, never followed.
        if (File.GetAttributes(path).HasFlag(FileAttributes.ReparsePoint))
            Directory.Delete(path, recursive: false);
        else
            Directory.Delete(path, recursive: true);
    }

    public BackupManifest? Get(Guid backupId) => _target.Get(backupId);

    public IReadOnlyList<BackupManifest> List(CancellationToken ct = default) => _target.List(ct);

    public IReadOnlyList<(Guid backupId, string reason)> ListCorrupt(CancellationToken ct = default) =>
        _target.ListCorrupt(ct);

    public IReadOnlyList<Guid> ListManifestIds(CancellationToken ct = default) =>
        _target.ListManifestIds(ct);

    /// <summary>
    /// Resolves the backup chain ending at <paramref name="leafBackupId"/> by walking
    /// <see cref="BackupManifest.ParentBackupId"/> links back to the root Full backup.
    /// Returns the chain in chronological order: Full first, leaf last.
    /// </summary>
    /// <exception cref="BackupChainException">
    /// Thrown when a manifest in the chain is missing from the catalog.
    /// </exception>
    public IReadOnlyList<BackupManifest> ResolveChain(Guid leafBackupId, CancellationToken ct = default)
    {
        List<BackupManifest> reversed = [];
        HashSet<Guid> seen = [];
        Guid? current = leafBackupId;

        while (current.HasValue)
        {
            ct.ThrowIfCancellationRequested();

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
    public IReadOnlyList<BackupManifest> ResolveAndValidate(Guid leafBackupId, CancellationToken ct = default)
    {
        IReadOnlyList<BackupManifest> chain = ResolveChain(leafBackupId, ct);
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

        // Every range's HLC bounds must be ordered (FromHlc ≤ ToHlc). A range whose start sorts
        // after its end is a corrupt manifest, independent of the index-continuity checks below.
        foreach (BackupManifest manifest in chain)
        {
            foreach (PartitionBackupRange range in manifest.PartitionRanges)
            {
                if (range.FromHlc.CompareTo(range.ToHlc) > 0)
                    throw new BackupChainException(
                        $"Backup {manifest.BackupId:N}, partition {range.PartitionId}: FromHlc {range.FromHlc} " +
                        $"sorts after ToHlc {range.ToHlc}.");
            }
        }

        // Running per-partition high-water mark (greatest ToIndex seen so far across the chain). A
        // sparse/empty intermediate manifest that omits an unchanged partition must NOT reset that
        // partition's expected continuation point — otherwise a later incremental could restart at 1
        // (duplicating the WAL) or skip a prefix (hidden gap after compaction) undetected.
        Dictionary<int, PartitionBackupRange> highWater = [];
        foreach (PartitionBackupRange range in chain[0].PartitionRanges)
            highWater[range.PartitionId] = range;

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

            // Validate each range against the latest earlier range for that partition anywhere in the
            // chain, not just the immediate predecessor.
            foreach (PartitionBackupRange currRange in curr.PartitionRanges)
            {
                if (!highWater.TryGetValue(currRange.PartitionId, out PartitionBackupRange? hw))
                    continue; // partition appears for the first time in the whole chain — acceptable

                long expectedFrom = hw.ToIndex + 1;
                if (currRange.FromIndex != expectedFrom)
                    throw new BackupChainException(
                        $"Index gap on partition {currRange.PartitionId}: {curr.BackupId:N} starts at " +
                        $"FromIndex={currRange.FromIndex} but the latest earlier coverage ends at " +
                        $"ToIndex={hw.ToIndex}; expected {expectedFrom}.");
            }

            // Advance the running high-water mark with this manifest's ranges.
            foreach (PartitionBackupRange currRange in curr.PartitionRanges)
            {
                if (!highWater.TryGetValue(currRange.PartitionId, out PartitionBackupRange? cur) ||
                    currRange.ToIndex > cur.ToIndex)
                    highWater[currRange.PartitionId] = currRange;
            }
        }
    }
}

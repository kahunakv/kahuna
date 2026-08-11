
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

    public Task PutAsync(BackupManifest manifest, CancellationToken ct = default) =>
        _target.PutAsync(manifest, ct);

    /// <summary>
    /// Removes a backup: the manifest first (tombstone), then its artifacts. Manifest-first ordering
    /// means a crash between the two steps leaves at worst an orphan artifact set with no manifest —
    /// which the orphan sweep reclaims — never a manifest resolving to absent artifacts.
    /// <para>
    /// Idempotent in both halves, which matters because artifact deletion is not atomic on every store:
    /// a store that deletes object-by-object can fail part-way, and re-running this must finish the job
    /// rather than throw or skip. The manifest is already gone by then, so the backup stays tombstoned
    /// throughout.
    /// </para>
    /// </summary>
    public async Task DeleteAsync(Guid backupId, IBackupArtifactStore artifacts, CancellationToken ct = default)
    {
        await _target.DeleteAsync(backupId, ct).ConfigureAwait(false);
        await artifacts.DeleteAllAsync(backupId, ct).ConfigureAwait(false);
    }

    public Task<BackupManifest?> GetAsync(Guid backupId, CancellationToken ct = default) =>
        _target.GetAsync(backupId, ct);

    public Task<IReadOnlyList<BackupManifest>> ListAsync(CancellationToken ct = default) => _target.ListAsync(ct);

    public Task<IReadOnlyList<(Guid backupId, string reason)>> ListCorruptAsync(CancellationToken ct = default) =>
        _target.ListCorruptAsync(ct);

    public Task<IReadOnlyList<Guid>> ListManifestIdsAsync(CancellationToken ct = default) =>
        _target.ListManifestIdsAsync(ct);

    /// <summary>
    /// Resolves the backup chain ending at <paramref name="leafBackupId"/> by walking
    /// <see cref="BackupManifest.ParentBackupId"/> links back to the root Full backup.
    /// Returns the chain in chronological order: Full first, leaf last.
    /// </summary>
    /// <exception cref="BackupChainException">
    /// Thrown when a manifest in the chain is missing from the catalog.
    /// </exception>
    public async Task<IReadOnlyList<BackupManifest>> ResolveChainAsync(Guid leafBackupId, CancellationToken ct = default)
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

            BackupManifest? manifest = await _target.GetAsync(current.Value, ct).ConfigureAwait(false);
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
    /// Equivalent to <c>Validate(await ResolveChainAsync(leafBackupId))</c>.
    /// </summary>
    public async Task<IReadOnlyList<BackupManifest>> ResolveAndValidateAsync(Guid leafBackupId, CancellationToken ct = default)
    {
        IReadOnlyList<BackupManifest> chain = await ResolveChainAsync(leafBackupId, ct).ConfigureAwait(false);
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

        // Identity consistency: a chain must not span different clusters, storage format revisions, or
        // topology generations. These fields are optional — null on legacy manifests and on backups taken
        // before the cluster-ownership/topology paths assign them — so only distinct NON-null values
        // conflict. A legacy or partially-stamped chain is therefore never rejected on identity grounds,
        // but two concretely different values are, so a foreign cluster's artifact, a mismatched storage
        // revision, or a stale topology generation can never be chained onto this base.
        EnsureConsistentIdentity(chain, m => m.ClusterId, "cluster id");
        EnsureConsistentIdentity(chain, m => m.StorageRevision, "storage revision");
        EnsureConsistentIdentity(chain, m => m.TopologyGeneration, "topology generation");

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

    /// <summary>
    /// Throws when <paramref name="chain"/> carries more than one distinct non-null value for the
    /// identity field returned by <paramref name="selector"/>. Null (unknown) values are skipped so a
    /// legacy or partially-stamped chain passes; two concretely different values fail closed.
    /// </summary>
    private static void EnsureConsistentIdentity(
        IReadOnlyList<BackupManifest> chain, Func<BackupManifest, object?> selector, string label)
    {
        object? reference = null;
        Guid referenceBackup = default;

        foreach (BackupManifest manifest in chain)
        {
            object? value = selector(manifest);
            if (value is null)
                continue;

            if (reference is null)
            {
                reference = value;
                referenceBackup = manifest.BackupId;
                continue;
            }

            if (!reference.Equals(value))
                throw new BackupChainException(
                    $"Backup chain spans multiple {label} values: {manifest.BackupId:N} has '{value}' but an " +
                    $"earlier manifest ({referenceBackup:N}) has '{reference}'. A chain must not cross " +
                    $"{label} boundaries; a new full backup is required across such a change.");
        }
    }
}

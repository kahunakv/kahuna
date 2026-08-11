
namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Bounds on how much backup history to keep. Every dimension is optional; a null dimension is not
/// enforced. When <see cref="IsEnabled"/> is false (all dimensions null) retention never deletes
/// anything — the safe default, so backups are only reclaimed when an operator explicitly opts in.
/// Retention operates at whole-chain granularity: a chain is kept or deleted as a unit, so a retained
/// incremental always keeps its Full root and every intermediate parent.
/// </summary>
internal readonly record struct BackupRetentionPolicy(int? MaxChains, TimeSpan? MaxAge, long? MaxTotalBytes)
{
    internal static readonly BackupRetentionPolicy Disabled = new(null, null, null);

    internal bool IsEnabled => MaxChains is not null || MaxAge is not null || MaxTotalBytes is not null;
}

/// <summary>
/// One backup slated for deletion, with why. Returned by the planners so retention and the orphan
/// sweep can be inspected (dry-run inventory) before — or instead of — being applied.
/// </summary>
internal readonly record struct BackupGcCandidate(Guid BackupId, BackupType Type, DateTime CreatedAtUtc, long Bytes, string Reason);

/// <summary>
/// A leftover directory or file under the backup directory that no valid manifest accounts for, with
/// why it is reclaimable. Returned by <see cref="BackupRetention.PlanOrphanSweep"/>.
/// </summary>
/// <summary>
/// One thing the orphan sweep would reclaim. Exactly one of <paramref name="OrphanedBackupId"/> and
/// <paramref name="Leftover"/> is set: the first for a backup whose artifacts exist with no manifest to
/// own them, the second for store debris that belongs to no backup at all.
/// <para>
/// <paramref name="Name"/> is a display name only — never an absolute server path — because it is
/// surfaced to operators over the network.
/// </para>
/// </summary>
internal readonly record struct OrphanSweepCandidate(
    string Name,
    bool IsDirectory,
    string Reason,
    Guid? OrphanedBackupId = null,
    BackupArtifactLeftover? Leftover = null);

/// <summary>
/// The result of a garbage-collection pass (or its dry-run): the backups deleted by the retention
/// policy and the orphaned/leftover artifacts reclaimed.
/// </summary>
internal readonly record struct BackupGcInventory(
    IReadOnlyList<BackupGcCandidate> RetentionDeletions,
    IReadOnlyList<OrphanSweepCandidate> OrphanReclamations);

/// <summary>
/// Chain-aware retention and orphan-artifact reclamation for the backup directory. All planning is
/// pure — it computes <b>what</b> would be deleted and why without touching anything — so the same
/// plan drives both a dry-run inventory and the actual delete. Deletion always removes descendants
/// before ancestors, and the manifest before its artifacts (via <see cref="BackupCatalog.Delete"/>),
/// so a crash mid-run never leaves a manifest resolving to absent artifacts.
/// </summary>
internal static class BackupRetention
{
    /// <summary>
    /// Markers a backup writes into transient directory/file names while staging an artifact, a
    /// manifest, or a restore. A leftover carrying any of these is an interrupted operation's remnant.
    /// </summary>
    /// <summary>
    /// Sums a manifest's recorded artifact bytes. Uses <see cref="BackupManifest.Sizes"/> so no
    /// filesystem access is needed; a manifest with no recorded sizes contributes 0.
    /// </summary>
    internal static long ManifestBytes(BackupManifest m)
    {
        long total = 0;
        foreach (long size in m.Sizes.Values)
            total += size;
        return total;
    }

    /// <summary>
    /// Computes which backups a <paramref name="policy"/> would delete, given every manifest currently
    /// in the catalog. Retention keeps the most-recent chains that fit within the enabled bounds and
    /// deletes the rest, whole chains at a time. The result is ordered descendants-first (safe to apply
    /// in order) and doubles as the dry-run inventory.
    /// <para>
    /// A "chain" is identified by a leaf — a manifest that is no one's parent. Leaves are ranked
    /// newest-first by <see cref="BackupManifest.CreatedAtUtc"/>; a leaf's chain is KEPT unless it
    /// exceeds the max chain count, is older than the max age, or would push retained bytes past the
    /// max — evaluated newest-first so the freshest history survives. A kept leaf pins its whole
    /// transitive parent closure, so a Full shared by a retained branch is never deleted.
    /// </para>
    /// </summary>
    internal static IReadOnlyList<BackupGcCandidate> PlanRetention(
        IReadOnlyList<BackupManifest> manifests, BackupRetentionPolicy policy, DateTime nowUtc)
    {
        if (!policy.IsEnabled || manifests.Count == 0)
            return [];

        Dictionary<Guid, BackupManifest> byId = new(manifests.Count);
        foreach (BackupManifest m in manifests)
            byId[m.BackupId] = m;

        // A leaf is a manifest referenced as no other manifest's parent.
        HashSet<Guid> referencedAsParent = [];
        foreach (BackupManifest m in manifests)
            if (m.ParentBackupId is { } p)
                referencedAsParent.Add(p);

        List<BackupManifest> leaves = [];
        foreach (BackupManifest m in manifests)
            if (!referencedAsParent.Contains(m.BackupId))
                leaves.Add(m);

        // Newest first; BackupId breaks ties so the plan is deterministic regardless of listing order.
        leaves.Sort((a, b) =>
        {
            int byTime = b.CreatedAtUtc.CompareTo(a.CreatedAtUtc);
            return byTime != 0 ? byTime : a.BackupId.CompareTo(b.BackupId);
        });

        DateTime? ageCutoff = policy.MaxAge is { } age ? nowUtc - age : null;

        HashSet<Guid> keep = [];
        long keptBytes = 0;
        int keptChains = 0;
        Dictionary<Guid, string> deleteReasonByLeaf = [];

        foreach (BackupManifest leaf in leaves)
        {
            // The chain is this leaf's transitive parent closure (down to its Full root).
            List<Guid> closure = AncestorClosure(leaf, byId);

            string? reject = null;
            if (policy.MaxChains is { } maxChains && keptChains >= maxChains)
                reject = $"exceeds the retained-chain limit of {maxChains}";
            else if (ageCutoff is { } cutoff && leaf.CreatedAtUtc < cutoff)
                reject = $"the chain's newest backup ({leaf.CreatedAtUtc:O}) is older than the retention age";

            if (reject is null && policy.MaxTotalBytes is { } maxBytes)
            {
                // Only the closure members not already retained by a newer kept leaf add new bytes.
                long newBytes = 0;
                foreach (Guid id in closure)
                    if (!keep.Contains(id) && byId.TryGetValue(id, out BackupManifest? cm))
                        newBytes += ManifestBytes(cm);

                // Always keep at least the newest chain even if it alone exceeds the budget — deleting the
                // only/most-recent backup to satisfy a byte cap would leave no recoverable history at all.
                if (keptChains >= 1 && keptBytes + newBytes > maxBytes)
                    reject = $"retaining the chain would exceed the {maxBytes}-byte budget";
            }

            if (reject is null)
            {
                keptChains++;
                foreach (Guid id in closure)
                {
                    if (keep.Add(id) && byId.TryGetValue(id, out BackupManifest? cm))
                        keptBytes += ManifestBytes(cm);
                }
            }
            else
            {
                deleteReasonByLeaf[leaf.BackupId] = reject;
            }
        }

        // Everything not pinned by a kept leaf is deletable. Attribute each to the rejected leaf whose
        // chain it belongs to (a doomed manifest reachable only through rejected leaves).
        List<BackupGcCandidate> doomed = [];
        foreach (BackupManifest m in manifests)
        {
            if (keep.Contains(m.BackupId))
                continue;

            string reason = ReasonForDoomed(m, byId, deleteReasonByLeaf)
                            ?? "not retained by any kept chain";
            doomed.Add(new BackupGcCandidate(m.BackupId, m.Type, m.CreatedAtUtc, ManifestBytes(m), reason));
        }

        // Descendants before ancestors: a manifest with a greater depth-from-root is deleted first, so no
        // surviving child ever briefly points at a deleted parent.
        doomed.Sort((a, b) =>
        {
            int byDepth = DepthFromRoot(b.BackupId, byId).CompareTo(DepthFromRoot(a.BackupId, byId));
            return byDepth != 0 ? byDepth : a.BackupId.CompareTo(b.BackupId);
        });

        return doomed;
    }

    /// <summary>
    /// Lists everything in <paramref name="artifacts"/> that no valid manifest accounts for: artifacts
    /// belonging to a backup id with no manifest (orphaned by a failed or interrupted backup), and
    /// store-specific debris from an interrupted publish, delete, or restore.
    /// <para>
    /// Backups in <paramref name="reservedIds"/> — every backup that has a manifest entry, including
    /// corrupt/unreadable ones, plus any in-flight reservation — are left alone, so artifacts backed by a
    /// manifest that merely failed to parse are never destroyed as orphans. That set is derived from
    /// manifest <i>presence</i>, so it fails closed regardless of manifest health.
    /// </para>
    /// <para>
    /// Both listings propagate their failures rather than degrading to empty. An unreadable listing must
    /// never be interpreted as "nothing is here, so nothing is owned" — on a remote store that is a
    /// transient network error, and treating it as authoritative would reclaim live backups.
    /// </para>
    /// </summary>
    internal static async Task<IReadOnlyList<OrphanSweepCandidate>> PlanOrphanSweepAsync(
        IBackupArtifactStore artifacts,
        ISet<Guid> validManifestIds,
        ISet<Guid> reservedIds,
        CancellationToken ct = default)
    {
        List<OrphanSweepCandidate> candidates = [];

        foreach (Guid id in await artifacts.ListBackupIdsAsync(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            if (reservedIds.Contains(id) || validManifestIds.Contains(id))
                continue;

            candidates.Add(new OrphanSweepCandidate(
                id.ToString("N"),
                IsDirectory: true,
                "artifacts with no manifest (orphaned by a failed or interrupted backup)",
                OrphanedBackupId: id));
        }

        foreach (BackupArtifactLeftover leftover in await artifacts.ListLeftoversAsync(ct).ConfigureAwait(false))
        {
            ct.ThrowIfCancellationRequested();
            candidates.Add(new OrphanSweepCandidate(
                leftover.Name, leftover.IsDirectory, leftover.Description, Leftover: leftover));
        }

        return candidates;
    }

    /// <summary>
    /// Applies a retention plan: deletes each backup (manifest first, then artifacts) in the plan's
    /// order (descendants before ancestors). Idempotent — a candidate already gone is skipped.
    /// </summary>
    internal static async Task ApplyRetentionAsync(
        IReadOnlyList<BackupGcCandidate> plan,
        BackupCatalog catalog,
        IBackupArtifactStore artifacts,
        CancellationToken ct = default)
    {
        foreach (BackupGcCandidate candidate in plan)
        {
            ct.ThrowIfCancellationRequested();
            await catalog.DeleteAsync(candidate.BackupId, artifacts, ct).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Applies an orphan-sweep plan. Idempotent in both branches: an entry already gone is skipped, and a
    /// partially deleted backup is finished rather than skipped, so a store that cannot delete a whole
    /// backup atomically converges over successive passes.
    /// </summary>
    internal static async Task ApplyOrphanSweepAsync(
        IReadOnlyList<OrphanSweepCandidate> plan, IBackupArtifactStore artifacts, CancellationToken ct = default)
    {
        foreach (OrphanSweepCandidate candidate in plan)
        {
            ct.ThrowIfCancellationRequested();

            if (candidate.OrphanedBackupId is { } orphanId)
                await artifacts.DeleteAllAsync(orphanId, ct).ConfigureAwait(false);
            else if (candidate.Leftover is { } leftover)
                await artifacts.DeleteLeftoverAsync(leftover, ct).ConfigureAwait(false);
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────

    /// <summary>The leaf and every transitive parent present in the catalog (cycle-safe).</summary>
    private static List<Guid> AncestorClosure(BackupManifest leaf, Dictionary<Guid, BackupManifest> byId)
    {
        List<Guid> closure = [];
        HashSet<Guid> seen = [];
        Guid? current = leaf.BackupId;
        while (current is { } id && seen.Add(id) && byId.TryGetValue(id, out BackupManifest? m))
        {
            closure.Add(id);
            current = m.ParentBackupId;
        }
        return closure;
    }

    /// <summary>Distance from the root Full (root = 0), used to order deletions descendants-first.</summary>
    private static int DepthFromRoot(Guid backupId, Dictionary<Guid, BackupManifest> byId)
    {
        int depth = 0;
        HashSet<Guid> seen = [];
        Guid? current = backupId;
        while (current is { } id && seen.Add(id) && byId.TryGetValue(id, out BackupManifest? m) && m.ParentBackupId is { } parent)
        {
            depth++;
            current = parent;
        }
        return depth;
    }

    /// <summary>
    /// Finds the rejection reason for a doomed manifest by walking DOWN to any rejected leaf that
    /// descends from it — the reason its chain was not retained. Returns null when none is found.
    /// </summary>
    private static string? ReasonForDoomed(
        BackupManifest doomedManifest, Dictionary<Guid, BackupManifest> byId, Dictionary<Guid, string> deleteReasonByLeaf)
    {
        if (deleteReasonByLeaf.TryGetValue(doomedManifest.BackupId, out string? own))
            return own;

        // Build children once is overkill for the small catalogs here; do a direct scan for descendants.
        foreach ((Guid leafId, string reason) in deleteReasonByLeaf)
        {
            Guid? current = leafId;
            HashSet<Guid> seen = [];
            while (current is { } id && seen.Add(id) && byId.TryGetValue(id, out BackupManifest? m))
            {
                if (id == doomedManifest.BackupId)
                    return reason;
                current = m.ParentBackupId;
            }
        }
        return null;
    }
}

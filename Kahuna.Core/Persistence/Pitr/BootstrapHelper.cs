
using Kommander.Data;
using Kommander.Time;
using Kommander.WAL;

using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Prepares a node's data directory from a restored backup chain so that when the node
/// joins an existing cluster via <c>--join-existing</c>, it presents to the leader at the
/// restored WAL index rather than as empty — allowing the leader to catch it up by a small
/// delta (AppendEntries) instead of a full snapshot install.
///
/// <para><b>Sequence for a production node:</b>
/// <list type="number">
///   <item>Copy the Full backup's checkpoint files into the node's persistence data
///         directory (RocksDB hard-link dir or SQLite file).</item>
///   <item>Call <see cref="BootstrapNode"/> with the backup chain, the persistence backend
///         pointed at that directory, and the Raft WAL adapter. The method replays the
///         incremental WAL segments up to T and writes synthetic WAL checkpoint entries.</item>
///   <item>Start the node with <c>--join-existing --initial-cluster &lt;seeds&gt;</c>. Raft
///         reads the WAL checkpoint and tells the leader "I'm at index N"; the leader ships
///         only N+1..now via AppendEntries.</item>
/// </list>
/// </para>
///
/// <para><b>Guard rail:</b> if T is older than <c>now − pitrWindow</c> the leader has
/// already compacted past that WAL index and will fall back to <c>InstallSnapshot</c>
/// regardless. In that case the method throws <see cref="BackupDriverException"/> with a
/// diagnostic message directing the operator to use a more recent backup or to let the node
/// bootstrap from the leader snapshot directly.</para>
/// </summary>
internal static class BootstrapHelper
{
    /// <summary>
    /// Seeds <paramref name="backend"/> from a validated backup chain and writes synthetic
    /// <see cref="RaftLogType.CommittedCheckpoint"/> entries into <paramref name="walAdapter"/>
    /// at the per-partition restored index.
    /// </summary>
    /// <param name="chain">
    /// Resolved, validated backup chain (Full first, most-recent incremental last).
    /// Produced by <see cref="BackupCatalog.ResolveAndValidate"/>.
    /// </param>
    /// <param name="artifactsDir">
    /// Root directory where per-backup artifact subdirectories live
    /// (<c>{artifactsDir}/{backupId:N}/</c>).
    /// </param>
    /// <param name="targetTime">HLC stop-timestamp T. Entries with <c>Time &gt; T</c> are not applied.</param>
    /// <param name="backend">
    /// Persistence backend pre-loaded with the Full backup's checkpoint state. For
    /// <see cref="MemoryPersistenceBackend"/> use <c>OpenCheckpoint(checkpointPath)</c>;
    /// for production backends ensure the checkpoint files are in the data directory before
    /// calling this method.
    /// </param>
    /// <param name="walAdapter">Raft WAL adapter for this node. Synthetic checkpoint entries are written here.</param>
    /// <param name="pitrWindow">Configured PITR retention window.</param>
    /// <param name="nowUtc">Wall-clock reference for the window guard rail.</param>
    /// <param name="manifestMacKey">
    /// The node's manifest-authentication key, or null when none is configured. When set, every manifest
    /// in the chain must carry a matching MAC, exactly as a restore requires: the seed trusts each
    /// manifest's coverage bounds and WAL indexes, so a tampered or stripped manifest must fail here.
    /// </param>
    /// <param name="clusterId">
    /// The node's backup cluster id, or null/empty when none is configured. When set, a manifest that
    /// names a different cluster fails the bootstrap. A restore only requires a chain to be consistent
    /// with itself, because its output is an offline directory; a seed goes further, because its
    /// checkpoints tell the leader of <i>this</i> cluster that the node already holds entries up to an
    /// index, and a foreign chain's indexes describe another cluster's log. A manifest with no cluster id
    /// (taken before an id was configured) is accepted, as everywhere else in the catalog.
    /// </param>
    /// <returns>Statistics from the WAL segment replay phase.</returns>
    /// <exception cref="BackupDriverException">
    /// Thrown when <paramref name="targetTime"/> is outside the PITR window or the chain is invalid.
    /// </exception>
    /// <exception cref="BackupArtifactException">A manifest is unauthenticated or its MAC does not match.</exception>
    /// <exception cref="BackupChainException">A manifest belongs to a different cluster.</exception>
    internal static async Task<RestoreResult> BootstrapNodeAsync(
        IReadOnlyList<BackupManifest> chain,
        IBackupArtifactStore artifacts,
        HLCTimestamp targetTime,
        IPersistenceBackend backend,
        IWAL walAdapter,
        TimeSpan pitrWindow,
        DateTime nowUtc,
        TimeSpan baseSnapshotInterval = default,
        byte[]? manifestMacKey = null,
        string? clusterId = null)
    {
        // Authenticate every manifest before any of its fields is trusted: the checks below read its
        // partitions, coverage and cluster id, and the seed writes its indexes into the WAL.
        foreach (BackupManifest manifest in chain)
            BackupManifestMac.Verify(manifest, manifestMacKey);

        EnsureChainBelongsToCluster(chain, clusterId);

        // A chain that covers only part of the cluster (captured by a node hosting a subset of the
        // partitions under replica placement) must not seed a node at all — the missing partitions'
        // absence would be indistinguishable from data loss. Fail closed before touching anything.
        BackupChainCoverage.ValidatePartitionCoverage(chain);

        // Validate the target against the chain's exact recoverable coverage BEFORE touching the
        // backend or WAL — a target below the base cut or above captured coverage (or a chain with an
        // unknown lower bound) fails closed here, so we never seed WAL checkpoints for a state that
        // was never reconstructed. Zero resolves to the validated natural end.
        targetTime = BackupChainCoverage.Resolve(chain, targetTime);

        // Guard rail: T must be within the effective PITR window.
        // The real WAL compaction floor on the leader is not exactly now−pitrWindow: compaction
        // fires every baseSnapshotInterval, so the oldest retained entry is up to one interval
        // older than now−pitrWindow. Subtracting baseSnapshotInterval gives the worst-case floor
        // and avoids falsely rejecting backups that are actually still above the compaction floor.
        long floorMs = (long)((nowUtc - pitrWindow - baseSnapshotInterval - DateTime.UnixEpoch).TotalMilliseconds);
        HLCTimestamp floor = new(0, floorMs, 0);

        if (targetTime.CompareTo(floor) < 0)
            throw new BackupDriverException(
                $"Backup target time {targetTime} is older than the PITR window floor " +
                $"({floor}; now − {pitrWindow.TotalHours:F0} h − {baseSnapshotInterval.TotalMinutes:F0} min snapshot interval). " +
                "The leader has compacted past this WAL index; it will fall back to a full " +
                "snapshot install, making the seed pointless. " +
                "Use a more recent backup within the retention window, or let the node " +
                "bootstrap directly from the leader snapshot.");

        // Replay incremental WAL segments into the backend up to T.
        RestoreResult restoreResult = await RestoreEngine.RestoreAsync(chain, artifacts, targetTime, backend).ConfigureAwait(false);

        // Pin the WAL adapter at the per-partition applied high-water mark so the Raft
        // layer presents this node at the correct index when it joins.
        SeedWalCheckpoints(chain, targetTime, backend, walAdapter);

        return restoreResult;
    }

    /// <summary>
    /// Refuses a chain any manifest of which names a cluster other than <paramref name="clusterId"/>.
    /// Dormant when the node has no cluster id. A manifest without one passes, because it was taken
    /// before an id was configured and carries no claim either way.
    /// </summary>
    private static void EnsureChainBelongsToCluster(IReadOnlyList<BackupManifest> chain, string? clusterId)
    {
        if (string.IsNullOrWhiteSpace(clusterId))
            return;

        foreach (BackupManifest manifest in chain)
        {
            if (manifest.ClusterId is not null && !string.Equals(manifest.ClusterId, clusterId, StringComparison.Ordinal))
                throw new BackupChainException(
                    $"Backup {manifest.BackupId:N} belongs to cluster '{manifest.ClusterId}', not to this node's cluster " +
                    $"'{clusterId}'. Seeding a node from another cluster's chain would report WAL indexes this cluster " +
                    "never wrote; bootstrap from a backup of this cluster instead.");
        }
    }

    /// <summary>
    /// Determines the per-partition "safe" WAL index — the highest <see cref="PartitionBackupRange.ToIndex"/>
    /// among ranges whose <see cref="PartitionBackupRange.ToHlc"/> is at or before
    /// <paramref name="targetTime"/> — installs one <see cref="RaftLogType.CommittedCheckpoint"/>
    /// boundary per partition into the WAL, and records the same index as the store's
    /// application-durability floor.
    ///
    /// <para><b>Why a snapshot boundary and not a plain write:</b> every WAL backend advances its
    /// persisted last-checkpoint id on a plain <see cref="IWAL.Write(List{ValueTuple{int, List{RaftLog}}})"/>
    /// only when every id below the checkpoint is present, because a checkpoint row that lands over a
    /// replication gap must not certify entries the node never held. The bootstrap WAL holds nothing
    /// below the restored index, so a plain write left the persisted id at "none": the node then
    /// presented to the leader at index 0, was backfilled from 1, and the seed bought nothing. The
    /// restored store is the certificate for the whole prefix — exactly what an installed snapshot is —
    /// and <see cref="IWAL.InstallSnapshotBoundary"/> stamps the row and the persisted id together
    /// without the prefix scan. Kommander's restart reconstruction trusts that persisted id and
    /// seeds the presence and commit frontiers from it.</para>
    ///
    /// <para>When an incremental's per-partition range straddles <paramref name="targetTime"/>
    /// (<c>ToHlc &gt; T</c> but some entries within have <c>Time ≤ T</c>), the previous
    /// range's <c>ToIndex</c> is used as a conservative floor. The leader will re-deliver
    /// entries already applied; per-shard upsert idempotency makes this safe.</para>
    ///
    /// <para><b>Term correctness:</b> the synthetic checkpoint carries <see cref="PartitionBackupRange.ToTerm"/>
    /// — the Raft term of the WAL entry at <c>ToIndex</c>, captured by <see cref="BackupDriver"/>
    /// at backup time. The leader's Log Matching check (<c>prevLogTerm</c>) compares against this
    /// field; a mismatch (e.g. term 0) triggers a <c>LogMismatch</c> rejection that backtracks
    /// <c>nextIndex</c> to the start, negating the delta catch-up the seed is meant to provide.</para>
    /// </summary>
    private static void SeedWalCheckpoints(
        IReadOnlyList<BackupManifest> chain,
        HLCTimestamp targetTime,
        IPersistenceBackend backend,
        IWAL walAdapter)
    {
        // Per-partition: the highest ToIndex/ToTerm where ToHlc ≤ targetTime.
        Dictionary<int, (long index, HLCTimestamp hlc, long term)> safeWater = [];

        foreach (BackupManifest manifest in chain)
        {
            foreach (PartitionBackupRange range in manifest.PartitionRanges)
            {
                if (range.ToHlc.CompareTo(targetTime) > 0)
                {
                    // This range straddles T: the restore stopped somewhere inside it.
                    // Keep the previous safe high-water mark (from the Full or earlier
                    // incremental) rather than over-estimating the applied index.
                    continue;
                }

                if (!safeWater.TryGetValue(range.PartitionId, out (long index, HLCTimestamp hlc, long term) cur) ||
                    range.ToIndex > cur.index)
                {
                    safeWater[range.PartitionId] = (range.ToIndex, range.ToHlc, range.ToTerm);
                }
            }
        }

        if (safeWater.Count == 0)
            return;

        // Seed one partition at a time and verify each step is durable. Both WAL steps overwrite by
        // (partitionId, id) and the floor store only ever raises, so this is safe to retry: re-running
        // BootstrapNode re-applies the idempotent backend restore and re-seeds without duplication.
        // Seeding per partition (rather than one batch) lets a partial failure be detected and reported
        // precisely rather than reported as a successful bootstrap while the WAL trails the state
        // already restored into the backend.
        List<(int PartitionId, long Floor)> floors = new(safeWater.Count);

        foreach (KeyValuePair<int, (long index, HLCTimestamp hlc, long term)> kv in safeWater)
        {
            int partitionId = kv.Key;
            (long index, HLCTimestamp hlc, long term) = kv.Value;

            // The boundary install is the step that makes the node present at the restored index: it
            // persists the last-checkpoint id in the same atomic write as the checkpoint row (see the
            // method summary). On a fresh WAL there is no row at the index, so the install finds no
            // suffix to discard; on a retry it finds the row it wrote before, with the same term.
            (RaftOperationStatus status, _) = walAdapter.InstallSnapshotBoundary(partitionId, index, term, sync: true);
            if (status != RaftOperationStatus.Success)
                throw SeedFailed(partitionId, index, status, "install the WAL checkpoint boundary");

            // The boundary row carries no timestamp. Re-stamp it with the range's HLC so the restart clock
            // floor covers the restored data: Kommander merges the maximum restored entry time into the node
            // clock before the partition serves, and a node whose wall clock lags the data must not mint a
            // timestamp below it. The persisted last-checkpoint id already equals this index, so this write
            // neither scans the prefix nor moves the id; it only overwrites the row by key.
            status = walAdapter.Write(
            [
                (partitionId, [new RaftLog
                {
                    Id   = index,
                    Type = RaftLogType.CommittedCheckpoint,
                    Time = hlc,
                    Term = term,
                }])
            ]);
            if (status != RaftOperationStatus.Success)
                throw SeedFailed(partitionId, index, status, "stamp the WAL checkpoint with its timestamp");

            // The store holds every entry the chain delivered through this index, which is exactly what the
            // application-durability floor certifies. A floor inherited from the full backup's checkpoint
            // sits at the full's index, below the checkpoint seeded here, and Kommander then reads its
            // restart replay window from the lower of the two. Never lowered: a floor the store already
            // vouched for stays.
            floors.Add((partitionId, Math.Max(backend.GetDurabilityFloor(partitionId), index)));
        }

        // Stored after every WAL boundary landed, so a failure between the two leaves the floor stale-low
        // (safe: Kommander redelivers from the checkpoint) and never ahead of a WAL that was not seeded.
        if (!backend.StoreDurabilityFloors(floors))
            throw new BackupDriverException(
                "Failed to record the application-durability floor for the seeded partitions in the " +
                "persistence backend; bootstrap did not complete. The backend restore already applied and " +
                "the WAL checkpoints already seeded are idempotent, so the bootstrap can be safely re-run; " +
                "do not start the node until it completes successfully.");
    }

    private static BackupDriverException SeedFailed(int partitionId, long index, RaftOperationStatus status, string step) =>
        new($"Failed to {step} for partition {partitionId} at index {index} (status {status}); bootstrap did " +
            "not complete. The backend restore already applied is idempotent and every seed step overwrites " +
            "by key, so the bootstrap can be safely re-run; do not start the node until it completes successfully.");
}

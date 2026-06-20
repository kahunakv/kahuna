
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
    /// <returns>Statistics from the WAL segment replay phase.</returns>
    /// <exception cref="BackupDriverException">
    /// Thrown when <paramref name="targetTime"/> is outside the PITR window or the chain is invalid.
    /// </exception>
    internal static RestoreResult BootstrapNode(
        IReadOnlyList<BackupManifest> chain,
        string artifactsDir,
        HLCTimestamp targetTime,
        IPersistenceBackend backend,
        IWAL walAdapter,
        TimeSpan pitrWindow,
        DateTime nowUtc,
        TimeSpan baseSnapshotInterval = default)
    {
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
        RestoreResult restoreResult = RestoreEngine.Restore(chain, artifactsDir, targetTime, backend);

        // Pin the WAL adapter at the per-partition applied high-water mark so the Raft
        // layer presents this node at the correct index when it joins.
        SeedWalCheckpoints(chain, targetTime, walAdapter);

        return restoreResult;
    }

    /// <summary>
    /// Determines the per-partition "safe" WAL index — the highest <see cref="PartitionBackupRange.ToIndex"/>
    /// among ranges whose <see cref="PartitionBackupRange.ToHlc"/> is at or before
    /// <paramref name="targetTime"/> — and writes one synthetic
    /// <see cref="RaftLogType.CommittedCheckpoint"/> entry per partition into the WAL.
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

        List<(int, List<RaftLog>)> writes = [];

        foreach (KeyValuePair<int, (long index, HLCTimestamp hlc, long term)> kv in safeWater)
        {
            writes.Add((kv.Key, [new RaftLog
            {
                Id   = kv.Value.index,
                Type = RaftLogType.CommittedCheckpoint,
                Time = kv.Value.hlc,
                Term = kv.Value.term,
            }]));
        }

        walAdapter.Write(writes);
    }
}

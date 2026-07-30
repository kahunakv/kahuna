
using System.Text.Json;
using Kahuna.Server.Persistence.Backend;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Orchestrates full and incremental backups by combining a storage-engine checkpoint
/// (for full backups) or serialised WAL segments (for incrementals) with a
/// <see cref="BackupManifest"/> stored in a <see cref="BackupCatalog"/>.
///
/// <para>The two core operations are exposed both as instance methods (production use,
/// via <see cref="IRaft"/>) and as internal static methods that accept <see cref="IWAL"/>
/// and a partition list directly (test use without a live cluster).</para>
///
/// <para><b>Flush-before-checkpoint contract:</b> a Full backup snapshots the storage engine
/// at the WAL committed-max (M). For the snapshot to actually contain all data through M, every
/// dirty write that corresponds to committed WAL entries must be flushed to the backend before
/// the snapshot is taken. Pass <paramref name="flushBeforeCheckpoint"/> in the constructor (or
/// the static overloads) to supply this guarantee; omitting it means the snapshot may only
/// reach the last spontaneous flush position F ≤ M, leaving [F+1, M] in neither the checkpoint
/// nor any subsequent incremental segment.</para>
/// </summary>
internal sealed class BackupDriver
{
    private readonly IRaft _raft;
    private readonly IPersistenceBackend _persistenceBackend;

    /// <summary>
    /// Optional async callback that drains all pending dirty objects to the storage backend
    /// before the storage-engine checkpoint is taken.  In production this is wired to
    /// <c>KahunaManager.FlushPersistenceAsync</c>; tests may supply a no-op or a delegate
    /// that pre-populates the backend with expected data.
    /// </summary>
    private readonly Func<Task>? _flushBeforeCheckpoint;

    public BackupDriver(IRaft raft, IPersistenceBackend persistenceBackend,
        Func<Task>? flushBeforeCheckpoint = null)
    {
        _raft = raft;
        _persistenceBackend = persistenceBackend;
        _flushBeforeCheckpoint = flushBeforeCheckpoint;
    }

    /// <summary>
    /// Flushes all pending writes to the storage engine, snapshots it, captures per-partition
    /// WAL coverage, writes a Full <see cref="BackupManifest"/> to <paramref name="catalog"/>,
    /// and returns the manifest.  Artifact files land in <c>{artifactsDir}/{backupId}/</c>.
    /// When <paramref name="snapshotT"/> is supplied each partition's coverage is capped at the
    /// last committed entry with <c>Time ≤ T</c> and T is recorded in the manifest as the
    /// cluster-wide consistent-cut timestamp.
    /// </summary>
    public Task<BackupManifest> TakeFullBackupAsync(string artifactsDir, BackupCatalog catalog,
        HLCTimestamp? snapshotT = null, CancellationToken ct = default) =>
        RunFullAsync(_raft.WalAdapter, _raft.GetPartitionMap(), _persistenceBackend,
            artifactsDir, catalog, _flushBeforeCheckpoint, snapshotT, ct);

    /// <summary>
    /// Reads committed WAL entries since the parent backup's <c>ToIndex</c>, serialises them
    /// as per-partition segment files, writes an Incremental <see cref="BackupManifest"/> to
    /// <paramref name="catalog"/>, and returns the manifest.
    /// Throws <see cref="BackupDriverException"/> when the parent range starts below the WAL
    /// compaction floor (a new full backup is required in that case).
    /// Artifact files land in <c>{artifactsDir}/{backupId}/</c>.
    /// When <paramref name="snapshotT"/> is supplied each partition's segment is capped at the
    /// first entry whose <c>Time > T</c> and T is recorded in the manifest.
    /// </summary>
    public BackupManifest TakeIncrementalBackup(Guid parentBackupId, string artifactsDir,
        BackupCatalog catalog, HLCTimestamp? snapshotT = null, CancellationToken ct = default) =>
        RunIncremental(_raft.WalAdapter, _raft.GetPartitionMap(), parentBackupId, artifactsDir, catalog, snapshotT, ct);

    // ── core logic (internal so tests can exercise without an IRaft) ─────────────────────

    /// <summary>
    /// <paramref name="flushBeforeCheckpoint"/> is awaited before the storage-engine snapshot
    /// so the checkpoint genuinely contains all committed data through the WAL committed-max.
    /// Pass <c>null</c> only in tests where the backend is already pre-populated.
    /// </summary>
    /// <param name="snapshotT">
    /// When provided, each partition's ToIndex is capped at the last committed entry with
    /// <c>Time ≤ snapshotT</c>, and the timestamp is stored in the manifest as the
    /// cluster-wide consistent-cut anchor. Use this for coordinated multi-partition backups
    /// where every shard must present a state as-of the same HLC.
    /// </param>
    internal static async Task<BackupManifest> RunFullAsync(
        IWAL wal,
        IReadOnlyList<RaftPartitionRange> partitions,
        IPersistenceBackend persistenceBackend,
        string artifactsDir,
        BackupCatalog catalog,
        Func<Task>? flushBeforeCheckpoint = null,
        HLCTimestamp? snapshotT = null,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        Guid backupId = Guid.NewGuid();
        string artifactPath = Path.Combine(artifactsDir, backupId.ToString("N"));
        Directory.CreateDirectory(artifactPath);

        try
        {
            // Read M (per-partition committed max) BEFORE flushing. The flush drains everything
            // committed as of its call, which is a superset of M. The checkpoint that follows only
            // adds more — so checkpoint ⊇ [1..M] is guaranteed. The safe order is: read M → flush
            // → checkpoint. Reversing flush and read leaves a window where a write that commits
            // after the flush but before M is read is counted in ToIndex yet absent from the backend
            // when the checkpoint fires (the original gap, just narrower).
            // An exact as-of image is required for a full backup: a physical-copy fallback would
            // over-include state committed after the cut, making the recorded BaseCut a lie. Fail
            // closed rather than publish an unprovable base image.
            if (!persistenceBackend.SupportsExactAsOfCheckpoint)
                throw new BackupDriverException(
                    "The persistence backend cannot produce an exact as-of checkpoint; a full backup " +
                    "with a proven base cut cannot be taken.") { ExactCheckpointUnavailable = true };

            List<PartitionBackupRange> ranges = [];
            long maxAppliedIndex = 0;
            HLCTimestamp maxCommittedHlc = default;

            foreach (RaftPartitionRange partition in partitions)
            {
                ct.ThrowIfCancellationRequested();

                if (partition.State is RaftPartitionState.Draining or RaftPartitionState.Removed)
                    continue;

                int partitionId = partition.PartitionId;
                (long lastId, HLCTimestamp lastHlc, long lastTerm) = snapshotT.HasValue
                    ? FindLastCommittedAtOrBefore(wal, partitionId, snapshotT.Value, ct)
                    : FindLastCommitted(wal, partitionId, ct);
                if (lastId <= 0)
                    continue;

                // Full ranges are always anchored at index 1; FromHlc is left at default because
                // the checkpoint image, not a WAL entry, is the actual starting point on restore.
                ranges.Add(PartitionBackupRange.Create(partitionId, 1, default, lastId, lastHlc, lastTerm));

                // The sidecar index is the largest committed index seen; it is per-partition-derived
                // metadata, NOT a global position. The cut is taken from HLC order below, not from
                // whichever partition happens to hold the largest index.
                if (lastId > maxAppliedIndex)
                    maxAppliedIndex = lastId;
                if (lastHlc.CompareTo(maxCommittedHlc) > 0)
                    maxCommittedHlc = lastHlc;
            }

            if (flushBeforeCheckpoint is not null)
                await flushBeforeCheckpoint();

            ct.ThrowIfCancellationRequested();

            // Cut the base image at a single HLC: the coordinated snapshot T when supplied, else the
            // maximum committed HLC across all captured partitions (by HLC order — partition log
            // indexes are partition-local and cannot be compared across partitions). No committed
            // state newer than the cut belongs in the image — otherwise replay (forward-only, stops
            // at the restore target) could never remove a post-cut value.
            HLCTimestamp cut = snapshotT ?? maxCommittedHlc;

            // Every captured range must be at or below the cut — otherwise the manifest would claim
            // coverage the trimmed checkpoint does not contain.
            foreach (PartitionBackupRange range in ranges)
            {
                if (range.ToHlc.CompareTo(cut) > 0)
                    throw new BackupDriverException(
                        $"Partition {range.PartitionId} range ends at {range.ToHlc} which is after the " +
                        $"base cut {cut}; refusing to publish an inconsistent full backup.");
            }

            string checkpointPath = Path.Combine(artifactPath, "checkpoint");
            persistenceBackend.CreateCheckpointAsOf(checkpointPath, maxAppliedIndex, cut, ct);

            // Hash every file the checkpoint produced (data files AND the sidecar), not just the
            // sidecar, so a truncated or altered checkpoint is caught before replay.
            (Dictionary<string, string> checksums, Dictionary<string, long> sizes) =
                BackupArtifactVerifier.HashDirectory(checkpointPath, "checkpoint/", ct);

            BackupManifest manifest = BackupManifest.CreateFull(ranges);
            manifest.BackupId = backupId;
            manifest.Checksums = checksums;
            manifest.Sizes = sizes;
            manifest.SetBaseCut(cut);
            if (snapshotT.HasValue)
                manifest.SetClusterSnapshotTime(snapshotT.Value);

            // Fail closed: never publish a manifest whose artifacts don't verify.
            BackupArtifactVerifier.Verify(manifest, artifactsDir, ct);

            catalog.Put(manifest);
            return manifest;
        }
        catch
        {
            TryDeleteDirectory(artifactPath);
            throw;
        }
    }

    /// <param name="snapshotT">
    /// When provided, each partition's WAL segment is capped at the first entry whose
    /// <c>Time > snapshotT</c> (assuming per-partition HLC monotonicity), and the timestamp
    /// is stored in the manifest. Combine with a Full backup taken at the same T to form a
    /// consistent cluster-wide cut.
    /// </param>
    internal static BackupManifest RunIncremental(
        IWAL wal,
        IReadOnlyList<RaftPartitionRange> partitions,
        Guid parentBackupId,
        string artifactsDir,
        BackupCatalog catalog,
        HLCTimestamp? snapshotT = null,
        CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        BackupManifest? parentManifest = catalog.Get(parentBackupId);
        if (parentManifest is null)
            throw new BackupDriverException(
                $"Parent backup {parentBackupId:N} not found in catalog.") { ParentMissing = true };

        Guid backupId = Guid.NewGuid();
        string artifactPath = Path.Combine(artifactsDir, backupId.ToString("N"));
        Directory.CreateDirectory(artifactPath);

        try
        {
            // Derive each partition's start from its TRANSITIVE high-water mark across the whole
            // ancestor chain (Full → … → parent), not just the immediate parent. A sparse/empty
            // parent that omitted an unchanged partition must not make this incremental treat that
            // partition as new (which would restart at the WAL floor/1 — silently skipping entries
            // after compaction, or duplicating the whole WAL without it).
            IReadOnlyList<BackupManifest> ancestors = catalog.ResolveChain(parentBackupId, ct);
            Dictionary<int, PartitionBackupRange> parentRanges = BuildHighWaterMarks(ancestors);

            List<PartitionBackupRange> ranges = [];
            Dictionary<string, string> checksums = [];
            Dictionary<string, long> sizes = [];

            foreach (RaftPartitionRange partition in partitions)
            {
                ct.ThrowIfCancellationRequested();

                if (partition.State is RaftPartitionState.Draining or RaftPartitionState.Removed)
                    continue;

                int partitionId = partition.PartitionId;
                parentRanges.TryGetValue(partitionId, out PartitionBackupRange? pr);
                long floor = wal.GetLastCheckpoint(partitionId);

                long fromIndex;
                if (pr is not null)
                {
                    fromIndex = pr.ToIndex + 1;
                    // Transitive coverage ends before the compaction floor: entries in
                    // [highWater.ToIndex+1, floor) may already be gone. A new full is required.
                    if (floor > 0 && fromIndex < floor)
                        throw new BackupDriverException(
                            $"Partition {partitionId}: incremental would start at WAL index {fromIndex} " +
                            $"but the compaction floor is {floor}; a new full backup is required.")
                        {
                            NeedsFullBackup = true
                        };
                }
                else
                {
                    // Partition appears in NO ancestor manifest — genuinely new. Start from the floor
                    // so we don't request entries that compaction has already removed.
                    fromIndex = floor > 0 ? floor : 1;
                }

                (List<WalSegmentEntry> segment, long toIndex, HLCTimestamp toHlc, long toTerm, HLCTimestamp fromHlc) =
                    ReadSegment(wal, partitionId, fromIndex, snapshotT, ct);

                if (toIndex == 0)
                    continue;

                string relPath = $"partition_{partitionId}.wal";
                string walFile = Path.Combine(artifactPath, relPath);
                WriteSegmentFile(walFile, segment);
                checksums[relPath] = BackupArtifactVerifier.ComputeSha256(walFile);
                sizes[relPath] = new FileInfo(walFile).Length;

                ranges.Add(PartitionBackupRange.Create(partitionId, fromIndex, fromHlc, toIndex, toHlc, toTerm));
            }

            BackupManifest manifest = BackupManifest.CreateIncremental(parentBackupId, ranges);
            manifest.BackupId = backupId;
            manifest.Checksums = checksums;
            manifest.Sizes = sizes;
            if (snapshotT.HasValue)
                manifest.SetClusterSnapshotTime(snapshotT.Value);

            // Fail closed: never publish a manifest whose artifacts don't verify. The verifier
            // treats a genuinely empty incremental (no ranges, no segments) as valid.
            BackupArtifactVerifier.Verify(manifest, artifactsDir, ct);

            catalog.Put(manifest);
            return manifest;
        }
        catch
        {
            TryDeleteDirectory(artifactPath);
            throw;
        }
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Builds the per-partition high-water mark across a resolved ancestor chain: for each partition,
    /// the range with the greatest <see cref="PartitionBackupRange.ToIndex"/> seen anywhere in the
    /// chain. Used so a sparse/empty intermediate manifest cannot lower a partition's continuation point.
    /// </summary>
    private static Dictionary<int, PartitionBackupRange> BuildHighWaterMarks(IReadOnlyList<BackupManifest> chain)
    {
        Dictionary<int, PartitionBackupRange> highWater = [];
        foreach (BackupManifest manifest in chain)
        {
            foreach (PartitionBackupRange range in manifest.PartitionRanges)
            {
                if (!highWater.TryGetValue(range.PartitionId, out PartitionBackupRange? cur) || range.ToIndex > cur.ToIndex)
                    highWater[range.PartitionId] = range;
            }
        }
        return highWater;
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort cleanup of a partial artifact directory; a leftover dir with no manifest
            // is swept by the orphan-cleanup path and never chained (it has no catalog entry).
        }
    }

    private const int PageSize = 256;

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    /// <summary>
    /// Scans backward through the WAL in page-sized windows to find the last committed entry.
    /// Pages until a committed entry is found or the start of the log is reached.
    /// Returns (0, default, 0) when the partition has no committed entries at all.
    /// </summary>
    internal static (long id, HLCTimestamp hlc, long term) FindLastCommitted(
        IWAL wal, int partitionId, CancellationToken ct = default)
    {
        long maxLog = wal.GetMaxLog(partitionId);
        if (maxLog <= 0)
            return (0, default, 0);

        long ceiling = maxLog;
        while (ceiling > 0)
        {
            ct.ThrowIfCancellationRequested();
            long start = Math.Max(1, ceiling - PageSize + 1);
            List<RaftLog> batch = wal.ReadLogsRange(partitionId, start, PageSize);
            if (batch.Count == 0)
                break;

            for (int i = batch.Count - 1; i >= 0; i--)
            {
                if (batch[i].Type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint)
                    return (batch[i].Id, batch[i].Time, batch[i].Term);
            }

            ceiling = start - 1;
        }

        return (0, default, 0);
    }

    /// <summary>
    /// Scans backward through the WAL to find the last committed entry whose HLC is at or
    /// before <paramref name="snapshotT"/>.  Used for coordinated backups where every partition
    /// must be capped at the same cluster-wide timestamp T.
    /// Returns (0, default, 0) when no qualifying committed entry exists.
    /// </summary>
    private static (long id, HLCTimestamp hlc, long term) FindLastCommittedAtOrBefore(
        IWAL wal, int partitionId, HLCTimestamp snapshotT, CancellationToken ct = default)
    {
        long maxLog = wal.GetMaxLog(partitionId);
        if (maxLog <= 0)
            return (0, default, 0);

        long ceiling = maxLog;
        while (ceiling > 0)
        {
            ct.ThrowIfCancellationRequested();
            long start = Math.Max(1, ceiling - PageSize + 1);
            List<RaftLog> batch = wal.ReadLogsRange(partitionId, start, PageSize);
            if (batch.Count == 0)
                break;

            for (int i = batch.Count - 1; i >= 0; i--)
            {
                RaftLog log = batch[i];
                if (log.Type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint
                    && log.Time.CompareTo(snapshotT) <= 0)
                    return (log.Id, log.Time, log.Term);
            }

            ceiling = start - 1;
        }

        return (0, default, 0);
    }

    /// <summary>
    /// Pages through the WAL from <paramref name="fromIndex"/> forward, collecting committed
    /// entries.  When <paramref name="snapshotT"/> is provided, collection stops at the first
    /// entry whose <c>Time > snapshotT</c> (per-partition HLC monotonicity is assumed, which
    /// holds in normal operation).
    /// Returns the segment entries, the final log id/hlc/term, and the HLC of the first entry.
    /// </summary>
    private static (List<WalSegmentEntry> entries, long toId, HLCTimestamp toHlc, long toTerm, HLCTimestamp fromHlc)
        ReadSegment(IWAL wal, int partitionId, long fromIndex, HLCTimestamp? snapshotT = null,
            CancellationToken ct = default)
    {
        List<WalSegmentEntry> entries = [];
        long toId = 0;
        HLCTimestamp toHlc = default;
        long toTerm = 0;
        HLCTimestamp fromHlc = default;
        bool first = true;
        bool hitCap = false;
        long cursor = fromIndex;

        while (!hitCap)
        {
            ct.ThrowIfCancellationRequested();
            List<RaftLog> batch = wal.ReadLogsRange(partitionId, cursor, PageSize);
            if (batch.Count == 0)
                break;

            foreach (RaftLog log in batch)
            {
                if (log.Type is not (RaftLogType.Committed or RaftLogType.CommittedCheckpoint))
                    continue;

                if (snapshotT.HasValue && log.Time.CompareTo(snapshotT.Value) > 0)
                {
                    hitCap = true;
                    break;
                }

                if (first)
                {
                    fromHlc = log.Time;
                    first = false;
                }

                entries.Add(WalSegmentEntry.From(log));
                toId = log.Id;
                toHlc = log.Time;
                toTerm = log.Term;
            }

            long lastInBatch = batch[^1].Id;
            if (batch.Count < PageSize)
                break;

            cursor = lastInBatch + 1;
        }

        return (entries, toId, toHlc, toTerm, fromHlc);
    }

    private static void WriteSegmentFile(string path, List<WalSegmentEntry> entries)
    {
        string tmp = path + ".tmp_" + Guid.NewGuid().ToString("N")[..8];
        File.WriteAllText(tmp, JsonSerializer.Serialize(entries, JsonOptions));
        File.Move(tmp, path, overwrite: true);
    }
}

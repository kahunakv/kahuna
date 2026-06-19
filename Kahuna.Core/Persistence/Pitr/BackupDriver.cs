
using System.Security.Cryptography;
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
    /// </summary>
    public Task<BackupManifest> TakeFullBackupAsync(string artifactsDir, BackupCatalog catalog) =>
        RunFullAsync(_raft.WalAdapter, _raft.GetPartitionMap(), _persistenceBackend,
            artifactsDir, catalog, _flushBeforeCheckpoint);

    /// <summary>
    /// Reads committed WAL entries since the parent backup's <c>ToIndex</c>, serialises them
    /// as per-partition segment files, writes an Incremental <see cref="BackupManifest"/> to
    /// <paramref name="catalog"/>, and returns the manifest.
    /// Throws <see cref="BackupDriverException"/> when the parent range starts below the WAL
    /// compaction floor (a new full backup is required in that case).
    /// Artifact files land in <c>{artifactsDir}/{backupId}/</c>.
    /// </summary>
    public BackupManifest TakeIncrementalBackup(Guid parentBackupId, string artifactsDir,
        BackupCatalog catalog) =>
        RunIncremental(_raft.WalAdapter, _raft.GetPartitionMap(), parentBackupId, artifactsDir, catalog);

    // ── core logic (internal so tests can exercise without an IRaft) ─────────────────────

    /// <summary>
    /// <paramref name="flushBeforeCheckpoint"/> is awaited before the storage-engine snapshot
    /// so the checkpoint genuinely contains all committed data through the WAL committed-max.
    /// Pass <c>null</c> only in tests where the backend is already pre-populated.
    /// </summary>
    internal static async Task<BackupManifest> RunFullAsync(
        IWAL wal,
        IReadOnlyList<RaftPartitionRange> partitions,
        IPersistenceBackend persistenceBackend,
        string artifactsDir,
        BackupCatalog catalog,
        Func<Task>? flushBeforeCheckpoint = null)
    {
        Guid backupId = Guid.NewGuid();
        string artifactPath = Path.Combine(artifactsDir, backupId.ToString("N"));
        Directory.CreateDirectory(artifactPath);

        // Read M (per-partition committed max) BEFORE flushing. The flush drains everything
        // committed as of its call, which is a superset of M. The checkpoint that follows only
        // adds more — so checkpoint ⊇ [1..M] is guaranteed. The safe order is: read M → flush
        // → checkpoint. Reversing flush and read leaves a window where a write that commits
        // after the flush but before M is read is counted in ToIndex yet absent from the backend
        // when the checkpoint fires (the original gap, just narrower).
        List<PartitionBackupRange> ranges = [];
        long maxAppliedIndex = 0;
        HLCTimestamp maxAppliedTime = default;

        foreach (RaftPartitionRange partition in partitions)
        {
            if (partition.State is RaftPartitionState.Draining or RaftPartitionState.Removed)
                continue;

            int partitionId = partition.PartitionId;
            (long lastId, HLCTimestamp lastHlc) = FindLastCommitted(wal, partitionId);
            if (lastId <= 0)
                continue;

            // Full ranges are always anchored at index 1; FromHlc is left at default because
            // the checkpoint image, not a WAL entry, is the actual starting point on restore.
            ranges.Add(PartitionBackupRange.Create(partitionId, 1, default, lastId, lastHlc));

            if (lastId > maxAppliedIndex)
            {
                maxAppliedIndex = lastId;
                maxAppliedTime = lastHlc;
            }
        }

        if (flushBeforeCheckpoint is not null)
            await flushBeforeCheckpoint();

        string checkpointPath = Path.Combine(artifactPath, "checkpoint");
        persistenceBackend.CreateCheckpoint(checkpointPath, maxAppliedIndex, maxAppliedTime);

        Dictionary<string, string> checksums = [];
        string manifestSidecar = Path.Combine(checkpointPath, CheckpointManifest.FileName);
        if (File.Exists(manifestSidecar))
            checksums["checkpoint/" + CheckpointManifest.FileName] = ComputeSha256(manifestSidecar);

        BackupManifest manifest = BackupManifest.CreateFull(ranges);
        manifest.BackupId = backupId;
        manifest.Checksums = checksums;

        catalog.Put(manifest);
        return manifest;
    }

    internal static BackupManifest RunIncremental(
        IWAL wal,
        IReadOnlyList<RaftPartitionRange> partitions,
        Guid parentBackupId,
        string artifactsDir,
        BackupCatalog catalog)
    {
        BackupManifest? parentManifest = catalog.Get(parentBackupId);
        if (parentManifest is null)
            throw new BackupDriverException(
                $"Parent backup {parentBackupId:N} not found in catalog.");

        Guid backupId = Guid.NewGuid();
        string artifactPath = Path.Combine(artifactsDir, backupId.ToString("N"));
        Directory.CreateDirectory(artifactPath);

        Dictionary<int, PartitionBackupRange> parentRanges =
            parentManifest.PartitionRanges.ToDictionary(r => r.PartitionId);

        List<PartitionBackupRange> ranges = [];
        Dictionary<string, string> checksums = [];

        foreach (RaftPartitionRange partition in partitions)
        {
            if (partition.State is RaftPartitionState.Draining or RaftPartitionState.Removed)
                continue;

            int partitionId = partition.PartitionId;
            parentRanges.TryGetValue(partitionId, out PartitionBackupRange? pr);
            long floor = wal.GetLastCheckpoint(partitionId);

            long fromIndex;
            if (pr is not null)
            {
                fromIndex = pr.ToIndex + 1;
                // Parent's coverage ends before the compaction floor: entries in [parent.ToIndex+1, floor)
                // may already be gone. A new full is required to recover this partition.
                if (floor > 0 && fromIndex < floor)
                    throw new BackupDriverException(
                        $"Partition {partitionId}: incremental would start at WAL index {fromIndex} " +
                        $"but the compaction floor is {floor}; a new full backup is required.");
            }
            else
            {
                // Partition first appears after the parent snapshot. Start from the floor so we
                // don't request entries that compaction has already removed.
                fromIndex = floor > 0 ? floor : 1;
            }

            (List<WalSegmentEntry> segment, long toIndex, HLCTimestamp toHlc, HLCTimestamp fromHlc) =
                ReadSegment(wal, partitionId, fromIndex);

            if (toIndex == 0)
                continue;

            string walFile = Path.Combine(artifactPath, $"partition_{partitionId}.wal");
            WriteSegmentFile(walFile, segment);
            checksums[$"partition_{partitionId}.wal"] = ComputeSha256(walFile);

            ranges.Add(PartitionBackupRange.Create(partitionId, fromIndex, fromHlc, toIndex, toHlc));
        }

        BackupManifest manifest = BackupManifest.CreateIncremental(parentBackupId, ranges);
        manifest.BackupId = backupId;
        manifest.Checksums = checksums;

        catalog.Put(manifest);
        return manifest;
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────

    private const int PageSize = 256;

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    /// <summary>
    /// Scans backward through the WAL in page-sized windows to find the last committed entry.
    /// Pages until a committed entry is found or the start of the log is reached.
    /// Returns (0, default) when the partition has no committed entries at all.
    /// </summary>
    private static (long id, HLCTimestamp hlc) FindLastCommitted(IWAL wal, int partitionId)
    {
        long maxLog = wal.GetMaxLog(partitionId);
        if (maxLog <= 0)
            return (0, default);

        long ceiling = maxLog;
        while (ceiling > 0)
        {
            long start = Math.Max(1, ceiling - PageSize + 1);
            List<RaftLog> batch = wal.ReadLogsRange(partitionId, start, PageSize);
            if (batch.Count == 0)
                break;

            for (int i = batch.Count - 1; i >= 0; i--)
            {
                if (batch[i].Type is RaftLogType.Committed or RaftLogType.CommittedCheckpoint)
                    return (batch[i].Id, batch[i].Time);
            }

            ceiling = start - 1;
        }

        return (0, default);
    }

    /// <summary>
    /// Pages through the WAL from <paramref name="fromIndex"/> forward, collecting committed
    /// entries. Returns the segment entries, the final log id/hlc, and the HLC of the first entry.
    /// </summary>
    private static (List<WalSegmentEntry> entries, long toId, HLCTimestamp toHlc, HLCTimestamp fromHlc)
        ReadSegment(IWAL wal, int partitionId, long fromIndex)
    {
        List<WalSegmentEntry> entries = [];
        long toId = 0;
        HLCTimestamp toHlc = default;
        HLCTimestamp fromHlc = default;
        bool first = true;
        long cursor = fromIndex;

        while (true)
        {
            List<RaftLog> batch = wal.ReadLogsRange(partitionId, cursor, PageSize);
            if (batch.Count == 0)
                break;

            foreach (RaftLog log in batch)
            {
                if (log.Type is not (RaftLogType.Committed or RaftLogType.CommittedCheckpoint))
                    continue;

                if (first)
                {
                    fromHlc = log.Time;
                    first = false;
                }

                entries.Add(WalSegmentEntry.From(log));
                toId = log.Id;
                toHlc = log.Time;
            }

            long lastInBatch = batch[^1].Id;
            if (batch.Count < PageSize)
                break;

            cursor = lastInBatch + 1;
        }

        return (entries, toId, toHlc, fromHlc);
    }

    private static void WriteSegmentFile(string path, List<WalSegmentEntry> entries)
    {
        string tmp = path + ".tmp_" + Guid.NewGuid().ToString("N")[..8];
        File.WriteAllText(tmp, JsonSerializer.Serialize(entries, JsonOptions));
        File.Move(tmp, path, overwrite: true);
    }

    private static string ComputeSha256(string filePath)
    {
        using FileStream stream = File.OpenRead(filePath);
        byte[] hash = SHA256.HashData(stream);
        return Convert.ToHexString(hash).ToLowerInvariant();
    }
}

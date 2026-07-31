
using System.Text.Json;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
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
    private readonly Func<int, HLCTimestamp>? _appliedHlcProbe;

    internal const int DefaultApplyBarrierTimeoutMs = 30_000;

    /// <summary>
    /// Acquires an MVCC snapshot-history hold pinning revision history at the given cut, returning a
    /// hold id, or <c>null</c> when protection cannot be guaranteed (the effective snapshot floor
    /// already passed the cut, or the hold could not be acquired) — in which case the full backup
    /// fails closed rather than producing a checkpoint whose history may have been pruned.
    /// </summary>
    internal delegate Task<string?> AcquireSnapshotHoldDelegate(HLCTimestamp cut, CancellationToken ct);

    /// <summary>
    /// Renews the lease on a snapshot-history hold, returning <c>true</c> when the hold is still
    /// live and its lease was extended, <c>false</c> when it could not be renewed (expired, lost to
    /// a leadership change, or the mutation could not be committed). A <c>false</c> result — or a
    /// thrown exception — is treated as renewal loss and fails the backup closed.
    /// </summary>
    internal delegate Task<bool> RenewSnapshotHoldDelegate(string holdId, CancellationToken ct);

    /// <summary>Releases a snapshot-history hold acquired by <see cref="AcquireSnapshotHoldDelegate"/>.</summary>
    internal delegate Task ReleaseSnapshotHoldDelegate(string holdId, CancellationToken ct);

    private readonly AcquireSnapshotHoldDelegate? _acquireSnapshotHold;
    private readonly RenewSnapshotHoldDelegate? _renewSnapshotHold;
    private readonly ReleaseSnapshotHoldDelegate? _releaseSnapshotHold;
    private readonly int _snapshotHoldLeaseMs;

    public BackupDriver(IRaft raft, IPersistenceBackend persistenceBackend,
        Func<Task>? flushBeforeCheckpoint = null,
        AcquireSnapshotHoldDelegate? acquireSnapshotHold = null,
        ReleaseSnapshotHoldDelegate? releaseSnapshotHold = null,
        RenewSnapshotHoldDelegate? renewSnapshotHold = null,
        int snapshotHoldLeaseMs = DefaultSnapshotHoldLeaseMs,
        Func<int, HLCTimestamp>? appliedHlcProbe = null)
    {
        _raft = raft;
        _persistenceBackend = persistenceBackend;
        _flushBeforeCheckpoint = flushBeforeCheckpoint;
        _appliedHlcProbe = appliedHlcProbe;
        _acquireSnapshotHold = acquireSnapshotHold;
        _releaseSnapshotHold = releaseSnapshotHold;
        _renewSnapshotHold = renewSnapshotHold;
        _snapshotHoldLeaseMs = snapshotHoldLeaseMs;
    }

    /// <summary>
    /// Default snapshot-history hold lease. The lease is renewed at roughly a third of this interval
    /// (see <see cref="RenewSnapshotHoldLoop"/>), so a hold survives a checkpoint/hash/verify/publish
    /// cycle far longer than the lease as long as renewal keeps succeeding.
    /// </summary>
    internal const int DefaultSnapshotHoldLeaseMs = 600_000;

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
            artifactsDir, catalog, _flushBeforeCheckpoint, snapshotT, ct,
            _acquireSnapshotHold, _releaseSnapshotHold, _renewSnapshotHold, _snapshotHoldLeaseMs, _appliedHlcProbe);

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
        BackupCatalog catalog, HLCTimestamp? snapshotT = null, CancellationToken ct = default,
        Func<int, long, IDisposable>? acquireRetentionHold = null) =>
        RunIncremental(_raft.WalAdapter, _raft.GetPartitionMap(), parentBackupId, artifactsDir, catalog, snapshotT, ct, acquireRetentionHold);

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
        CancellationToken ct = default,
        AcquireSnapshotHoldDelegate? acquireSnapshotHold = null,
        ReleaseSnapshotHoldDelegate? releaseSnapshotHold = null,
        RenewSnapshotHoldDelegate? renewSnapshotHold = null,
        int snapshotHoldLeaseMs = DefaultSnapshotHoldLeaseMs,
        Func<int, HLCTimestamp>? appliedHlcProbe = null,
        int applyBarrierTimeoutMs = DefaultApplyBarrierTimeoutMs)
    {
        ct.ThrowIfCancellationRequested();

        Guid backupId = Guid.NewGuid();
        string artifactPath = Path.Combine(artifactsDir, backupId.ToString("N"));
        Directory.CreateDirectory(artifactPath);

        string? snapshotHoldId = null;

        // Renewal machinery: while a hold is held, a background loop keeps its lease alive across the
        // whole checkpoint/hash/verify/publish window. If renewal is ever lost, renewalLost is
        // tripped, which cancels workCt so the in-flight work aborts and nothing is published.
        CancellationTokenSource? renewalLost = null;
        CancellationTokenSource? renewalStop = null;
        CancellationTokenSource? linkedWork = null;
        Task renewalLoop = Task.CompletedTask;
        CancellationToken workCt = ct;

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

            // Applied-index barrier: before flushing, wait until every captured partition's committed
            // key-value writes (by their commit HLC) have been applied and enqueued for persistence, so
            // the flush cannot miss a committed write that is still mid-apply. Fail closed on timeout —
            // never publish a checkpoint that may lack committed data.
            if (appliedHlcProbe is not null)
                await WaitForAppliedBarrierAsync(wal, ranges, appliedHlcProbe, applyBarrierTimeoutMs, ct);

            if (flushBeforeCheckpoint is not null)
                await flushBeforeCheckpoint();

            ct.ThrowIfCancellationRequested();

            // Cut the base image at a single HLC: the coordinated snapshot T when supplied, else the
            // maximum committed HLC across all captured partitions (by HLC order — partition log
            // indexes are partition-local and cannot be compared across partitions). No committed
            // state newer than the cut belongs in the image — otherwise replay (forward-only, stops
            // at the restore target) could never remove a post-cut value.
            HLCTimestamp cut = snapshotT ?? maxCommittedHlc;

            // Refuse a cut whose per-key boundary history retention may already have pruned before this
            // backup began. A live snapshot hold only fences pruning from here on; it cannot restore a
            // boundary already gone. The backend's durable pruned-history floor records the highest cut
            // below which a boundary may be missing, so a cut below it cannot be proven exact.
            HLCTimestamp prunedHistoryFloor = persistenceBackend.GetPrunedHistoryFloor();
            if (cut != HLCTimestamp.Zero && prunedHistoryFloor != HLCTimestamp.Zero
                && cut.CompareTo(prunedHistoryFloor) < 0)
                throw new BackupDriverException(
                    $"The backup cut {cut} is below the pruned-history floor {prunedHistoryFloor}; revision " +
                    "history needed to reconstruct the cut exactly has already been pruned, so the backup " +
                    "was not taken.")
                {
                    ExactCheckpointUnavailable = true
                };

            // Pin MVCC revision history at the cut for the duration of the checkpoint + verification so
            // concurrent pruning cannot remove the as-of revisions the trim depends on. Fails closed
            // when the effective snapshot floor already passed the cut (history may already be gone).
            if (acquireSnapshotHold is not null && cut != HLCTimestamp.Zero)
            {
                snapshotHoldId = await acquireSnapshotHold(cut, ct);
                if (snapshotHoldId is null)
                    throw new BackupDriverException(
                        $"Could not pin MVCC history at the backup cut {cut} (the retention floor has " +
                        "already passed it, or the hold could not be acquired); an exact backup cannot be taken.")
                    {
                        ExactCheckpointUnavailable = true
                    };
            }

            // Keep the hold's lease renewed for the entire remaining lifetime (checkpoint, hashing,
            // verification, publish). A checkpoint copy + trim + VACUUM/compaction + hash + verify can
            // outlast a single lease, and if it expired mid-run pruning could reclaim the as-of history
            // the checkpoint depends on. renewalLost cancels workCt the instant a renew fails, so the
            // work below aborts before publishing anything.
            if (snapshotHoldId is not null && renewSnapshotHold is not null)
            {
                renewalLost = new CancellationTokenSource();
                renewalStop = new CancellationTokenSource();
                linkedWork = CancellationTokenSource.CreateLinkedTokenSource(ct, renewalLost.Token);
                workCt = linkedWork.Token;
                renewalLoop = RenewSnapshotHoldLoop(
                    renewSnapshotHold, snapshotHoldId, snapshotHoldLeaseMs, renewalLost, renewalStop.Token);
            }

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
            persistenceBackend.CreateCheckpointAsOf(checkpointPath, maxAppliedIndex, cut, workCt);

            // Hash every file the checkpoint produced (data files AND the sidecar), not just the
            // sidecar, so a truncated or altered checkpoint is caught before replay.
            (Dictionary<string, string> checksums, Dictionary<string, long> sizes) =
                BackupArtifactVerifier.HashDirectory(checkpointPath, "checkpoint/", workCt);

            BackupManifest manifest = BackupManifest.CreateFull(ranges);
            manifest.BackupId = backupId;
            manifest.Checksums = checksums;
            manifest.Sizes = sizes;
            manifest.SetBaseCut(cut);
            if (snapshotT.HasValue)
                manifest.SetClusterSnapshotTime(snapshotT.Value);

            // Fail closed: never publish a manifest whose artifacts don't verify.
            BackupArtifactVerifier.Verify(manifest, artifactsDir, workCt);

            // Final gate before publishing: if renewal was lost at any point the hold may have
            // expired and pruning may have reclaimed the as-of history, so the checkpoint we just
            // hashed can no longer be trusted as an exact base image. Never publish it.
            if (renewalLost is not null && renewalLost.IsCancellationRequested)
                throw new BackupDriverException(
                    "The snapshot-history hold could not be renewed during the backup; the base image " +
                    "may have been pruned, so the backup was not published.")
                {
                    ExactCheckpointUnavailable = true
                };

            catalog.Put(manifest);
            return manifest;
        }
        catch (OperationCanceledException) when (
            renewalLost is not null && renewalLost.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            // The work was aborted specifically by renewal loss (not by the caller's cancellation).
            // Surface it as a fail-closed backup error rather than a bare cancellation.
            TryDeleteDirectory(artifactPath);
            throw new BackupDriverException(
                "The snapshot-history hold could not be renewed during the backup; the base image " +
                "may have been pruned, so the backup was not published.")
            {
                ExactCheckpointUnavailable = true
            };
        }
        catch
        {
            TryDeleteDirectory(artifactPath);
            throw;
        }
        finally
        {
            // Stop the renewal loop and wait for it to unwind before touching its cancellation
            // sources, so nothing races on a disposed CTS.
            renewalStop?.Cancel();
            try { await renewalLoop.ConfigureAwait(false); } catch { /* loop already unwinding */ }
            linkedWork?.Dispose();
            renewalLost?.Dispose();
            renewalStop?.Dispose();

            // Release the snapshot-history hold after the manifest is published (or the attempt
            // failed) so pruning is fenced across the whole checkpoint + verification window. This is
            // best-effort cleanup: once catalog.Put has committed, a release failure must NOT turn a
            // published, successful backup into a reported failure (that would provoke a duplicate
            // retry). The hold's lease expires on its own if the release never lands.
            if (snapshotHoldId is not null && releaseSnapshotHold is not null)
            {
                try { await releaseSnapshotHold(snapshotHoldId, CancellationToken.None).ConfigureAwait(false); }
                catch { /* published state stands; the lease reclaims the hold if release is lost */ }
            }
        }
    }

    /// <summary>
    /// Keeps a snapshot-history hold's lease alive until <paramref name="stopCt"/> is signalled.
    /// Renews at roughly a third of the lease so a renewal has two more chances before the lease
    /// would lapse. The first renew that returns <c>false</c> or throws is treated as renewal loss:
    /// <paramref name="renewalLost"/> is cancelled (which aborts the backup's work) and the loop
    /// exits. Never throws to its awaiter.
    /// </summary>
    internal static async Task RenewSnapshotHoldLoop(
        RenewSnapshotHoldDelegate renew,
        string holdId,
        int leaseMs,
        CancellationTokenSource renewalLost,
        CancellationToken stopCt)
    {
        // Renew well before expiry. Floor at 1 ms so a short test lease still yields a positive delay.
        int intervalMs = Math.Max(1, leaseMs / 3);

        try
        {
            while (!stopCt.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(intervalMs, stopCt).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return; // stopped normally (backup finished or aborted)
                }

                bool renewed;
                try
                {
                    renewed = await renew(holdId, stopCt).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return; // stopped normally during the renew call
                }
                catch
                {
                    renewed = false; // transport or leader-change failure → renewal lost
                }

                if (!renewed)
                {
                    if (!renewalLost.IsCancellationRequested)
                        renewalLost.Cancel();
                    return;
                }
            }
        }
        catch
        {
            // Any unexpected loop failure fails closed: treat it as renewal loss.
            if (!renewalLost.IsCancellationRequested)
            {
                try { renewalLost.Cancel(); } catch { /* already disposed/cancelled */ }
            }
        }
    }

    /// <summary>
    /// Waits until each captured partition's committed key-value writes have been applied and enqueued
    /// for persistence, observed via <paramref name="appliedHlcProbe"/> (the background writer's
    /// max-enqueued transaction commit HLC). Throws a fail-closed <see cref="BackupDriverException"/> on
    /// timeout so no checkpoint is published while a committed write is still mid-apply. Uses a monotonic
    /// clock for the deadline (never wall time).
    /// <para>
    /// The per-partition target is the max <b>commit HLC</b> (payload <c>LastModified</c>) among the
    /// captured committed writes — the same clock the probe reports. It is deliberately NOT
    /// <see cref="PartitionBackupRange.ToHlc"/>: that is a per-partition Raft WAL entry Time, stamped
    /// from the partition's local clock and always at or after the shared commit HLC, so the commit-HLC
    /// probe could never reach it and the backup would hang until timeout.
    /// </para>
    /// </summary>
    private static async Task WaitForAppliedBarrierAsync(
        IWAL wal, List<PartitionBackupRange> ranges, Func<int, HLCTimestamp> appliedHlcProbe, int timeoutMs, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        foreach (PartitionBackupRange range in ranges)
        {
            HLCTimestamp target = MaxCommittedKeyValueCommitHlc(wal, range.PartitionId, range.ToIndex, ct);
            if (target == HLCTimestamp.Zero)
                continue;

            while (appliedHlcProbe(range.PartitionId).CompareTo(target) < 0)
            {
                ct.ThrowIfCancellationRequested();
                if (Environment.TickCount64 > deadline)
                    throw new BackupDriverException(
                        $"Timed out waiting for partition {range.PartitionId} to apply and enqueue committed " +
                        $"writes up to commit HLC {target}; the checkpoint would be missing committed data, so " +
                        "the backup was not taken.")
                    {
                        ExactCheckpointUnavailable = true
                    };
                await Task.Delay(10, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// The maximum transaction commit HLC (payload <see cref="KeyValueMessage"/> <c>LastModified</c>)
    /// among committed key-value entries at or below <paramref name="toIndex"/> on a partition — the
    /// axis the applied-index barrier compares against, because the background writer's max-enqueued
    /// HLC is a commit HLC, not the Raft WAL entry Time. Scans backward and stops as soon as an entry's
    /// WAL Time (monotonic with index and always at or after that entry's commit HLC) drops to or below
    /// the running maximum, since no earlier entry can then carry a larger commit HLC; in normal
    /// operation this touches only the newest, still-settling entries.
    /// Returns <see cref="HLCTimestamp.Zero"/> when no committed key-value entry exists at or below the index.
    /// </summary>
    internal static HLCTimestamp MaxCommittedKeyValueCommitHlc(
        IWAL wal, int partitionId, long toIndex, CancellationToken ct = default)
    {
        if (toIndex <= 0)
            return HLCTimestamp.Zero;

        HLCTimestamp max = HLCTimestamp.Zero;
        long ceiling = toIndex;
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
                if (log.Id > toIndex)
                    continue;

                // Early stop: the WAL entry Time is monotonic with index and never below the entry's
                // commit HLC, so once it reaches or falls under the running max, no earlier entry can raise it.
                if (max != HLCTimestamp.Zero && log.Time.CompareTo(max) <= 0)
                    return max;

                if (log.Type is not (RaftLogType.Committed or RaftLogType.CommittedCheckpoint))
                    continue;
                if (log.LogType != ReplicationTypes.KeyValues || log.LogData is null || log.LogData.Length == 0)
                    continue;

                KeyValueMessage msg = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);
                HLCTimestamp commitHlc = new(msg.LastModifiedNode, msg.LastModifiedPhysical, msg.LastModifiedCounter);
                if (commitHlc.CompareTo(max) > 0)
                    max = commitHlc;
            }

            ceiling = start - 1;
        }

        return max;
    }

    internal static BackupManifest RunIncremental(
        IWAL wal,
        IReadOnlyList<RaftPartitionRange> partitions,
        Guid parentBackupId,
        string artifactsDir,
        BackupCatalog catalog,
        HLCTimestamp? snapshotT = null,
        CancellationToken ct = default,
        Func<int, long, IDisposable>? acquireRetentionHold = null)
    {
        ct.ThrowIfCancellationRequested();

        BackupManifest? parentManifest = catalog.Get(parentBackupId);
        if (parentManifest is null)
            throw new BackupDriverException(
                $"Parent backup {parentBackupId:N} not found in catalog.") { ParentMissing = true };

        Guid backupId = Guid.NewGuid();
        string artifactPath = Path.Combine(artifactsDir, backupId.ToString("N"));
        Directory.CreateDirectory(artifactPath);

        // Retention holds keep the WAL prefix each partition starts at from being compacted while we
        // page the log and until the manifest is published. Released in the finally.
        List<IDisposable> holds = [];

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

                // Hold the retention floor at fromIndex BEFORE paging the WAL so a concurrent horizon
                // advance cannot compact this prefix mid-read (Kommander composes this hold with the
                // periodic floor via minimum). Then re-read the floor: if it already passed fromIndex
                // in the window before the hold took effect, fail closed (NeedsFull).
                if (acquireRetentionHold is not null)
                {
                    holds.Add(acquireRetentionHold(partitionId, fromIndex));
                    long floorAfterHold = wal.GetLastCheckpoint(partitionId);
                    if (pr is not null && floorAfterHold > 0 && fromIndex < floorAfterHold)
                        throw new BackupDriverException(
                            $"Partition {partitionId}: WAL prefix from {fromIndex} was compacted before the " +
                            $"retention hold took effect (floor {floorAfterHold}); a new full backup is required.")
                        {
                            NeedsFullBackup = true
                        };
                }

                // Capture the full contiguous committed range; the coordinated cut (snapshotT), when
                // present, is recorded in the manifest and applied at restore on the commit HLC, not by
                // truncating the segment here (which would tear straddling multi-partition transactions).
                (List<WalSegmentEntry> segment, long toIndex, HLCTimestamp toHlc, long toTerm, HLCTimestamp fromHlc) =
                    ReadSegment(wal, partitionId, fromIndex, ct);

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
        finally
        {
            // Release retention holds only after the manifest is published (or the attempt failed),
            // so the protected prefix is never compacted between capture and publication.
            foreach (IDisposable hold in holds)
                hold.Dispose();
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
    /// Pages through the WAL from <paramref name="fromIndex"/> forward, collecting every committed
    /// entry as a contiguous index range up to the current WAL end.
    /// <para>
    /// The segment is deliberately NOT capped at a coordinated snapshot timestamp. Capping by the
    /// per-partition WAL entry <c>Time</c> would drop a committed transaction's entry on the partition
    /// whose local append clock ran ahead of the cut while a sibling partition kept its half — a torn
    /// backup. Capping by the shared commit HLC instead would punch holes in the index range (an
    /// after-cut entry sitting between two before-cut entries), and since the next incremental resumes
    /// at <c>ToIndex + 1</c> a hole would be lost forever. So the whole contiguous range is captured and
    /// the coordinated cut is applied at restore time on the commit HLC (see <see cref="RestoreEngine"/>),
    /// which includes or excludes each transaction as a whole.
    /// </para>
    /// Returns the segment entries, the final log id/hlc/term, and the HLC of the first entry.
    /// </summary>
    private static (List<WalSegmentEntry> entries, long toId, HLCTimestamp toHlc, long toTerm, HLCTimestamp fromHlc)
        ReadSegment(IWAL wal, int partitionId, long fromIndex, CancellationToken ct = default)
    {
        List<WalSegmentEntry> entries = [];
        long toId = 0;
        HLCTimestamp toHlc = default;
        long toTerm = 0;
        HLCTimestamp fromHlc = default;
        bool first = true;
        long cursor = fromIndex;

        while (true)
        {
            ct.ThrowIfCancellationRequested();
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
                toTerm = log.Term;
            }

            long lastInBatch = batch[^1].Id;
            if (batch.Count < PageSize)
                break;

            cursor = lastInBatch + 1;
        }

        return (entries, toId, toHlc, toTerm, fromHlc);
    }

    private static void WriteSegmentFile(string path, List<WalSegmentEntry> entries) =>
        WalSegmentEntry.WriteSegment(path, entries);
}

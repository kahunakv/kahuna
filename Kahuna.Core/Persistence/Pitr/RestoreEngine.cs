
using System.Text.Json;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kommander.Time;

namespace Kahuna.Server.Persistence.Pitr;

/// <summary>
/// Replays a chain of backup manifests into a persistence backend up to a chosen HLC
/// stop-timestamp, implementing the Phase 1 offline PITR restore.
///
/// <para><b>Preconditions for the caller:</b>
/// <list type="bullet">
///   <item>Resolve and validate the chain with <see cref="BackupCatalog.ResolveAndValidate"/>
///         before calling <see cref="Restore"/>.</item>
///   <item>Open the Full backup's checkpoint into <paramref name="backend"/> before calling
///         <see cref="Restore"/>. For <see cref="MemoryPersistenceBackend"/> use
///         <c>MemoryPersistenceBackend.OpenCheckpoint(checkpointPath)</c>.</item>
/// </list>
/// </para>
///
/// <para><b>Intent resolution as-of T:</b> WAL segment files produced by
/// <see cref="BackupDriver"/> contain only committed entries (all of type
/// <c>RaftLogType.Committed</c>). Any 2PC transaction that prepared before T but did not
/// commit by T is therefore absent from the segments — the key remains at its last committed
/// value before that transaction touched it. No additional intent-resolution pass is needed.
/// </para>
///
/// <para><b>Idempotency:</b> <c>StoreKeyValues</c> on every backend is an upsert keyed by
/// <c>(key, revision)</c>; replaying the same segment twice writes the same rows a second
/// time and produces the same final state. Partial restores that were interrupted mid-segment
/// are therefore safe to re-run from the start of that segment.</para>
/// </summary>
internal static class RestoreEngine
{
    private const int ApplyBatchSize = 256;

    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    /// <summary>
    /// Replays all incremental WAL segments in <paramref name="chain"/> into
    /// <paramref name="backend"/>, applying entries whose <c>Time ≤ targetTime</c> and
    /// stopping at the first entry whose <c>Time > targetTime</c>.
    /// </summary>
    /// <param name="chain">
    /// The resolved, validated backup chain (Full first, most-recent incremental last).
    /// Produced by <see cref="BackupCatalog.ResolveAndValidate"/>.
    /// </param>
    /// <param name="artifactsDir">
    /// Root directory where per-backup artifact subdirectories live
    /// (<c>{artifactsDir}/{backupId:N}/</c>).
    /// </param>
    /// <param name="targetTime">
    /// HLC stop-timestamp T. All entries with <c>Time ≤ T</c> are applied; the first entry
    /// with <c>Time > T</c> halts replay for that partition.
    /// </param>
    /// <param name="backend">
    /// Persistence backend pre-loaded with the Full backup's checkpoint state.
    /// </param>
    /// <param name="pitrWindow">
    /// When provided together with <paramref name="nowUtc"/>, <paramref name="targetTime"/>
    /// is validated against <c>[now − pitrWindow, now]</c>.
    /// </param>
    /// <param name="nowUtc">Wall-clock reference for window validation.</param>
    /// <exception cref="BackupDriverException">
    /// Thrown when <paramref name="targetTime"/> is outside the recoverable window.
    /// </exception>
    internal static RestoreResult Restore(
        IReadOnlyList<BackupManifest> chain,
        string artifactsDir,
        HLCTimestamp targetTime,
        IPersistenceBackend backend,
        TimeSpan? pitrWindow = null,
        DateTime? nowUtc = null)
    {
        ValidateWindow(targetTime, pitrWindow, nowUtc);

        long totalApplied = 0;
        HLCTimestamp lastAppliedTime = default;
        long lastAppliedIndex = 0;
        HashSet<int> activePartitions = [];

        // Skip the first (Full) entry — its state is already in the backend via the checkpoint.
        // Replay incrementals in chronological order.
        foreach (BackupManifest manifest in chain.Skip(1))
        {
            string artifactPath = Path.Combine(artifactsDir, manifest.BackupId.ToString("N"));

            foreach (PartitionBackupRange range in manifest.PartitionRanges)
            {
                int partitionId = range.PartitionId;
                string walFile = Path.Combine(artifactPath, $"partition_{partitionId}.wal");

                if (!File.Exists(walFile))
                    continue;

                List<WalSegmentEntry>? entries = JsonSerializer.Deserialize<List<WalSegmentEntry>>(
                    File.ReadAllText(walFile), JsonOptions);

                if (entries is null || entries.Count == 0)
                    continue;

                List<PersistenceRequestItem> batch = new(ApplyBatchSize);

                foreach (WalSegmentEntry entry in entries)
                {
                    // HLC stop-predicate: entries are in ascending index (and therefore HLC) order.
                    if (entry.Time.CompareTo(targetTime) > 0)
                        break;

                    PersistenceRequestItem? item = ToRequestItem(entry);
                    if (item is null)
                        continue;

                    batch.Add(item.Value);
                    if (entry.Time.CompareTo(lastAppliedTime) >= 0)
                    {
                        lastAppliedTime = entry.Time;
                        lastAppliedIndex = entry.Id;
                    }
                    totalApplied++;
                    activePartitions.Add(partitionId);

                    if (batch.Count >= ApplyBatchSize)
                    {
                        backend.StoreKeyValues(batch);
                        batch.Clear();
                    }
                }

                if (batch.Count > 0)
                    backend.StoreKeyValues(batch);
            }
        }

        return new RestoreResult(activePartitions.Count, totalApplied, lastAppliedTime, lastAppliedIndex);
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────

    private static void ValidateWindow(HLCTimestamp targetTime, TimeSpan? pitrWindow, DateTime? nowUtc)
    {
        if (pitrWindow is null || nowUtc is null)
            return;

        long nowMs = (long)((nowUtc.Value - DateTime.UnixEpoch).TotalMilliseconds);
        long earliestMs = (long)((nowUtc.Value - pitrWindow.Value - DateTime.UnixEpoch).TotalMilliseconds);

        HLCTimestamp earliest = new(0, earliestMs, 0);
        HLCTimestamp now = new(0, nowMs, 0);

        if (targetTime.CompareTo(earliest) < 0 || targetTime.CompareTo(now) > 0)
            throw new BackupDriverException(
                $"Target time {targetTime} is outside the recoverable window " +
                $"[{earliest}, {now}] (window = {pitrWindow.Value.TotalMinutes:F0} min).");
    }

    /// <summary>
    /// Converts a WAL segment entry to a <see cref="PersistenceRequestItem"/> by decoding
    /// the protobuf <see cref="KeyValueMessage"/> payload. Returns <c>null</c> for entries
    /// that are not key-value mutations (e.g. range-map or lock entries).
    /// </summary>
    private static PersistenceRequestItem? ToRequestItem(WalSegmentEntry entry)
    {
        if (entry.LogType != ReplicationTypes.KeyValues)
            return null;

        if (entry.LogData is null || entry.LogData.Length == 0)
            return null;

        KeyValueMessage msg = ReplicationSerializer.UnserializeKeyValueMessage(entry.LogData);

        (KeyValueState state, byte[]? value) = KeyValueMessageDecoder.Decode(msg);

        if (state == KeyValueState.Undefined)
            return null;

        return new PersistenceRequestItem(
            msg.Key,
            value,
            msg.Revision,
            msg.ExpireNode,
            msg.ExpirePhysical,
            msg.ExpireCounter,
            msg.LastUsedNode,
            msg.LastUsedPhysical,
            msg.LastUsedCounter,
            msg.LastModifiedNode,
            msg.LastModifiedPhysical,
            msg.LastModifiedCounter,
            (int)state,
            msg.NoRevision);
    }
}

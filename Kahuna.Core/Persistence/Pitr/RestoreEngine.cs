
using System.Buffers;
using System.Security.Cryptography;
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
/// stop-timestamp — the offline point-in-time restore path.
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
    /// <paramref name="backend"/>, applying every key-value entry whose transaction commit HLC
    /// (the payload <c>LastModified</c>, shared across all participants of a transaction) is
    /// <c>≤ targetTime</c> and skipping those after it. The cut is on the commit HLC, not the
    /// per-partition WAL entry Time, so a multi-partition transaction is applied whole or not at
    /// all and can never be torn across partitions.
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
    /// HLC stop-timestamp T. Every key-value entry whose commit HLC (payload <c>LastModified</c>)
    /// is <c>≤ T</c> is applied; entries with a commit HLC <c>&gt; T</c> are skipped.
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
    internal static async Task<RestoreResult> RestoreAsync(
        IReadOnlyList<BackupManifest> chain,
        string artifactsDir,
        HLCTimestamp targetTime,
        IPersistenceBackend backend,
        TimeSpan? pitrWindow = null,
        DateTime? nowUtc = null,
        bool alreadyVerified = false,
        CancellationToken ct = default)
    {
        ValidateWindow(targetTime, pitrWindow, nowUtc);

        // Verify the Full's artifacts up front (fail closed before mutating anything). Incrementals
        // are verified at point of use — immediately before their segments are read — to minimize the
        // verify-then-use window. Callers that verify externally (e.g. after staging the checkpoint)
        // pass alreadyVerified to skip re-hashing here.
        if (!alreadyVerified && chain.Count > 0)
            BackupArtifactVerifier.Verify(chain[0], artifactsDir, ct);

        long totalApplied = 0;
        HLCTimestamp lastAppliedTime = default;
        long lastAppliedIndex = 0;
        HashSet<int> activePartitions = [];

        // A private, process-owned staging directory that no other actor can write to. Each segment is
        // copied here and re-hashed at point of use, so the exact bytes replayed into the backend are the
        // bytes that just passed digest verification — a source artifact swapped after the up-front check
        // can never reach the backend. Removed on every exit path.
        string stagingRoot = Directory.CreateTempSubdirectory("kahuna_restore_").FullName;
        try
        {
            // Skip the first (Full) entry — its state is already in the backend via the checkpoint.
            // Replay incrementals in chronological order.
            foreach (BackupManifest manifest in chain.Skip(1))
            {
                ct.ThrowIfCancellationRequested();

                // Verify this incremental's artifacts immediately before consuming them.
                if (!alreadyVerified)
                    BackupArtifactVerifier.Verify(manifest, artifactsDir, ct);

                string artifactPath = Path.Combine(artifactsDir, manifest.BackupId.ToString("N"));

                foreach (PartitionBackupRange range in manifest.PartitionRanges)
                {
                    int partitionId = range.PartitionId;
                    string relKey = $"partition_{partitionId}.wal";
                    string walFile = Path.Combine(artifactPath, relKey);

                    // Every range in an incremental manifest was written with a segment file. A missing
                    // file is corruption, not an empty partition — fail closed rather than silently
                    // dropping the partition's changes.
                    if (!File.Exists(walFile))
                        throw new BackupArtifactException(
                            $"Backup {manifest.BackupId:N}: declared segment for partition {partitionId} " +
                            $"is missing ({walFile}).");

                    // Copy the source into private staging while hashing, and verify size + digest against
                    // the manifest BEFORE replaying. From here on we read only the staged copy, closing the
                    // verify/use race between digest verification and replay.
                    string stagedFile = StageAndVerifySegment(walFile, relKey, manifest, stagingRoot, ct);

                    List<PersistenceRequestItem> batch = new(ApplyBatchSize);

                    // Stream the staged segment one record at a time (async I/O) so peak memory is a single
                    // entry plus one write batch, not the whole (potentially multi-GB) segment, and disk
                    // reads do not block the calling thread.
                    await foreach (WalSegmentEntry entry in WalSegmentEntry.ReadSegmentAsync(stagedFile, ct).ConfigureAwait(false))
                    {
                        ct.ThrowIfCancellationRequested();

                        (PersistenceRequestItem item, HLCTimestamp commitHlc)? decoded = ToRequestItem(entry);

                        // Cut on the transaction COMMIT HLC carried in the payload, not the Raft WAL entry
                        // Time. Every participant of a multi-partition transaction shares one commit HLC, but
                        // each partition stamps its WAL entry with its own local clock — so a WAL-Time cut can
                        // include one half of a committed transaction and exclude the other (a torn restore).
                        // Comparing the shared commit HLC includes or excludes a transaction as a whole. A
                        // non-key-value entry carries no commit HLC; fall back to its WAL Time.
                        HLCTimestamp cutHlc = decoded?.commitHlc ?? entry.Time;

                        // The commit HLC is NOT monotonic with WAL index across keys (different coordinators,
                        // different local append clocks), so a single past-target entry does not mean the rest
                        // of the segment is also past target — skip it and keep scanning rather than break.
                        // Per key the commit HLC IS monotonic with revision, and StoreKeyValues is an idempotent
                        // upsert keyed by (key, revision), so applying every at-or-before-target revision and
                        // dropping every after-target one reconstructs each key's exact as-of-target state.
                        if (cutHlc.CompareTo(targetTime) > 0)
                            continue;

                        if (decoded is null)
                            continue;

                        batch.Add(decoded.Value.item);
                        if (cutHlc.CompareTo(lastAppliedTime) >= 0)
                        {
                            lastAppliedTime = cutHlc;
                            lastAppliedIndex = entry.Id;
                        }
                        totalApplied++;
                        activePartitions.Add(partitionId);

                        if (batch.Count >= ApplyBatchSize)
                        {
                            StoreBatchOrThrow(backend, batch, manifest.BackupId, partitionId);
                            batch.Clear();
                        }
                    }

                    if (batch.Count > 0)
                        StoreBatchOrThrow(backend, batch, manifest.BackupId, partitionId);

                    // The staged copy is consumed; drop it so peak disk use is one segment, not the chain.
                    TryDeleteFile(stagedFile);
                }
            }
        }
        finally
        {
            TryDeleteDirectory(stagingRoot);
        }

        return new RestoreResult(activePartitions.Count, totalApplied, lastAppliedTime, lastAppliedIndex);
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Persists a batch and fails closed if the backend could not store it. Every backend returns
    /// <c>false</c> (not an exception) when a write fails — the SQLite backend, for instance, catches
    /// its own persistence exceptions and returns <c>false</c>. Ignoring that result would let a
    /// restore/bootstrap report success while durable state trails the claimed restored position, so a
    /// failed batch must abort the whole operation. Because <c>StoreKeyValues</c> is an idempotent
    /// upsert keyed by <c>(key, revision)</c>, re-running the restore after a transient failure safely
    /// re-applies already-stored batches.
    /// </summary>
    private static void StoreBatchOrThrow(
        IPersistenceBackend backend, List<PersistenceRequestItem> batch, Guid backupId, int partitionId)
    {
        if (!backend.StoreKeyValues(batch))
            throw new BackupDriverException(
                $"Backup {backupId:N}: persistence backend failed to store a batch of {batch.Count} " +
                $"key-value entries for partition {partitionId}; the restore cannot be completed safely.");
    }

    /// <summary>
    /// Copies a source segment into <paramref name="stagingDir"/> (a private, process-owned directory)
    /// while computing its SHA-256 and byte length, then verifies both against the manifest's recorded
    /// checksum and size <b>before</b> any replay. Returns the staged path, which the caller replays from
    /// instead of the source. Because the staged copy lives in a directory nothing else can write to, the
    /// bytes replayed are provably the bytes that passed verification — closing the window in which a
    /// source artifact could be swapped between digest verification and replay.
    /// </summary>
    private static string StageAndVerifySegment(
        string sourceWalFile, string relKey, BackupManifest manifest, string stagingDir, CancellationToken ct)
    {
        if (!manifest.Checksums.TryGetValue(relKey, out string? expectedDigest))
            throw new BackupArtifactException(
                $"Backup {manifest.BackupId:N}: segment '{relKey}' has no recorded checksum to verify against.");
        if (!manifest.Sizes.TryGetValue(relKey, out long expectedSize))
            throw new BackupArtifactException(
                $"Backup {manifest.BackupId:N}: segment '{relKey}' has no recorded size to verify against.");

        string staged = Path.Combine(stagingDir, $"{manifest.BackupId:N}_{relKey.Replace('/', '_')}");

        long size = 0;
        byte[] hash;

        // Open the source without a share-write lock and copy through a hashing stream. FileMode.CreateNew
        // guarantees the staged path is freshly ours.
        using (FileStream src = new(sourceWalFile, FileMode.Open, FileAccess.Read, FileShare.Read, 65536, useAsync: false))
        using (FileStream dst = new(staged, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536))
        using (SHA256 sha = SHA256.Create())
        using (CryptoStream cs = new(dst, sha, CryptoStreamMode.Write, leaveOpen: true))
        {
            byte[] buffer = ArrayPool<byte>.Shared.Rent(65536);
            try
            {
                int read;
                while ((read = src.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ct.ThrowIfCancellationRequested();
                    cs.Write(buffer, 0, read);
                    size += read;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }

            cs.FlushFinalBlock();
            hash = sha.Hash!;
        }

        if (size != expectedSize)
            throw new BackupArtifactException(
                $"Backup {manifest.BackupId:N}: segment '{relKey}' staged size {size} does not match the " +
                $"recorded size {expectedSize} (the source changed after verification).");

        string actualDigest = Convert.ToHexString(hash).ToLowerInvariant();
        if (!string.Equals(actualDigest, expectedDigest, StringComparison.OrdinalIgnoreCase))
            throw new BackupArtifactException(
                $"Backup {manifest.BackupId:N}: segment '{relKey}' failed digest verification at replay " +
                "(the source bytes changed after the artifact was verified).");

        return staged;
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);
        }
        catch
        {
            // Best-effort; the whole staging directory is removed in the finally block regardless.
        }
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
            // Best-effort cleanup of a transient staging directory under the system temp root.
        }
    }

    private static void ValidateWindow(HLCTimestamp targetTime, TimeSpan? pitrWindow, DateTime? nowUtc)
    {
        if (pitrWindow is null || nowUtc is null)
            return;

        long nowMs = (long)((nowUtc.Value - DateTime.UnixEpoch).TotalMilliseconds);
        long earliestMs = (long)((nowUtc.Value - pitrWindow.Value - DateTime.UnixEpoch).TotalMilliseconds);

        // Bound with the same inclusive-end-of-millisecond convention the Unix-ms target mapping uses
        // (counter = uint.MaxValue): a target at the current millisecond, or at the earliest recoverable
        // millisecond, resolves to the whole millisecond and must not be rejected by a bare (·, ms, 0).
        HLCTimestamp earliest = new(0, earliestMs, 0);
        HLCTimestamp now = new(0, nowMs, uint.MaxValue);

        if (targetTime.CompareTo(earliest) < 0 || targetTime.CompareTo(now) > 0)
            throw new BackupDriverException(
                $"Target time {targetTime} is outside the recoverable window " +
                $"[{earliest}, {now}] (window = {pitrWindow.Value.TotalMinutes:F0} min).");
    }

    /// <summary>
    /// Converts a WAL segment entry to a <see cref="PersistenceRequestItem"/> by decoding
    /// the protobuf <see cref="KeyValueMessage"/> payload, and returns the transaction commit
    /// HLC (<c>LastModified</c>) carried in that payload alongside it. The commit HLC is the
    /// single timestamp the coordinator stamps identically on every participant of a
    /// (possibly multi-partition) transaction, so it — not the per-partition WAL entry Time —
    /// is the correct axis for an as-of cut. Returns <c>null</c> for entries that are not
    /// key-value mutations (e.g. range-map or lock entries).
    /// </summary>
    private static (PersistenceRequestItem item, HLCTimestamp commitHlc)? ToRequestItem(WalSegmentEntry entry)
    {
        if (entry.LogType != ReplicationTypes.KeyValues)
            return null;

        if (entry.LogData is null || entry.LogData.Length == 0)
            return null;

        KeyValueMessage msg = ReplicationSerializer.UnserializeKeyValueMessage(entry.LogData);

        (KeyValueState state, byte[]? value) = KeyValueMessageDecoder.Decode(msg);

        if (state == KeyValueState.Undefined)
            return null;

        PersistenceRequestItem item = new(
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

        HLCTimestamp commitHlc = new(msg.LastModifiedNode, msg.LastModifiedPhysical, msg.LastModifiedCounter);

        return (item, commitHlc);
    }
}

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Outcome of a decision-store mutation attempt.
/// </summary>
internal enum CoordinatorDecisionMutationResult
{
    /// <summary>The delta replicated and the in-memory record set was published.</summary>
    Applied,

    /// <summary>The anchor partition has no local leader (or a stale routing generation); retry.</summary>
    MustRetry,

    /// <summary>The upsert would change the participant set of an already-written decision.</summary>
    RejectedParticipantsFrozen
}

/// <summary>
/// Partition-scoped, replicated store of durable coordinator decision records. Following the placement
/// principle of CockroachDB's transaction record, a record lives in the data partition that currently
/// routes its <see cref="CoordinatorDecisionRecord.RecordAnchorKey"/> (the first confirmed modified key),
/// rather than through the meta partition. Records are keyed by transaction id — globally unique via the
/// HLC node id — and are internal metadata, never inserted into the user key-value namespace.
///
/// <para><b>Replication model.</b> Each mutation replicates a <see cref="CoordinatorDecisionDeltaMessage"/>
/// (an idempotent upsert/remove batch) under <see cref="ReplicationTypes.CoordinatorDecision"/> on the
/// anchor's current data partition. Deltas are idempotent by transaction id, so Raft in-order
/// re-delivery and replay of the log tail above an installed snapshot both converge. In-memory state is
/// published only after Raft success. A durable on-disk snapshot holds the full record set for cold
/// restart; whole-partition state transfer folds records into <c>KvStateMachineTransfer</c>.</para>
///
/// <para><b>Routing authority.</b> The record anchor and participant keys are the routing authority; a
/// stored partition id would be diagnostic only and is not carried. A mutation resolves the anchor to its
/// current partition/generation via <see cref="resolveAnchorPartition"/> and is fenced by the caller's
/// routed generation — a stale generation (after a split/merge) returns
/// <see cref="CoordinatorDecisionMutationResult.MustRetry"/> so the caller re-resolves the anchor.</para>
///
/// <para><b>Single writer.</b> <see cref="UpsertAsync"/> and <see cref="RemoveAsync"/> serialize through
/// <see cref="mutateGate"/> locally and the data-partition Raft log globally. Followers and restore
/// replays call <see cref="Replicate"/> / <see cref="Restore"/>, which apply committed deltas without the
/// gate (Raft delivers them single-threaded and in order).</para>
/// </summary>
internal sealed class CoordinatorDecisionStore : IDisposable
{
    private readonly IRaft raft;

    // Resolves a record anchor key to its current owning data partition and routing generation
    // (RangeRouting.Locate, exposed via KeyValueLocator.LocateRange). Pure map lookup, no forwarding.
    // Attached after construction: the store is built before the locator exists (so the key-value actors
    // can share the one instance for install-on-apply, which needs no resolver), then the resolver is
    // wired once the locator is ready. Only the coordinator-driven UpsertAsync/RemoveAsync use it, and
    // those never run before full manager construction completes.
    private Func<string, (int PartitionId, long Generation)>? resolveAnchorPartition;

    private readonly ILogger<IKahuna> logger;

    private readonly SemaphoreSlim mutateGate = new(1, 1);

    // Serializes every in-memory publish of the node-global record set. The set is a single copy-on-write
    // dictionary shared across all partitions, but Kommander runs a separate executor per partition, so two
    // partitions' apply paths (and the leader's own publish) can otherwise read the same old dictionary, each
    // build a copy, and each publish — silently dropping one partition's change. Held only for the synchronous
    // copy-modify-publish (never across an await), so it cannot deadlock with the mutation path's replicate.
    private readonly object publishLock = new();

    // Directory + filename prefix for the per-partition on-disk snapshots. Each data partition writes its own
    // subset to "{prefix}_p{partitionId}.snapshot", so a per-partition WAL checkpoint can gate on the matching
    // snapshot being durable, and one partition's snapshot failure never blocks another's checkpoint. Null when
    // persistence is disabled (memory-only stores).
    private readonly string? snapshotDirectory;
    private readonly string? snapshotPrefix;

    private readonly object fileLock = new();

    /// <summary>
    /// Test-only injection point: when set and it returns true for a partition, <see cref="PersistSnapshot"/>
    /// reports failure without writing, simulating a snapshot write that could not be made durable so the
    /// checkpoint gate must not advance the WAL retention floor. Never wired in production paths.
    /// </summary>
    internal Func<int, bool>? PersistSnapshotFault { get; set; }

    /// <summary>
    /// The committed record set, keyed by transaction id. Written only inside <see cref="CommitInMemory"/>
    /// (leader eager-commit, follower replicate, restore, state-transfer install); read lock-free.
    /// </summary>
    private volatile IReadOnlyDictionary<HLCTimestamp, CoordinatorDecisionRecord> records =
        new Dictionary<HLCTimestamp, CoordinatorDecisionRecord>();

    /// <summary>
    /// Test-only injection point: when set and it returns true for a record, <see cref="UpsertAsync"/>
    /// returns <see cref="CoordinatorDecisionMutationResult.MustRetry"/> without replicating, simulating a
    /// leader that could not durably persist the progress write. Never wired in production paths.
    /// </summary>
    internal Func<CoordinatorDecisionRecord, bool>? UpsertFault { get; set; }

    /// <summary>
    /// Test-only injection point: when set and it returns true for a destination partition,
    /// <see cref="ReplicateImportToPartitionAsync"/> reports failure without replicating, simulating a
    /// split/merge decision handoff that could not be made durable so cutover must abort. Never wired in
    /// production paths.
    /// </summary>
    internal Func<int, bool>? ReplicateImportFault { get; set; }

    public CoordinatorDecisionStore(
        IRaft raft,
        string? storagePath,
        string? storageRevision,
        ILogger<IKahuna> logger)
    {
        this.raft = raft;
        this.logger = logger;

        if (string.IsNullOrEmpty(storagePath))
        {
            snapshotDirectory = null;
            snapshotPrefix = null;
        }
        else
        {
            snapshotDirectory = storagePath;
            snapshotPrefix = $"coordinatordecision_{storageRevision}";
        }

        LoadFromDisk();
    }

    /// <summary>
    /// Wires the anchor-key → data-partition resolver once the locator is constructed. Called during
    /// manager construction, before any coordinator-driven mutation can run.
    /// </summary>
    public void AttachAnchorResolver(Func<string, (int PartitionId, long Generation)> resolver)
        => resolveAnchorPartition = resolver;

    // ── Reads ──────────────────────────────────────────────────────────────────────────────────

    public int Count => records.Count;

    public bool TryGet(HLCTimestamp transactionId, out CoordinatorDecisionRecord record) =>
        records.TryGetValue(transactionId, out record!);

    /// <summary>A point-in-time snapshot of every record, for the per-partition-leader recovery sweep.</summary>
    public IReadOnlyList<CoordinatorDecisionRecord> SnapshotAll() => records.Values.ToList();

    /// <summary>Records whose anchor currently routes into <c>[startKey, endKey)</c> (ordinal, half-open).</summary>
    public IReadOnlyList<CoordinatorDecisionRecord> SnapshotRange(string? startKey, string? endKey)
    {
        List<CoordinatorDecisionRecord> result = [];
        foreach (CoordinatorDecisionRecord record in records.Values)
        {
            string anchor = record.RecordAnchorKey;
            if (startKey is not null && string.CompareOrdinal(anchor, startKey) < 0)
                continue;
            if (endKey is not null && string.CompareOrdinal(anchor, endKey) >= 0)
                continue;
            result.Add(record);
        }
        return result;
    }

    // ── Write path (leader) ──────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Installs or updates a decision record on its anchor's current data partition. The participant set
    /// is frozen after the first write: an upsert that would change it is rejected. When
    /// <paramref name="expectedGeneration"/> is non-zero it fences the anchor's routing — a mismatch
    /// (a split/merge moved the anchor) returns <see cref="CoordinatorDecisionMutationResult.MustRetry"/>.
    /// </summary>
    public async Task<CoordinatorDecisionMutationResult> UpsertAsync(
        CoordinatorDecisionRecord record, long expectedGeneration, CancellationToken ct)
    {
        if (UpsertFault is not null && UpsertFault(record))
            return CoordinatorDecisionMutationResult.MustRetry;

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            (int partitionId, long generation) = resolveAnchorPartition!(record.RecordAnchorKey);
            if (expectedGeneration != 0 && generation != expectedGeneration)
                return CoordinatorDecisionMutationResult.MustRetry;

            // The participant set is immutable once the decision is written. Progress fields
            // (acked/receiptReleased/status/cleanup) may change; the participant identities and their
            // prepared proposal tickets may not.
            if (records.TryGetValue(record.TransactionId, out CoordinatorDecisionRecord? existing) &&
                !SameParticipants(existing.Participants, record.Participants))
                return CoordinatorDecisionMutationResult.RejectedParticipantsFrozen;

            // Replicate first, then merge the upsert into the latest record set under the publish lock — a
            // forward-only merge so a stale request-path/recovery/echo write cannot regress an ack, a
            // receipt-release, a cleanup-release, or the CommitDecided → Completed transition.
            return await ReplicateThenPublish(
                partitionId, UpsertDelta(record),
                next => MergeUpsert(next, record), ct).ConfigureAwait(false);
        }
        finally
        {
            mutateGate.Release();
        }
    }

    /// <summary>Retires a completed decision record from its anchor's current data partition.</summary>
    public async Task<CoordinatorDecisionMutationResult> RemoveAsync(
        HLCTimestamp transactionId, string recordAnchorKey, CancellationToken ct)
    {
        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            (int partitionId, _) = resolveAnchorPartition!(recordAnchorKey);

            return await ReplicateThenPublish(
                partitionId, RemoveDelta(transactionId),
                next => next.Remove(transactionId), ct).ConfigureAwait(false);
        }
        finally
        {
            mutateGate.Release();
        }
    }

    // Caller holds mutateGate. Replicates the delta on the anchor partition, then applies the same mutation
    // to the latest record set under the publish lock, so the leader reads its own writes immediately (without
    // waiting for the commit echo) while never losing a concurrent partition's publish.
    private async Task<CoordinatorDecisionMutationResult> ReplicateThenPublish(
        int partitionId, CoordinatorDecisionDeltaMessage delta,
        Action<Dictionary<HLCTimestamp, CoordinatorDecisionRecord>> publish, CancellationToken ct)
    {
        byte[] data = ReplicationSerializer.Serialize(delta);

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.CoordinatorDecision,
            data,
            cancellationToken: ct
        ).ConfigureAwait(false);

        if (!result.Success)
        {
            logger.LogWarning(
                "Failed to replicate coordinator-decision mutation Partition={Partition} Status={Status} Ticket={Ticket}",
                partitionId, result.Status, result.TicketId);
            return CoordinatorDecisionMutationResult.MustRetry;
        }

        PublishUnderLock(publish);
        return CoordinatorDecisionMutationResult.Applied;
    }

    // ── Apply path (follower / restore) ────────────────────────────────────────────────────────

    /// <summary>Rebuilds records from a data-partition log entry replayed during WAL restore.</summary>
    public bool Restore(int partitionId, RaftLog log) => Apply(partitionId, log);

    /// <summary>Applies a committed data-partition log entry received via replication (follower / leader echo).</summary>
    public bool Replicate(int partitionId, RaftLog log) => Apply(partitionId, log);

    private bool Apply(int partitionId, RaftLog log)
    {
        if (log.LogType != ReplicationTypes.CoordinatorDecision || log.LogData is null)
            return true;

        try
        {
            CoordinatorDecisionDeltaMessage delta =
                ReplicationSerializer.UnserializeCoordinatorDecisionDeltaMessage(log.LogData);

            // Layer the delta onto the latest record set under the publish lock (never a wholesale replace, and
            // never racing another partition's publish). Idempotent and forward-only by transaction id — a
            // re-delivery or a tail delta replayed below an already-advanced on-disk snapshot merges forward
            // rather than regressing it, and Raft's in-order apply means an upsert cannot resurrect a record a
            // later remove already retired.
            PublishUnderLock(next => ApplyDeltaEntries(next, delta));
            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to apply coordinator-decision log on partition {Partition}", partitionId);
            return false;
        }
    }

    private static void ApplyDeltaEntries(
        Dictionary<HLCTimestamp, CoordinatorDecisionRecord> target, CoordinatorDecisionDeltaMessage delta)
    {
        foreach (CoordinatorDecisionDeltaEntry entry in delta.Entries)
        {
            CoordinatorDecisionRecord record = FromMessage(entry.Record);
            if (entry.Remove)
                target.Remove(record.TransactionId);
            else
                MergeUpsert(target, record);
        }
    }

    // ── Initial install riding the anchor key-value commit ───────────────────────────────────────

    /// <summary>
    /// Installs the initial <c>CommitDecided</c> record as its anchor mutation's committed proposal is
    /// applied (leader inline commit, follower replication, WAL restore) — never a separate Raft round
    /// trip, so the anchor value, its completion receipt, and this record converge from one committed
    /// entry. Idempotent by transaction id: re-delivery and restore replay overwrite with the same record,
    /// and because the record ships the participant set already frozen, a later progress delta on the
    /// decision log still matches. It never regresses a record a later progress delta already advanced,
    /// because the anchor commit precedes those deltas in the anchor partition's single ordered log.
    /// </summary>
    public void InstallFromAnchorCommit(CoordinatorDecisionRecord record)
        => PublishUnderLock(next => MergeUpsert(next, record));

    /// <summary>Serializes one record into the anchor commit envelope's embedded-decision blob.</summary>
    public static byte[] SerializeRecord(CoordinatorDecisionRecord record) => SerializeRecords([record]);

    /// <summary>Reads the single record from an anchor commit envelope's embedded-decision blob.</summary>
    public static CoordinatorDecisionRecord? DeserializeRecord(byte[] data)
    {
        IReadOnlyList<CoordinatorDecisionRecord> records = DeserializeRecords(data);
        return records.Count > 0 ? records[0] : null;
    }

    // ── State transfer (folded into KvStateMachineTransfer) ────────────────────────────────────

    /// <summary>Serializes a set of records into a single snapshot blob for cross-node transfer.</summary>
    public static byte[] SerializeRecords(IEnumerable<CoordinatorDecisionRecord> records)
    {
        CoordinatorDecisionSnapshotMessage message = new();
        foreach (CoordinatorDecisionRecord record in records)
            message.Records.Add(ToMessage(record));
        return ReplicationSerializer.Serialize(message);
    }

    /// <summary>Deserializes a snapshot blob produced by <see cref="SerializeRecords"/>.</summary>
    public static IReadOnlyList<CoordinatorDecisionRecord> DeserializeRecords(byte[] data)
    {
        CoordinatorDecisionSnapshotMessage message =
            ReplicationSerializer.UnserializeCoordinatorDecisionSnapshotMessage(data);
        List<CoordinatorDecisionRecord> result = new(message.Records.Count);
        foreach (CoordinatorDecisionMessage recordMessage in message.Records)
            result.Add(FromMessage(recordMessage));
        return result;
    }

    /// <summary>
    /// Merges transferred records into the local set (split/merge and catch-up repair). Forward-only: a record
    /// already present is advanced, never overwritten backward, so an older handoff cannot clobber newer local
    /// progress and a newer handoff repairs a lagging local copy.
    /// </summary>
    public void ImportRecords(IEnumerable<CoordinatorDecisionRecord> incoming)
    {
        List<CoordinatorDecisionRecord> toMerge = incoming as List<CoordinatorDecisionRecord> ?? incoming.ToList();
        if (toMerge.Count == 0)
            return;

        PublishUnderLock(next =>
        {
            foreach (CoordinatorDecisionRecord record in toMerge)
                MergeUpsert(next, record);
        });

        // Imported records arrive by state transfer, not through this partition's WAL, so WAL-tail replay cannot
        // reconstruct them after a restart. Capture the affected partitions' snapshots now so an imported record
        // is durable before the next checkpoint; the checkpoint gate re-persists and confirms it thereafter.
        if (resolveAnchorPartition is null)
            return;

        HashSet<int> affected = [];
        foreach (CoordinatorDecisionRecord record in toMerge)
            affected.Add(resolveAnchorPartition(record.RecordAnchorKey).PartitionId);

        foreach (int partitionId in affected)
            PersistSnapshot(partitionId);
    }

    /// <summary>
    /// Replicates a batch of decision records onto an explicit destination partition's Raft log during a
    /// range split or merge — not the anchor's currently-resolved partition, because before cutover the anchor
    /// still routes to the source. Every replica of the destination range applies the delta, so a
    /// destination-leader change right after cutover preserves the record instead of stranding it on a former
    /// leader. Returns whether the replication was durable; the split/merge caller gates cutover on it. The
    /// batch is a forward-only upsert, so a record already present on the destination is advanced, never
    /// regressed.
    /// </summary>
    public async Task<bool> ReplicateImportToPartitionAsync(
        int destinationPartitionId, IReadOnlyCollection<CoordinatorDecisionRecord> incoming, CancellationToken ct)
    {
        if (incoming.Count == 0)
            return true;

        if (ReplicateImportFault is not null && ReplicateImportFault(destinationPartitionId))
            return false;

        CoordinatorDecisionDeltaMessage delta = new();
        foreach (CoordinatorDecisionRecord record in incoming)
            delta.Entries.Add(new CoordinatorDecisionDeltaEntry { Remove = false, Record = ToMessage(record) });

        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            CoordinatorDecisionMutationResult result = await ReplicateThenPublish(
                destinationPartitionId, delta,
                next =>
                {
                    foreach (CoordinatorDecisionRecord record in incoming)
                        MergeUpsert(next, record);
                }, ct).ConfigureAwait(false);

            return result == CoordinatorDecisionMutationResult.Applied;
        }
        finally
        {
            mutateGate.Release();
        }
    }

    // ── Delta builders ────────────────────────────────────────────────────────────────────────

    private static CoordinatorDecisionDeltaMessage UpsertDelta(CoordinatorDecisionRecord record)
    {
        CoordinatorDecisionDeltaMessage delta = new();
        delta.Entries.Add(new CoordinatorDecisionDeltaEntry { Remove = false, Record = ToMessage(record) });
        return delta;
    }

    private static CoordinatorDecisionDeltaMessage RemoveDelta(HLCTimestamp transactionId)
    {
        CoordinatorDecisionDeltaMessage delta = new();
        // On remove only the transaction id is read; carry a minimal record.
        delta.Entries.Add(new CoordinatorDecisionDeltaEntry
        {
            Remove = true,
            Record = new CoordinatorDecisionMessage
            {
                TransactionIdNode = transactionId.N,
                TransactionIdPhysical = transactionId.L,
                TransactionIdCounter = transactionId.C
            }
        });
        return delta;
    }

    // ── In-memory publish + durable snapshot ──────────────────────────────────────────────────

    // Applies one mutation to a copy of the current record set and publishes it, serialized against every
    // other publish site (leader upsert/remove, follower/restore apply, anchor install, import) so the
    // node-global copy-on-write set is never lost-updated by a concurrent partition's executor. Purely
    // synchronous: never holds the lock across an await, so it cannot deadlock with a replicate in flight.
    private void PublishUnderLock(Action<Dictionary<HLCTimestamp, CoordinatorDecisionRecord>> mutate)
    {
        lock (publishLock)
        {
            Dictionary<HLCTimestamp, CoordinatorDecisionRecord> next = new(records);
            mutate(next);
            records = next;
            // No eager per-mutation snapshot: every mutation is already durable in the anchor partition's WAL
            // (a replicated decision delta, or the anchor commit that embeds the install). The on-disk snapshot
            // is written per partition at checkpoint time, gating the WAL retention floor so a compacted entry is
            // always captured first; between checkpoints, WAL-tail replay reconstructs the record set.
        }
    }

    // Forward-only upsert into a record-set copy: installs a record that is not yet present, otherwise merges
    // the incoming progress into the existing record without ever regressing it.
    private static void MergeUpsert(
        Dictionary<HLCTimestamp, CoordinatorDecisionRecord> target, CoordinatorDecisionRecord record)
    {
        target[record.TransactionId] = target.TryGetValue(record.TransactionId, out CoordinatorDecisionRecord? existing)
            ? MergeForward(existing, record)
            : record;
    }

    // Merges two views of the same decision record so every monotonic progress field moves only forward: the
    // status advances CommitDecided → Completed but never back, and each participant ack, receipt-release, and
    // cleanup-release latches true. Participant and cleanup identities are frozen at install, so they are taken
    // from the existing record and paired positionally; the immutable header (coordinator/anchor/timestamps) is
    // kept from the existing record. This makes a request-path vs recovery vs echo race, and an out-of-order
    // transfer/replay, converge to the forward-most state instead of clobbering newer progress.
    private static CoordinatorDecisionRecord MergeForward(
        CoordinatorDecisionRecord existing, CoordinatorDecisionRecord incoming)
    {
        List<CoordinatorParticipant> participants = new(existing.Participants.Count);
        for (int i = 0; i < existing.Participants.Count; i++)
        {
            CoordinatorParticipant e = existing.Participants[i];
            CoordinatorParticipant n = i < incoming.Participants.Count ? incoming.Participants[i] : e;
            participants.Add(e with
            {
                Acked = e.Acked || n.Acked,
                ReceiptReleased = e.ReceiptReleased || n.ReceiptReleased
            });
        }

        List<CoordinatorCleanupEffect> cleanup = new(existing.CleanupEffects.Count);
        for (int i = 0; i < existing.CleanupEffects.Count; i++)
        {
            CoordinatorCleanupEffect e = existing.CleanupEffects[i];
            CoordinatorCleanupEffect n = i < incoming.CleanupEffects.Count ? incoming.CleanupEffects[i] : e;
            cleanup.Add(e with { Released = e.Released || n.Released });
        }

        CoordinatorDecisionStatus status = (CoordinatorDecisionStatus)Math.Max((int)existing.Status, (int)incoming.Status);
        HLCTimestamp completedAt = existing.CompletedAt != HLCTimestamp.Zero ? existing.CompletedAt : incoming.CompletedAt;

        return existing with
        {
            Status = status,
            Participants = participants,
            CleanupEffects = cleanup,
            CompletedAt = completedAt
        };
    }

    /// <summary>
    /// Applies an upsert delta through the follower/restore apply path, for tests that exercise the
    /// concurrent cross-partition publish. Mirrors exactly what a replicated decision delta does.
    /// </summary>
    internal bool ApplyUpsertForTest(int partitionId, CoordinatorDecisionRecord record) =>
        Replicate(partitionId, new RaftLog
        {
            LogType = ReplicationTypes.CoordinatorDecision,
            LogData = ReplicationSerializer.Serialize(UpsertDelta(record))
        });

    /// <summary>
    /// Writes the records anchored to <paramref name="partitionId"/> to that partition's on-disk snapshot
    /// (atomic tmp-then-move) and reports whether the write is durable. Called at checkpoint time, before the
    /// partition's WAL retention floor advances: only on a <c>true</c> return may the checkpoint proceed, so a
    /// decision entry about to be compacted is always captured first. A no-op returning <c>true</c> when
    /// persistence is disabled or the anchor resolver is not yet attached (no checkpoint runs that early).
    /// </summary>
    public bool PersistSnapshot(int partitionId)
    {
        if (snapshotDirectory is null || snapshotPrefix is null)
            return true;

        if (PersistSnapshotFault is not null && PersistSnapshotFault(partitionId))
            return false;

        if (resolveAnchorPartition is null)
            return true;

        string path = Path.Combine(snapshotDirectory, $"{snapshotPrefix}_p{partitionId}.snapshot");

        try
        {
            CoordinatorDecisionSnapshotMessage message = new();
            foreach (CoordinatorDecisionRecord record in records.Values)
            {
                (int recordPartition, _) = resolveAnchorPartition(record.RecordAnchorKey);
                if (recordPartition == partitionId)
                    message.Records.Add(ToMessage(record));
            }

            byte[] data = ReplicationSerializer.Serialize(message);
            lock (fileLock)
            {
                string tmp = path + ".tmp";
                File.WriteAllBytes(tmp, data);
                File.Move(tmp, path, overwrite: true);
            }

            return true;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to persist coordinator-decision snapshot to {Path}", path);
            return false;
        }
    }

    // Loads every per-partition snapshot present in the storage directory into the record set, merging forward
    // so overlapping partition files converge. A snapshot file that exists but cannot be parsed is corruption
    // that the record set cannot silently recover from — a below-floor entry may be gone from the WAL — so it
    // fails closed (throws) rather than starting empty and losing a committed decision.
    private void LoadFromDisk()
    {
        if (snapshotDirectory is null || snapshotPrefix is null || !Directory.Exists(snapshotDirectory))
            return;

        string[] files;
        lock (fileLock)
            files = Directory.GetFiles(snapshotDirectory, $"{snapshotPrefix}_p*.snapshot");

        if (files.Length == 0)
            return;

        Dictionary<HLCTimestamp, CoordinatorDecisionRecord> loaded = [];
        foreach (string path in files)
        {
            byte[] data;
            try
            {
                lock (fileLock)
                    data = File.ReadAllBytes(path);
            }
            catch (Exception ex)
            {
                throw new IOException($"Failed to read coordinator-decision snapshot {path}; refusing to start with a possibly incomplete decision set", ex);
            }

            CoordinatorDecisionSnapshotMessage message;
            try
            {
                message = ReplicationSerializer.UnserializeCoordinatorDecisionSnapshotMessage(data);
            }
            catch (Exception ex)
            {
                throw new InvalidDataException($"Corrupt coordinator-decision snapshot {path}; refusing to start empty and lose a committed decision", ex);
            }

            foreach (CoordinatorDecisionMessage recordMessage in message.Records)
                MergeUpsert(loaded, FromMessage(recordMessage));
        }

        records = loaded;
    }

    // ── Proto <-> model conversion ─────────────────────────────────────────────────────────────

    private static CoordinatorDecisionMessage ToMessage(CoordinatorDecisionRecord record)
    {
        CoordinatorDecisionMessage message = new()
        {
            TransactionIdNode = record.TransactionId.N,
            TransactionIdPhysical = record.TransactionId.L,
            TransactionIdCounter = record.TransactionId.C,
            CoordinatorKey = record.CoordinatorKey,
            RecordAnchorKey = record.RecordAnchorKey,
            CommitTimestampNode = record.CommitTimestamp.N,
            CommitTimestampPhysical = record.CommitTimestamp.L,
            CommitTimestampCounter = record.CommitTimestamp.C,
            Status = record.Status == CoordinatorDecisionStatus.Completed
                ? CoordinatorDecisionStatusMessage.CoordinatorDecisionCompleted
                : CoordinatorDecisionStatusMessage.CoordinatorDecisionCommitDecided,
            CreatedAtNode = record.CreatedAt.N,
            CreatedAtPhysical = record.CreatedAt.L,
            CreatedAtCounter = record.CreatedAt.C,
            CompletedAtNode = record.CompletedAt.N,
            CompletedAtPhysical = record.CompletedAt.L,
            CompletedAtCounter = record.CompletedAt.C
        };

        foreach (CoordinatorParticipant participant in record.Participants)
            message.Participants.Add(new CoordinatorParticipantMessage
            {
                Key = participant.Key,
                Durability = (int)participant.Durability,
                TicketIdNode = participant.TicketId.N,
                TicketIdPhysical = participant.TicketId.L,
                TicketIdCounter = participant.TicketId.C,
                Acked = participant.Acked,
                ReceiptReleased = participant.ReceiptReleased
            });

        foreach (CoordinatorCleanupEffect effect in record.CleanupEffects)
            message.CleanupEffects.Add(new CoordinatorCleanupEffectMessage
            {
                Kind = effect.Kind switch
                {
                    CoordinatorCleanupKind.PrefixLock => CoordinatorCleanupKindMessage.CoordinatorCleanupPrefixLock,
                    CoordinatorCleanupKind.RangeLock => CoordinatorCleanupKindMessage.CoordinatorCleanupRangeLock,
                    _ => CoordinatorCleanupKindMessage.CoordinatorCleanupKeyRelease
                },
                Key = effect.Key,
                Durability = (int)effect.Durability,
                StartKey = effect.StartKey ?? "",
                StartInclusive = effect.StartInclusive,
                EndKey = effect.EndKey ?? "",
                EndInclusive = effect.EndInclusive,
                StartKeyNull = effect.StartKey is null,
                EndKeyNull = effect.EndKey is null,
                Released = effect.Released
            });

        return message;
    }

    private static CoordinatorDecisionRecord FromMessage(CoordinatorDecisionMessage message)
    {
        List<CoordinatorParticipant> participants = new(message.Participants.Count);
        foreach (CoordinatorParticipantMessage participant in message.Participants)
            participants.Add(new CoordinatorParticipant(
                participant.Key,
                (KeyValueDurability)participant.Durability,
                new HLCTimestamp(participant.TicketIdNode, participant.TicketIdPhysical, participant.TicketIdCounter),
                participant.Acked,
                participant.ReceiptReleased));

        List<CoordinatorCleanupEffect> cleanupEffects = new(message.CleanupEffects.Count);
        foreach (CoordinatorCleanupEffectMessage effect in message.CleanupEffects)
            cleanupEffects.Add(new CoordinatorCleanupEffect(
                effect.Kind switch
                {
                    CoordinatorCleanupKindMessage.CoordinatorCleanupPrefixLock => CoordinatorCleanupKind.PrefixLock,
                    CoordinatorCleanupKindMessage.CoordinatorCleanupRangeLock => CoordinatorCleanupKind.RangeLock,
                    _ => CoordinatorCleanupKind.KeyRelease
                },
                effect.Key,
                (KeyValueDurability)effect.Durability,
                effect.StartKeyNull ? null : effect.StartKey,
                effect.StartInclusive,
                effect.EndKeyNull ? null : effect.EndKey,
                effect.EndInclusive,
                effect.Released));

        return new CoordinatorDecisionRecord(
            new HLCTimestamp(message.TransactionIdNode, message.TransactionIdPhysical, message.TransactionIdCounter),
            message.CoordinatorKey,
            message.RecordAnchorKey,
            new HLCTimestamp(message.CommitTimestampNode, message.CommitTimestampPhysical, message.CommitTimestampCounter),
            message.Status == CoordinatorDecisionStatusMessage.CoordinatorDecisionCompleted
                ? CoordinatorDecisionStatus.Completed
                : CoordinatorDecisionStatus.CommitDecided,
            participants,
            cleanupEffects,
            new HLCTimestamp(message.CreatedAtNode, message.CreatedAtPhysical, message.CreatedAtCounter),
            new HLCTimestamp(message.CompletedAtNode, message.CompletedAtPhysical, message.CompletedAtCounter));
    }

    private static bool SameParticipants(
        IReadOnlyList<CoordinatorParticipant> a, IReadOnlyList<CoordinatorParticipant> b)
    {
        if (a.Count != b.Count)
            return false;
        for (int i = 0; i < a.Count; i++)
            if (!string.Equals(a[i].Key, b[i].Key, StringComparison.Ordinal) ||
                a[i].Durability != b[i].Durability ||
                a[i].TicketId != b[i].TicketId)
                return false;
        return true;
    }

    public void Dispose()
    {
        mutateGate.Dispose();
    }
}

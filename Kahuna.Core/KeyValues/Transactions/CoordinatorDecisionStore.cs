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

    private readonly string? snapshotPath;

    private readonly object fileLock = new();

    /// <summary>
    /// The committed record set, keyed by transaction id. Written only inside <see cref="CommitInMemory"/>
    /// (leader eager-commit, follower replicate, restore, state-transfer install); read lock-free.
    /// </summary>
    private volatile IReadOnlyDictionary<HLCTimestamp, CoordinatorDecisionRecord> records =
        new Dictionary<HLCTimestamp, CoordinatorDecisionRecord>();

    public CoordinatorDecisionStore(
        IRaft raft,
        string? storagePath,
        string? storageRevision,
        ILogger<IKahuna> logger)
    {
        this.raft = raft;
        this.logger = logger;

        snapshotPath = string.IsNullOrEmpty(storagePath)
            ? null
            : Path.Combine(storagePath, $"coordinatordecision_{storageRevision}.snapshot");

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
        await mutateGate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            (int partitionId, long generation) = resolveAnchorPartition!(record.RecordAnchorKey);
            if (expectedGeneration != 0 && generation != expectedGeneration)
                return CoordinatorDecisionMutationResult.MustRetry;

            // The participant set is immutable once the decision is written. Progress fields
            // (acked/receiptReleased/status/cleanup) may change; the participant identities may not.
            if (records.TryGetValue(record.TransactionId, out CoordinatorDecisionRecord? existing) &&
                !SameParticipants(existing.Participants, record.Participants))
                return CoordinatorDecisionMutationResult.RejectedParticipantsFrozen;

            Dictionary<HLCTimestamp, CoordinatorDecisionRecord> next = new(records)
            {
                [record.TransactionId] = record
            };

            return await ReplicateDeltaAsync(partitionId, UpsertDelta(record), next, ct).ConfigureAwait(false);
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

            Dictionary<HLCTimestamp, CoordinatorDecisionRecord> next = new(records);
            next.Remove(transactionId);

            return await ReplicateDeltaAsync(partitionId, RemoveDelta(transactionId), next, ct).ConfigureAwait(false);
        }
        finally
        {
            mutateGate.Release();
        }
    }

    // Caller holds mutateGate. Replicates the delta on the anchor partition, then publishes the
    // already-computed record set so the leader reads its own writes without waiting for the commit echo.
    private async Task<CoordinatorDecisionMutationResult> ReplicateDeltaAsync(
        int partitionId, CoordinatorDecisionDeltaMessage delta,
        Dictionary<HLCTimestamp, CoordinatorDecisionRecord> next, CancellationToken ct)
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

        CommitInMemory(next);
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

            // Layer the delta onto the current record set (never a wholesale replace). Idempotent by
            // transaction id — re-delivery or tail replay above an installed snapshot converges, and Raft's
            // in-order apply means an upsert cannot resurrect a record a later remove already retired.
            Dictionary<HLCTimestamp, CoordinatorDecisionRecord> next = new(records);
            ApplyDeltaEntries(next, delta);
            CommitInMemory(next);
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
                target[record.TransactionId] = record;
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
    {
        Dictionary<HLCTimestamp, CoordinatorDecisionRecord> next = new(records)
        {
            [record.TransactionId] = record
        };
        CommitInMemory(next);
    }

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

    /// <summary>Merges transferred records into the local set (split/merge and catch-up repair).</summary>
    public void ImportRecords(IEnumerable<CoordinatorDecisionRecord> incoming)
    {
        Dictionary<HLCTimestamp, CoordinatorDecisionRecord> next = new(records);
        bool changed = false;
        foreach (CoordinatorDecisionRecord record in incoming)
        {
            next[record.TransactionId] = record;
            changed = true;
        }
        if (changed)
            CommitInMemory(next);
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

    private void CommitInMemory(Dictionary<HLCTimestamp, CoordinatorDecisionRecord> next)
    {
        records = next;
        PersistToDisk(next);
    }

    private void PersistToDisk(IReadOnlyDictionary<HLCTimestamp, CoordinatorDecisionRecord> snapshot)
    {
        if (snapshotPath is null)
            return;

        try
        {
            CoordinatorDecisionSnapshotMessage message = new();
            foreach (CoordinatorDecisionRecord record in snapshot.Values)
                message.Records.Add(ToMessage(record));

            byte[] data = ReplicationSerializer.Serialize(message);
            lock (fileLock)
            {
                string tmp = snapshotPath + ".tmp";
                File.WriteAllBytes(tmp, data);
                File.Move(tmp, snapshotPath, overwrite: true);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to persist coordinator-decision snapshot to {Path}", snapshotPath);
        }
    }

    private void LoadFromDisk()
    {
        if (snapshotPath is null || !File.Exists(snapshotPath))
            return;

        try
        {
            byte[] data;
            lock (fileLock)
                data = File.ReadAllBytes(snapshotPath);

            CoordinatorDecisionSnapshotMessage message =
                ReplicationSerializer.UnserializeCoordinatorDecisionSnapshotMessage(data);

            Dictionary<HLCTimestamp, CoordinatorDecisionRecord> loaded = [];
            foreach (CoordinatorDecisionMessage recordMessage in message.Records)
            {
                CoordinatorDecisionRecord record = FromMessage(recordMessage);
                loaded[record.TransactionId] = record;
            }
            records = loaded;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load coordinator-decision snapshot from {Path}", snapshotPath);
        }
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
                Effect = effect.Effect,
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
            cleanupEffects.Add(new CoordinatorCleanupEffect(effect.Effect, effect.Released));

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
            if (!string.Equals(a[i].Key, b[i].Key, StringComparison.Ordinal) || a[i].Durability != b[i].Durability)
                return false;
        return true;
    }

    public void Dispose()
    {
        mutateGate.Dispose();
    }
}

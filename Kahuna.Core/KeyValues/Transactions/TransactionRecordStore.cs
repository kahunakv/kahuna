using System.Collections.Concurrent;
using Google.Protobuf;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// The partition-scoped authority for canonical transaction records, keyed by <c>(TransactionId, Epoch)</c> for
/// every transaction whose record anchor routes to this data partition. Every transition replicates as a
/// <see cref="TransactionRecordDeltaMessage"/> of commands; <see cref="Replicate"/>/<see cref="Restore"/> re-apply
/// those commands through the deterministic <see cref="TransactionRecordStateMachine"/> in Raft log order, so a
/// commit/abort race resolves identically on every replica. Mutation happens on the single per-partition apply
/// path; reads may run concurrently, which the concurrent map makes safe.
/// </summary>
internal sealed class TransactionRecordStore
{
    private readonly ConcurrentDictionary<(HLCTimestamp TransactionId, long Epoch), TransactionRecord> records = new();

    private readonly string? snapshotDirectory;

    private readonly string? snapshotPrefix;

    private readonly ILogger<IKahuna>? logger;

    private readonly object fileLock = new();

    // Serializes the read-decide-write of a single transition so the compare-and-set is atomic. The concurrent map
    // makes each dictionary operation safe, but not the read-then-write pair: a commit and an abort for the same
    // record can be applied concurrently (Raft-ordered apply, producer-side apply, recovery), and without this the
    // losing transition could observe the pre-decision state and overwrite the winner.
    private readonly object applyGate = new();

    // Resolves a record's anchor key to its current data partition, so a per-partition snapshot/transfer only
    // covers the records this partition owns. Null in the pure/in-memory configuration used by unit tests.
    private Func<string, (int PartitionId, long Generation)>? resolveAnchorPartition;

    /// <summary>In-memory configuration (unit tests): no persistence.</summary>
    public TransactionRecordStore() { }

    /// <summary>Durable configuration: a per-partition on-disk snapshot under <paramref name="storagePath"/> is
    /// loaded on construction and rewritten by <see cref="PersistSnapshot"/> before the WAL checkpoint discards
    /// the log tail. A parse failure fails closed rather than silently starting without a committed decision.</summary>
    public TransactionRecordStore(string? storagePath, string? storageRevision, ILogger<IKahuna>? logger)
    {
        this.logger = logger;
        if (!string.IsNullOrEmpty(storagePath))
        {
            snapshotDirectory = storagePath;
            snapshotPrefix = $"transactionrecord_{storageRevision}";
            LoadFromDisk();
        }
    }

    /// <summary>Wires the anchor-key → data-partition resolver once the locator exists (manager construction).</summary>
    public void AttachAnchorResolver(Func<string, (int PartitionId, long Generation)> resolver) =>
        resolveAnchorPartition = resolver;

    /// <summary>Applies one transition to the record it targets and reflects the result in the map. This is the
    /// single apply entry point shared by local proposal apply, follower replication, and restore.</summary>
    public TransactionRecordApplyResult Apply(TransactionRecordCommand command)
    {
        (HLCTimestamp, long) key = KeyOf(command);

        lock (applyGate)
        {
            records.TryGetValue(key, out TransactionRecord? existing);

            TransactionRecordApplyResult result = TransactionRecordStateMachine.Apply(existing, command);

            if (result.Outcome == TransactionApplyOutcome.Applied && result.Record is not null)
                records[key] = result.Record;
            else if (result.Outcome == TransactionApplyOutcome.Removed)
                records.TryRemove(key, out _);

            return result;
        }
    }

    public TransactionRecord? Get(HLCTimestamp transactionId, long epoch) =>
        records.TryGetValue((transactionId, epoch), out TransactionRecord? record) ? record : null;

    public IReadOnlyCollection<TransactionRecord> Snapshot() => records.Values.ToArray();

    public int Count => records.Count;

    // ── replication ─────────────────────────────────────────────────────────────

    public bool Restore(int partitionId, RaftLog log) => ApplyLog(log);

    public bool Replicate(int partitionId, RaftLog log) => ApplyLog(log);

    private bool ApplyLog(RaftLog log)
    {
        if (log.LogType != ReplicationTypes.TransactionRecord || log.LogData is null)
            return true;

        TransactionRecordDeltaMessage delta = ReplicationSerializer.UnserializeTransactionRecordDeltaMessage(log.LogData);

        foreach (TransactionRecordCommandMessage message in delta.Commands)
            Apply(ToCommand(message));

        return true;
    }

    /// <summary>Serializes a batch of transitions for one atomic data-partition log entry.</summary>
    public static byte[] SerializeDelta(IEnumerable<TransactionRecordCommand> commands)
    {
        TransactionRecordDeltaMessage delta = new();
        foreach (TransactionRecordCommand command in commands)
            delta.Commands.Add(ToProto(command));

        return ReplicationSerializer.Serialize(delta);
    }

    /// <summary>
    /// Serializes a delta of the transitions that faithfully reconstruct each of <paramref name="records"/> on a
    /// destination partition through the ordinary deterministic apply — used to hand moved records to the
    /// destination of a split/merge without a bespoke import log type. A manifestless abort tombstone replays its
    /// abort alone (so it stays manifestless); every other record replays its initialization, then its terminal
    /// decision (if any) reusing the winning op id and decided-at HLC so the deadline gate re-accepts the commit.
    /// </summary>
    public static byte[] SerializeReconstructionDelta(IEnumerable<TransactionRecord> records)
    {
        List<TransactionRecordCommand> commands = [];

        foreach (TransactionRecord r in records)
        {
            bool manifestlessTombstone = r.Decision == TransactionDecision.Abort && !r.ManifestPresent;

            if (!manifestlessTombstone)
                commands.Add(new InitializeTransactionCommand(
                    r.TransactionId, r.Epoch, r.CoordinatorKey, r.RecordAnchorKey,
                    r.CommitTimestamp, r.DecisionDeadline, r.ManifestHash, r.Participants,
                    HLCTimestamp.Zero, r.CreatedAt));

            switch (r.Decision)
            {
                case TransactionDecision.Commit:
                    commands.Add(new CommitTransactionCommand(r.TransactionId, r.Epoch, r.ManifestHash, r.WinningOpId, r.DecidedAt));
                    break;

                case TransactionDecision.Abort:
                    commands.Add(new AbortTransactionCommand(r.TransactionId, r.Epoch, r.ManifestHash, r.AbortClass, r.WinningOpId, r.DecidedAt,
                        r.RecordAnchorKey, r.CommitTimestamp, r.DecisionDeadline, r.CreatedAt));
                    break;
            }
        }

        return SerializeDelta(commands);
    }

    private static (HLCTimestamp, long) KeyOf(TransactionRecordCommand command) => command switch
    {
        InitializeTransactionCommand i => (i.TransactionId, i.Epoch),
        CommitTransactionCommand c => (c.TransactionId, c.Epoch),
        AbortTransactionCommand a => (a.TransactionId, a.Epoch),
        PurgeTransactionCommand p => (p.TransactionId, p.Epoch),
        _ => throw new ArgumentOutOfRangeException(nameof(command), command.GetType().Name, "unknown transaction-record command")
    };

    // ── command <-> proto ─────────────────────────────────────────────────────────

    private static TransactionRecordCommandMessage ToProto(TransactionRecordCommand command)
    {
        switch (command)
        {
            case InitializeTransactionCommand i:
            {
                TransactionRecordCommandMessage m = new()
                {
                    Kind = TransactionRecordCommandKindMessage.TransactionRecordInitialize,
                    TransactionIdNode = i.TransactionId.N, TransactionIdPhysical = i.TransactionId.L, TransactionIdCounter = i.TransactionId.C,
                    Epoch = i.Epoch, ManifestHash = i.ManifestHash,
                    OpIdNode = i.OpId.N, OpIdPhysical = i.OpId.L, OpIdCounter = i.OpId.C,
                    CoordinatorKey = i.CoordinatorKey, RecordAnchorKey = i.RecordAnchorKey,
                    CommitTimestampNode = i.CommitTimestamp.N, CommitTimestampPhysical = i.CommitTimestamp.L, CommitTimestampCounter = i.CommitTimestamp.C,
                    DecisionDeadlineNode = i.DecisionDeadline.N, DecisionDeadlinePhysical = i.DecisionDeadline.L, DecisionDeadlineCounter = i.DecisionDeadline.C,
                    CreatedAtNode = i.CreatedAt.N, CreatedAtPhysical = i.CreatedAt.L, CreatedAtCounter = i.CreatedAt.C
                };

                foreach (TransactionParticipantRef p in i.Participants)
                    m.Participants.Add(new TransactionParticipantRefMessage { Key = p.Key, Durability = (int)p.Durability });

                return m;
            }

            case CommitTransactionCommand c:
                return new()
                {
                    Kind = TransactionRecordCommandKindMessage.TransactionRecordCommit,
                    TransactionIdNode = c.TransactionId.N, TransactionIdPhysical = c.TransactionId.L, TransactionIdCounter = c.TransactionId.C,
                    Epoch = c.Epoch, ManifestHash = c.ManifestHash,
                    OpIdNode = c.OpId.N, OpIdPhysical = c.OpId.L, OpIdCounter = c.OpId.C,
                    AttemptNode = c.AttemptHlc.N, AttemptPhysical = c.AttemptHlc.L, AttemptCounter = c.AttemptHlc.C
                };

            case AbortTransactionCommand a:
                return new()
                {
                    Kind = TransactionRecordCommandKindMessage.TransactionRecordAbort,
                    TransactionIdNode = a.TransactionId.N, TransactionIdPhysical = a.TransactionId.L, TransactionIdCounter = a.TransactionId.C,
                    Epoch = a.Epoch, ManifestHash = a.ManifestHash, AbortClass = (int)a.AbortClass,
                    OpIdNode = a.OpId.N, OpIdPhysical = a.OpId.L, OpIdCounter = a.OpId.C,
                    AttemptNode = a.AttemptHlc.N, AttemptPhysical = a.AttemptHlc.L, AttemptCounter = a.AttemptHlc.C,
                    RecordAnchorKey = a.RecordAnchorKey,
                    CommitTimestampNode = a.CommitTimestamp.N, CommitTimestampPhysical = a.CommitTimestamp.L, CommitTimestampCounter = a.CommitTimestamp.C,
                    DecisionDeadlineNode = a.DecisionDeadline.N, DecisionDeadlinePhysical = a.DecisionDeadline.L, DecisionDeadlineCounter = a.DecisionDeadline.C,
                    CreatedAtNode = a.CreatedAt.N, CreatedAtPhysical = a.CreatedAt.L, CreatedAtCounter = a.CreatedAt.C
                };

            case PurgeTransactionCommand p:
                return new()
                {
                    Kind = TransactionRecordCommandKindMessage.TransactionRecordPurge,
                    TransactionIdNode = p.TransactionId.N, TransactionIdPhysical = p.TransactionId.L, TransactionIdCounter = p.TransactionId.C,
                    Epoch = p.Epoch
                };

            default:
                throw new ArgumentOutOfRangeException(nameof(command), command.GetType().Name, "unknown transaction-record command");
        }
    }

    private static TransactionRecordCommand ToCommand(TransactionRecordCommandMessage m)
    {
        HLCTimestamp txId = new(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter);
        HLCTimestamp opId = new(m.OpIdNode, m.OpIdPhysical, m.OpIdCounter);

        switch (m.Kind)
        {
            case TransactionRecordCommandKindMessage.TransactionRecordInitialize:
            {
                List<TransactionParticipantRef> participants = new(m.Participants.Count);
                foreach (TransactionParticipantRefMessage p in m.Participants)
                    participants.Add(new TransactionParticipantRef(p.Key, (KeyValueDurability)p.Durability));

                return new InitializeTransactionCommand(
                    txId, m.Epoch, m.CoordinatorKey, m.RecordAnchorKey,
                    new HLCTimestamp(m.CommitTimestampNode, m.CommitTimestampPhysical, m.CommitTimestampCounter),
                    new HLCTimestamp(m.DecisionDeadlineNode, m.DecisionDeadlinePhysical, m.DecisionDeadlineCounter),
                    m.ManifestHash, participants, opId,
                    new HLCTimestamp(m.CreatedAtNode, m.CreatedAtPhysical, m.CreatedAtCounter));
            }

            case TransactionRecordCommandKindMessage.TransactionRecordCommit:
                return new CommitTransactionCommand(txId, m.Epoch, m.ManifestHash, opId,
                    new HLCTimestamp(m.AttemptNode, m.AttemptPhysical, m.AttemptCounter));

            case TransactionRecordCommandKindMessage.TransactionRecordAbort:
                return new AbortTransactionCommand(txId, m.Epoch, m.ManifestHash, (TransactionAbortClass)m.AbortClass, opId,
                    new HLCTimestamp(m.AttemptNode, m.AttemptPhysical, m.AttemptCounter),
                    m.RecordAnchorKey,
                    new HLCTimestamp(m.CommitTimestampNode, m.CommitTimestampPhysical, m.CommitTimestampCounter),
                    new HLCTimestamp(m.DecisionDeadlineNode, m.DecisionDeadlinePhysical, m.DecisionDeadlineCounter),
                    new HLCTimestamp(m.CreatedAtNode, m.CreatedAtPhysical, m.CreatedAtCounter));

            case TransactionRecordCommandKindMessage.TransactionRecordPurge:
                return new PurgeTransactionCommand(txId, m.Epoch);

            default:
                throw new ArgumentOutOfRangeException(nameof(m), m.Kind, "unknown transaction-record command kind");
        }
    }

    // ── durable snapshot ──────────────────────────────────────────────────────────

    /// <summary>Atomically rewrites this partition's on-disk snapshot with the records whose anchor currently
    /// routes to it. Returns true (durable) so the WAL checkpoint may discard the covered log tail; false on a
    /// write failure gates the checkpoint. A no-op (true) when persistence or the resolver is not configured.</summary>
    public bool PersistSnapshot(int partitionId)
    {
        if (snapshotDirectory is null || snapshotPrefix is null || resolveAnchorPartition is null)
            return true;

        string path = Path.Combine(snapshotDirectory, $"{snapshotPrefix}_p{partitionId}.snapshot");

        try
        {
            TransactionRecordSnapshotMessage message = new();
            foreach (TransactionRecord record in records.Values)
            {
                if (resolveAnchorPartition(record.RecordAnchorKey).PartitionId == partitionId)
                    message.Records.Add(ToSnapshotEntry(record));
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
            logger?.LogError(ex, "Failed to persist transaction-record snapshot to {Path}", path);
            return false;
        }
    }

    private void LoadFromDisk()
    {
        if (snapshotDirectory is null || snapshotPrefix is null || !Directory.Exists(snapshotDirectory))
            return;

        string[] files;
        lock (fileLock)
            files = Directory.GetFiles(snapshotDirectory, $"{snapshotPrefix}_p*.snapshot");

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
                throw new IOException($"Failed to read transaction-record snapshot {path}; refusing to start with a possibly incomplete decision set", ex);
            }

            TransactionRecordSnapshotMessage message;
            try
            {
                message = ReplicationSerializer.UnserializeTransactionRecordSnapshotMessage(data);
            }
            catch (Exception ex)
            {
                throw new InvalidDataException($"Corrupt transaction-record snapshot {path}; refusing to start empty and lose a committed decision", ex);
            }

            foreach (TransactionRecordSnapshotEntry entry in message.Records)
                MergeLoad(FromSnapshotEntry(entry));
        }
    }

    // Load-time merge across (possibly overlapping) per-partition files: a terminal decision is authoritative
    // over an Undecided one; two terminal records for one identity must agree (immutable decision), so the first
    // wins and a conflict is logged rather than silently overwriting.
    private void MergeLoad(TransactionRecord incoming)
    {
        (HLCTimestamp, long) key = (incoming.TransactionId, incoming.Epoch);
        if (!records.TryGetValue(key, out TransactionRecord? existing))
        {
            records[key] = incoming;
            return;
        }

        if (existing.IsTerminal && incoming.IsTerminal && existing.Decision != incoming.Decision)
        {
            logger?.LogError("Conflicting terminal transaction records on load for {TxId}/{Epoch}: {A} vs {B}",
                incoming.TransactionId, incoming.Epoch, existing.Decision, incoming.Decision);
            return;
        }

        if (!existing.IsTerminal && incoming.IsTerminal)
            records[key] = incoming;
    }

    // ── state transfer (split/merge) ────────────────────────────────────────────────

    /// <summary>Records whose anchor currently routes into <c>[startKey, endKey)</c> (ordinal, half-open) — the
    /// set a range split/merge hands to the destination partition.</summary>
    public IReadOnlyList<TransactionRecord> SnapshotRange(string? startKey, string? endKey)
    {
        List<TransactionRecord> result = [];
        foreach (TransactionRecord record in records.Values)
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

    /// <summary>Folds transferred records into this partition's set (idempotent by identity + terminal-decision
    /// authority), for whole-partition state transfer that repairs a below-floor node or a split/merge cutover.</summary>
    public void ImportRecords(IEnumerable<TransactionRecord> incoming)
    {
        foreach (TransactionRecord record in incoming)
            MergeLoad(record);
    }

    public static byte[] SerializeRecords(IEnumerable<TransactionRecord> records)
    {
        TransactionRecordSnapshotMessage message = new();
        foreach (TransactionRecord record in records)
            message.Records.Add(ToSnapshotEntry(record));

        return ReplicationSerializer.Serialize(message);
    }

    public static IReadOnlyList<TransactionRecord> DeserializeRecords(byte[] data)
    {
        TransactionRecordSnapshotMessage message = ReplicationSerializer.UnserializeTransactionRecordSnapshotMessage(data);
        List<TransactionRecord> result = new(message.Records.Count);
        foreach (TransactionRecordSnapshotEntry entry in message.Records)
            result.Add(FromSnapshotEntry(entry));

        return result;
    }

    // ── record <-> snapshot proto ───────────────────────────────────────────────────

    private static TransactionRecordSnapshotEntry ToSnapshotEntry(TransactionRecord r)
    {
        TransactionRecordSnapshotEntry entry = new()
        {
            TransactionIdNode = r.TransactionId.N, TransactionIdPhysical = r.TransactionId.L, TransactionIdCounter = r.TransactionId.C,
            Epoch = r.Epoch, CoordinatorKey = r.CoordinatorKey, RecordAnchorKey = r.RecordAnchorKey,
            CommitTimestampNode = r.CommitTimestamp.N, CommitTimestampPhysical = r.CommitTimestamp.L, CommitTimestampCounter = r.CommitTimestamp.C,
            DecisionDeadlineNode = r.DecisionDeadline.N, DecisionDeadlinePhysical = r.DecisionDeadline.L, DecisionDeadlineCounter = r.DecisionDeadline.C,
            ManifestHash = r.ManifestHash, ManifestPresent = r.ManifestPresent,
            Decision = r.Decision switch
            {
                TransactionDecision.Commit => TransactionDecisionMessage.TransactionDecisionCommit,
                TransactionDecision.Abort => TransactionDecisionMessage.TransactionDecisionAbort,
                _ => TransactionDecisionMessage.TransactionDecisionUndecided
            },
            AbortClass = (int)r.AbortClass,
            WinningOpIdNode = r.WinningOpId.N, WinningOpIdPhysical = r.WinningOpId.L, WinningOpIdCounter = r.WinningOpId.C,
            CreatedAtNode = r.CreatedAt.N, CreatedAtPhysical = r.CreatedAt.L, CreatedAtCounter = r.CreatedAt.C,
            DecidedAtNode = r.DecidedAt.N, DecidedAtPhysical = r.DecidedAt.L, DecidedAtCounter = r.DecidedAt.C
        };

        foreach (TransactionParticipantRef p in r.Participants)
            entry.Participants.Add(new TransactionParticipantRefMessage { Key = p.Key, Durability = (int)p.Durability });

        return entry;
    }

    private static TransactionRecord FromSnapshotEntry(TransactionRecordSnapshotEntry e)
    {
        List<TransactionParticipantRef> participants = new(e.Participants.Count);
        foreach (TransactionParticipantRefMessage p in e.Participants)
            participants.Add(new TransactionParticipantRef(p.Key, (KeyValueDurability)p.Durability));

        return new TransactionRecord(
            new HLCTimestamp(e.TransactionIdNode, e.TransactionIdPhysical, e.TransactionIdCounter),
            e.Epoch, e.CoordinatorKey, e.RecordAnchorKey,
            new HLCTimestamp(e.CommitTimestampNode, e.CommitTimestampPhysical, e.CommitTimestampCounter),
            new HLCTimestamp(e.DecisionDeadlineNode, e.DecisionDeadlinePhysical, e.DecisionDeadlineCounter),
            e.ManifestHash, participants, e.ManifestPresent,
            e.Decision switch
            {
                TransactionDecisionMessage.TransactionDecisionCommit => TransactionDecision.Commit,
                TransactionDecisionMessage.TransactionDecisionAbort => TransactionDecision.Abort,
                _ => TransactionDecision.Undecided
            },
            (TransactionAbortClass)e.AbortClass,
            new HLCTimestamp(e.WinningOpIdNode, e.WinningOpIdPhysical, e.WinningOpIdCounter),
            new HLCTimestamp(e.CreatedAtNode, e.CreatedAtPhysical, e.CreatedAtCounter),
            new HLCTimestamp(e.DecidedAtNode, e.DecidedAtPhysical, e.DecidedAtCounter));
    }
}

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
/// The partition-scoped authority for durable prepared intents. Holds at most one live intent per logical key and
/// routes every prepare/resolve/remove transition through the deterministic
/// <see cref="PreparedIntentStateMachine"/>, so leader apply, follower apply, WAL replay, and state-transfer
/// replay converge. It is the source of truth for a key's pending mutation after its prepare commits — the
/// actor-local write intent is only a cache.
///
/// <para>Mutation happens on the single per-partition apply path (Raft apply / restore); reads (visibility
/// lookups and the recovery sweep) may run concurrently, which the concurrent map makes safe. This type owns no
/// replication or persistence; that plumbing serializes/dispatches these same transitions.</para>
/// </summary>
internal sealed class PreparedIntentStore
{
    private readonly ConcurrentDictionary<string, PreparedIntent> intents = new();

    private readonly string? snapshotDirectory;

    private readonly string? snapshotPrefix;

    private readonly ILogger<IKahuna>? logger;

    private readonly object fileLock = new();

    // Resolves an intent's key to its current data partition, so a per-partition snapshot/transfer only covers
    // the intents this partition owns. Null in the pure/in-memory configuration used by unit tests.
    private Func<string, int>? resolvePartition;

    /// <summary>In-memory configuration (unit tests): no persistence.</summary>
    public PreparedIntentStore() { }

    /// <summary>Durable configuration: a per-partition on-disk snapshot under <paramref name="storagePath"/> is
    /// loaded on construction and rewritten by <see cref="PersistSnapshot"/> before the WAL checkpoint discards
    /// the log tail; a parse failure fails closed.</summary>
    public PreparedIntentStore(string? storagePath, string? storageRevision, ILogger<IKahuna>? logger)
    {
        this.logger = logger;
        if (!string.IsNullOrEmpty(storagePath))
        {
            snapshotDirectory = storagePath;
            snapshotPrefix = $"preparedintent_{storageRevision}";
            LoadFromDisk();
        }
    }

    /// <summary>Wires the key → data-partition resolver once the locator exists (manager construction).</summary>
    public void AttachPartitionResolver(Func<string, int> resolver) => resolvePartition = resolver;

    /// <summary>Applies one transition to the intent at the command's key and reflects the result in the map:
    /// install/update on <see cref="TransactionApplyOutcome.Applied"/> with a record, delete when the applied
    /// record is null (removal), and leave the map unchanged on a no-op or rejection.</summary>
    public PreparedIntentApplyResult Apply(PreparedIntentCommand command)
    {
        string key = KeyOf(command);

        intents.TryGetValue(key, out PreparedIntent? existing);

        PreparedIntentApplyResult result = PreparedIntentStateMachine.Apply(existing, command);

        if (result.Outcome == TransactionApplyOutcome.Applied)
        {
            if (result.Intent is null)
                intents.TryRemove(key, out _);
            else
                intents[key] = result.Intent;
        }

        return result;
    }

    /// <summary>The current intent at <paramref name="key"/>, or null.</summary>
    public PreparedIntent? Get(string key) =>
        intents.TryGetValue(key, out PreparedIntent? intent) ? intent : null;

    /// <summary>The intent at <paramref name="key"/> only when it belongs to the given transaction attempt.</summary>
    public PreparedIntent? GetByIdentity(HLCTimestamp transactionId, long epoch, string key) =>
        intents.TryGetValue(key, out PreparedIntent? intent)
        && intent.TransactionId == transactionId
        && intent.Epoch == epoch
            ? intent
            : null;

    /// <summary>Every intent currently held on this partition — the input to the recovery sweep.</summary>
    public IReadOnlyCollection<PreparedIntent> Snapshot() => intents.Values.ToArray();

    /// <summary>Pending (undecided) intents whose recovery deadline is at or before <paramref name="now"/> —
    /// candidates for a recovery decision lookup. Deadline comparison is by HLC, never a local clock.</summary>
    public IReadOnlyList<PreparedIntent> DueForRecovery(HLCTimestamp now)
    {
        List<PreparedIntent> due = [];
        foreach (PreparedIntent intent in intents.Values)
        {
            if (intent.IsPending && intent.RecoveryDeadline != HLCTimestamp.Zero && intent.RecoveryDeadline <= now)
                due.Add(intent);
        }

        return due;
    }

    /// <summary>Due pending intents whose key routes to <paramref name="partitionId"/>. When no partition resolver
    /// is attached (the in-memory/test configuration), every due intent is returned.</summary>
    public IReadOnlyList<PreparedIntent> DueForRecovery(HLCTimestamp now, int partitionId)
    {
        Func<string, int>? resolver = resolvePartition;
        List<PreparedIntent> due = [];
        foreach (PreparedIntent intent in intents.Values)
        {
            if (!intent.IsPending || intent.RecoveryDeadline == HLCTimestamp.Zero || intent.RecoveryDeadline > now)
                continue;
            if (resolver is not null && resolver(intent.Key) != partitionId)
                continue;
            due.Add(intent);
        }

        return due;
    }

    public int Count => intents.Count;

    private static string KeyOf(PreparedIntentCommand command) => command switch
    {
        PrepareIntentCommand prepare => prepare.Intent.Key,
        ResolveIntentCommand resolve => resolve.Key,
        RemoveIntentCommand remove => remove.Key,
        _ => throw new ArgumentOutOfRangeException(nameof(command), command.GetType().Name, "unknown prepared-intent command")
    };

    // ── replication ─────────────────────────────────────────────────────────────

    public bool Restore(int partitionId, RaftLog log) => ApplyLog(log);

    public bool Replicate(int partitionId, RaftLog log) => ApplyLog(log);

    private bool ApplyLog(RaftLog log)
    {
        if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is null)
            return true;

        PreparedIntentDeltaMessage delta = ReplicationSerializer.UnserializePreparedIntentDeltaMessage(log.LogData);

        foreach (PreparedIntentCommandMessage message in delta.Commands)
            Apply(ToCommand(message));

        return true;
    }

    /// <summary>Serializes a batch of transitions for one atomic data-partition log entry.</summary>
    public static byte[] SerializeDelta(IEnumerable<PreparedIntentCommand> commands)
    {
        PreparedIntentDeltaMessage delta = new();
        foreach (PreparedIntentCommand command in commands)
            delta.Commands.Add(ToProto(command));

        return ReplicationSerializer.Serialize(delta);
    }

    private static PreparedIntentCommandMessage ToProto(PreparedIntentCommand command)
    {
        switch (command)
        {
            case PrepareIntentCommand prepare:
                return PrepareProtoOf(prepare.Intent);

            case ResolveIntentCommand resolve:
                return new()
                {
                    Kind = PreparedIntentCommandKindMessage.PreparedIntentResolve,
                    TransactionIdNode = resolve.TransactionId.N, TransactionIdPhysical = resolve.TransactionId.L, TransactionIdCounter = resolve.TransactionId.C,
                    Epoch = resolve.Epoch, Key = resolve.Key, Commit = resolve.Commit
                };

            case RemoveIntentCommand remove:
                return new()
                {
                    Kind = PreparedIntentCommandKindMessage.PreparedIntentRemove,
                    TransactionIdNode = remove.TransactionId.N, TransactionIdPhysical = remove.TransactionId.L, TransactionIdCounter = remove.TransactionId.C,
                    Epoch = remove.Epoch, Key = remove.Key
                };

            default:
                throw new ArgumentOutOfRangeException(nameof(command), command.GetType().Name, "unknown prepared-intent command");
        }
    }

    private static PreparedIntentCommand ToCommand(PreparedIntentCommandMessage m)
    {
        HLCTimestamp txId = new(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter);

        switch (m.Kind)
        {
            case PreparedIntentCommandKindMessage.PreparedIntentPrepare:
                return new PrepareIntentCommand(IntentOf(m));

            case PreparedIntentCommandKindMessage.PreparedIntentResolve:
                return new ResolveIntentCommand(txId, m.Epoch, m.Key, m.Commit);

            case PreparedIntentCommandKindMessage.PreparedIntentRemove:
                return new RemoveIntentCommand(txId, m.Epoch, m.Key);

            default:
                throw new ArgumentOutOfRangeException(nameof(m), m.Kind, "unknown prepared-intent command kind");
        }
    }

    // A prepared intent maps to (and from) a PREPARE-kind command message, which carries every intent field plus
    // its resolution — reused for both delta commands and full snapshot entries.
    private static PreparedIntentCommandMessage PrepareProtoOf(PreparedIntent i) => new()
    {
        Kind = PreparedIntentCommandKindMessage.PreparedIntentPrepare,
        TransactionIdNode = i.TransactionId.N, TransactionIdPhysical = i.TransactionId.L, TransactionIdCounter = i.TransactionId.C,
        Epoch = i.Epoch, Key = i.Key,
        ManifestHash = i.ManifestHash, RecordAnchorKey = i.RecordAnchorKey,
        CommitTimestampNode = i.CommitTimestamp.N, CommitTimestampPhysical = i.CommitTimestamp.L, CommitTimestampCounter = i.CommitTimestamp.C,
        State = (int)i.State,
        Value = i.Value is null ? ByteString.Empty : ByteString.CopyFrom(i.Value), ValueNull = i.Value is null,
        Bucket = i.Bucket ?? string.Empty, BucketNull = i.Bucket is null,
        Revision = i.Revision,
        ExpiresNode = i.Expires.N, ExpiresPhysical = i.Expires.L, ExpiresCounter = i.Expires.C,
        NoRevision = i.NoRevision,
        BaseRevision = i.BaseRevision, BaseState = (int)i.BaseState,
        RecoveryDeadlineNode = i.RecoveryDeadline.N, RecoveryDeadlinePhysical = i.RecoveryDeadline.L, RecoveryDeadlineCounter = i.RecoveryDeadline.C,
        Resolution = (int)i.Resolution
    };

    private static PreparedIntent IntentOf(PreparedIntentCommandMessage m) => new(
        new HLCTimestamp(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter),
        m.Epoch, m.Key, m.ManifestHash, m.RecordAnchorKey,
        new HLCTimestamp(m.CommitTimestampNode, m.CommitTimestampPhysical, m.CommitTimestampCounter),
        (KeyValueState)m.State,
        m.ValueNull ? null : m.Value.ToByteArray(),
        m.BucketNull ? null : m.Bucket,
        m.Revision,
        new HLCTimestamp(m.ExpiresNode, m.ExpiresPhysical, m.ExpiresCounter),
        m.NoRevision,
        m.BaseRevision, (KeyValueState)m.BaseState,
        new HLCTimestamp(m.RecoveryDeadlineNode, m.RecoveryDeadlinePhysical, m.RecoveryDeadlineCounter),
        (PreparedIntentResolution)m.Resolution);

    // ── durable snapshot ──────────────────────────────────────────────────────────

    /// <summary>Atomically rewrites this partition's on-disk intent snapshot. Returns true (durable) so the WAL
    /// checkpoint may discard the covered tail; false on write failure gates the checkpoint. No-op (true) when
    /// persistence or the resolver is not configured.</summary>
    public bool PersistSnapshot(int partitionId)
    {
        if (snapshotDirectory is null || snapshotPrefix is null || resolvePartition is null)
            return true;

        string path = Path.Combine(snapshotDirectory, $"{snapshotPrefix}_p{partitionId}.snapshot");

        try
        {
            PreparedIntentSnapshotMessage message = new();
            foreach (PreparedIntent intent in intents.Values)
            {
                if (resolvePartition(intent.Key) == partitionId)
                    message.Intents.Add(PrepareProtoOf(intent));
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
            logger?.LogError(ex, "Failed to persist prepared-intent snapshot to {Path}", path);
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
                throw new IOException($"Failed to read prepared-intent snapshot {path}; refusing to start with a possibly incomplete intent set", ex);
            }

            PreparedIntentSnapshotMessage message;
            try
            {
                message = ReplicationSerializer.UnserializePreparedIntentSnapshotMessage(data);
            }
            catch (Exception ex)
            {
                throw new InvalidDataException($"Corrupt prepared-intent snapshot {path}; refusing to start empty and lose a prepared intent", ex);
            }

            foreach (PreparedIntentCommandMessage entry in message.Intents)
                MergeLoad(IntentOf(entry));
        }
    }

    // Load-time merge across (possibly overlapping) per-partition files: at most one live intent per key. A more
    // resolved intent for the same identity is authoritative; a different transaction on the same key is a
    // conflict that keeps the existing intent and is logged rather than silently overwritten.
    private void MergeLoad(PreparedIntent incoming)
    {
        if (!intents.TryGetValue(incoming.Key, out PreparedIntent? existing))
        {
            intents[incoming.Key] = incoming;
            return;
        }

        if (existing.TransactionId != incoming.TransactionId || existing.Epoch != incoming.Epoch)
        {
            logger?.LogError("Conflicting prepared intents on load for key {Key}: {A} vs {B}",
                incoming.Key, existing.TransactionId, incoming.TransactionId);
            return;
        }

        if (existing.IsPending && incoming.IsResolved)
            intents[incoming.Key] = incoming;
    }

    // ── state transfer (split/merge) ────────────────────────────────────────────────

    /// <summary>Intents whose key routes into <c>[startKey, endKey)</c> (ordinal, half-open) — the set a range
    /// split/merge hands to the destination partition.</summary>
    public IReadOnlyList<PreparedIntent> SnapshotRange(string? startKey, string? endKey)
    {
        List<PreparedIntent> result = [];
        foreach (PreparedIntent intent in intents.Values)
        {
            if (startKey is not null && string.CompareOrdinal(intent.Key, startKey) < 0)
                continue;
            if (endKey is not null && string.CompareOrdinal(intent.Key, endKey) >= 0)
                continue;
            result.Add(intent);
        }

        return result;
    }

    /// <summary>Folds transferred intents into this partition's set (idempotent by key + resolution authority).</summary>
    public void ImportIntents(IEnumerable<PreparedIntent> incoming)
    {
        foreach (PreparedIntent intent in incoming)
            MergeLoad(intent);
    }

    public static byte[] SerializeIntents(IEnumerable<PreparedIntent> intents)
    {
        PreparedIntentSnapshotMessage message = new();
        foreach (PreparedIntent intent in intents)
            message.Intents.Add(PrepareProtoOf(intent));

        return ReplicationSerializer.Serialize(message);
    }

    public static IReadOnlyList<PreparedIntent> DeserializeIntents(byte[] data)
    {
        PreparedIntentSnapshotMessage message = ReplicationSerializer.UnserializePreparedIntentSnapshotMessage(data);
        List<PreparedIntent> result = new(message.Intents.Count);
        foreach (PreparedIntentCommandMessage entry in message.Intents)
            result.Add(IntentOf(entry));

        return result;
    }
}

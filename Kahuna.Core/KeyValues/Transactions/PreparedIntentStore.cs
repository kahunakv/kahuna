
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
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

    // Monotonic change stamp for the intent set, bumped whenever an intent is installed, updated, or removed. A
    // partition's checkpoint snapshot is skipped when the set hasn't changed since that partition's last durable
    // write — the file on disk already reflects exactly this content — which turns the common quiet checkpoint
    // from a full scan + serialize + rewrite into a counter comparison. Intents only change partitions through
    // replicated split/merge transfer deltas, which apply here and bump the stamp, so a skip can never hide a
    // moved intent.
    private long version;

    // Per-partition value of <see cref="version"/> captured just before that partition's last successful
    // snapshot write. Captured before the scan, so a mutation racing the scan leaves the stamp ahead and the
    // next checkpoint rewrites the file.
    private readonly ConcurrentDictionary<int, long> persistedVersion = new();

    private readonly string? snapshotDirectory;

    private readonly string? snapshotPrefix;

    private readonly ILogger<IKahuna>? logger;

    private readonly object fileLock = new();

    // Serializes the read-decide-write of a single transition so the compare-and-set is atomic. The concurrent map
    // makes each dictionary operation safe, but not the read-then-write pair: Raft-ordered apply, follower apply,
    // the recovery sweep, and producer-side apply can all call Apply concurrently, and two interleaved transitions
    // on the same key would otherwise both observe the pre-state and clobber each other.
    private readonly object applyGate = new();

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

        lock (applyGate)
        {
            intents.TryGetValue(key, out PreparedIntent? existing);

            PreparedIntentApplyResult result = PreparedIntentStateMachine.Apply(existing, command);

            if (result.Outcome == TransactionApplyOutcome.Applied)
            {
                if (result.Intent is null)
                {
                    intents.TryRemove(key, out _);
                    if (existing is not null)
                        Interlocked.Add(ref totalBytes, -IntentBytes(existing));
                }
                else
                {
                    intents[key] = result.Intent;
                    Interlocked.Add(ref totalBytes, IntentBytes(result.Intent) - (existing is null ? 0 : IntentBytes(existing)));
                }

                Interlocked.Increment(ref version);
            }

            return result;
        }
    }

    /// <summary>The current intent at <paramref name="key"/>, or null. The emptiness pre-check is
    /// deliberate: this runs on every point read/write in the actor hot path, and on workloads with
    /// no durable transactions it skips hashing the key into the shared map entirely (an empty map
    /// can only ever answer null).</summary>
    public PreparedIntent? Get(string key) =>
        !intents.IsEmpty && intents.TryGetValue(key, out PreparedIntent? intent) ? intent : null;

    /// <summary>The intent at <paramref name="key"/> only when it belongs to the given transaction attempt.</summary>
    public PreparedIntent? GetByIdentity(HLCTimestamp transactionId, long epoch, string key) =>
        !intents.IsEmpty
        && intents.TryGetValue(key, out PreparedIntent? intent)
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

    // Running sum of resident intent value bytes, maintained on every install/update/remove across Apply and the
    // load/import merge, so durable admission can bound resident prepared-intent memory without an O(n) scan.
    private long totalBytes;

    /// <summary>Resident prepared-intent value bytes across this node (durable admission bound / observability).</summary>
    public long TotalBytes => Interlocked.Read(ref totalBytes);

    private static long IntentBytes(PreparedIntent intent) => intent.Value?.Length ?? 0;

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

    // The node that proposes a delta serialized it from live command objects moments before Raft hands the very
    // same byte array back to the local apply path, so the decoded form is registered against the produced bytes
    // and the local apply reuses it instead of parsing and rebuilding every command. The table is weak on the
    // byte array, so an entry vanishes with the proposal bytes; any reader that misses — a follower, WAL replay
    // on restart, state transfer, all of which see freshly materialized arrays — parses as before, so
    // correctness never depends on a hit. Commands and their intents are immutable, which is what makes reusing
    // the producer's instances safe even when an in-process transport shares the array across nodes.
    private static readonly ConditionalWeakTable<byte[], PreparedIntentCommand[]> locallyProposedDeltas = new();

    // A hit is taken single-shot: with the redundant-apply ledger only one local apply runs per committed entry,
    // and clearing on take keeps the WAL's in-memory retention of the bytes from pinning the decoded commands.
    // A concurrent second taker simply misses and parses, which is the same double work it did before.
    private static bool TryTakeLocallyProposed(byte[] data, [NotNullWhen(true)] out PreparedIntentCommand[]? commands)
    {
        if (!locallyProposedDeltas.TryGetValue(data, out commands))
            return false;

        locallyProposedDeltas.Remove(data);
        return true;
    }

    /// <summary>Applies a delta and reports whether every PREPARE command in it took ownership of its key. Returns
    /// <see langword="false"/> when any prepare is rejected by the state machine — another transaction already
    /// holds the key, or the same identity re-prepared a divergent mutation — which is NOT an acknowledged
    /// prepare: the producer must abort rather than commit a mutation whose recoverable intent it never owned.
    /// Resolve/remove deltas carry no prepares, so they always report true.</summary>
    public bool ApplyDeltaAckPrepares(RaftLog log)
    {
        if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is null)
            return true;

        bool allPreparesAccepted = true;

        if (TryTakeLocallyProposed(log.LogData, out PreparedIntentCommand[]? proposed))
        {
            foreach (PreparedIntentCommand command in proposed)
            {
                PreparedIntentApplyResult result = Apply(command);
                if (command is PrepareIntentCommand && result.Outcome == TransactionApplyOutcome.Rejected)
                    allPreparesAccepted = false;
            }

            return allPreparesAccepted;
        }

        PreparedIntentDeltaMessage delta = ReplicationSerializer.UnserializePreparedIntentDeltaMessage(log.LogData);

        foreach (PreparedIntentCommandMessage message in delta.Commands)
        {
            PreparedIntentCommand command = ToCommand(message, delta.Header);
            PreparedIntentApplyResult result = Apply(command);
            if (command is PrepareIntentCommand && result.Outcome == TransactionApplyOutcome.Rejected)
                allPreparesAccepted = false;
        }

        return allPreparesAccepted;
    }

    private bool ApplyLog(RaftLog log)
    {
        if (log.LogType != ReplicationTypes.PreparedIntent || log.LogData is null)
            return true;

        if (TryTakeLocallyProposed(log.LogData, out PreparedIntentCommand[]? proposed))
        {
            foreach (PreparedIntentCommand command in proposed)
                Apply(command);

            return true;
        }

        PreparedIntentDeltaMessage delta = ReplicationSerializer.UnserializePreparedIntentDeltaMessage(log.LogData);

        foreach (PreparedIntentCommandMessage message in delta.Commands)
            Apply(ToCommand(message, delta.Header));

        return true;
    }

    /// <summary>Serializes a batch of transitions for one atomic data-partition log entry. The produced bytes
    /// remember their decoded commands so the local apply of this same entry can skip re-parsing them.</summary>
    public static byte[] SerializeDelta(IEnumerable<PreparedIntentCommand> commands)
    {
        PreparedIntentCommand[] batch = commands as PreparedIntentCommand[] ?? [.. commands];

        PreparedIntentDeltaMessage delta = new();

        foreach (PreparedIntentCommand command in batch)
            delta.Commands.Add(ToProto(command));

        HoistSharedHeader(delta);

        byte[] data = ReplicationSerializer.Serialize(delta);
        locallyProposedDeltas.AddOrUpdate(data, batch);

        return data;
    }

    /// <summary>
    /// Moves the fields every transition in the delta agrees on into one delta-level header and clears them from the
    /// commands, so the transaction identity, manifest hash, commit timestamp, recovery deadline and record-anchor
    /// key are encoded once for the batch instead of once per key. A cleared field is at its proto3 default and so is
    /// not written at all. Does nothing when the commands disagree, which keeps the per-command form correct for any
    /// delta this can't compact.
    /// </summary>
    private static void HoistSharedHeader(PreparedIntentDeltaMessage delta)
    {
        if (delta.Commands.Count < 2)
            return;

        PreparedIntentCommandMessage first = delta.Commands[0];

        // Every command's identity must agree. The mutation fields only exist on PREPARE commands, so they need only
        // agree among those — a resolve/remove never reads them back.
        PreparedIntentCommandMessage? firstPrepare = null;

        foreach (PreparedIntentCommandMessage command in delta.Commands)
        {
            if (command.TransactionIdNode != first.TransactionIdNode
                || command.TransactionIdPhysical != first.TransactionIdPhysical
                || command.TransactionIdCounter != first.TransactionIdCounter
                || command.Epoch != first.Epoch)
                return;

            if (command.Kind != PreparedIntentCommandKindMessage.PreparedIntentPrepare)
                continue;

            if (firstPrepare is null)
            {
                firstPrepare = command;
                continue;
            }

            if (command.ManifestHash != firstPrepare.ManifestHash
                || !string.Equals(command.RecordAnchorKey, firstPrepare.RecordAnchorKey, StringComparison.Ordinal)
                || command.CommitTimestampNode != firstPrepare.CommitTimestampNode
                || command.CommitTimestampPhysical != firstPrepare.CommitTimestampPhysical
                || command.CommitTimestampCounter != firstPrepare.CommitTimestampCounter
                || command.RecoveryDeadlineNode != firstPrepare.RecoveryDeadlineNode
                || command.RecoveryDeadlinePhysical != firstPrepare.RecoveryDeadlinePhysical
                || command.RecoveryDeadlineCounter != firstPrepare.RecoveryDeadlineCounter)
                return;
        }

        delta.Header = new()
        {
            TransactionIdNode = first.TransactionIdNode,
            TransactionIdPhysical = first.TransactionIdPhysical,
            TransactionIdCounter = first.TransactionIdCounter,
            Epoch = first.Epoch,
            ManifestHash = firstPrepare?.ManifestHash ?? 0,
            RecordAnchorKey = firstPrepare?.RecordAnchorKey ?? string.Empty,
            CommitTimestampNode = firstPrepare?.CommitTimestampNode ?? 0,
            CommitTimestampPhysical = firstPrepare?.CommitTimestampPhysical ?? 0,
            CommitTimestampCounter = firstPrepare?.CommitTimestampCounter ?? 0,
            RecoveryDeadlineNode = firstPrepare?.RecoveryDeadlineNode ?? 0,
            RecoveryDeadlinePhysical = firstPrepare?.RecoveryDeadlinePhysical ?? 0,
            RecoveryDeadlineCounter = firstPrepare?.RecoveryDeadlineCounter ?? 0
        };

        foreach (PreparedIntentCommandMessage command in delta.Commands)
        {
            command.TransactionIdNode = 0;
            command.TransactionIdPhysical = 0;
            command.TransactionIdCounter = 0;
            command.Epoch = 0;

            if (command.Kind != PreparedIntentCommandKindMessage.PreparedIntentPrepare)
                continue;

            command.ManifestHash = 0;
            command.RecordAnchorKey = string.Empty;
            command.CommitTimestampNode = 0;
            command.CommitTimestampPhysical = 0;
            command.CommitTimestampCounter = 0;
            command.RecoveryDeadlineNode = 0;
            command.RecoveryDeadlinePhysical = 0;
            command.RecoveryDeadlineCounter = 0;
        }
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

    /// <summary>Rebuilds one transition, taking the fields the batch shares from <paramref name="header"/> when the
    /// writer hoisted them there and from the command itself when it did not.</summary>
    private static PreparedIntentCommand ToCommand(PreparedIntentCommandMessage m, PreparedIntentDeltaHeaderMessage? header)
    {
        HLCTimestamp txId = header is null
            ? new(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter)
            : new(header.TransactionIdNode, header.TransactionIdPhysical, header.TransactionIdCounter);

        long epoch = header?.Epoch ?? m.Epoch;

        switch (m.Kind)
        {
            case PreparedIntentCommandKindMessage.PreparedIntentPrepare:
                return new PrepareIntentCommand(IntentOf(m, header));

            case PreparedIntentCommandKindMessage.PreparedIntentResolve:
                return new ResolveIntentCommand(txId, epoch, m.Key, m.Commit);

            case PreparedIntentCommandKindMessage.PreparedIntentRemove:
                return new RemoveIntentCommand(txId, epoch, m.Key);

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
        // Wrap the intent's value array without copying: the committed value is immutable and the message is
        // serialized synchronously by the caller before the array could change, so aliasing it here is safe and
        // avoids a full value copy per intent on every prepare/settle serialization.
        Value = i.Value is null ? ByteString.Empty : UnsafeByteOperations.UnsafeWrap(i.Value), ValueNull = i.Value is null,
        Bucket = i.Bucket ?? string.Empty, BucketNull = i.Bucket is null,
        Revision = i.Revision,
        ExpiresNode = i.Expires.N, ExpiresPhysical = i.Expires.L, ExpiresCounter = i.Expires.C,
        NoRevision = i.NoRevision,
        BaseRevision = i.BaseRevision, BaseState = (int)i.BaseState,
        RecoveryDeadlineNode = i.RecoveryDeadline.N, RecoveryDeadlinePhysical = i.RecoveryDeadline.L, RecoveryDeadlineCounter = i.RecoveryDeadline.C,
        Resolution = (int)i.Resolution
    };

    private static PreparedIntent IntentOf(PreparedIntentCommandMessage m, PreparedIntentDeltaHeaderMessage? header = null) => new(
        header is null
            ? new HLCTimestamp(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter)
            : new HLCTimestamp(header.TransactionIdNode, header.TransactionIdPhysical, header.TransactionIdCounter),
        header?.Epoch ?? m.Epoch, m.Key, header?.ManifestHash ?? m.ManifestHash, header?.RecordAnchorKey ?? m.RecordAnchorKey,
        header is null
            ? new HLCTimestamp(m.CommitTimestampNode, m.CommitTimestampPhysical, m.CommitTimestampCounter)
            : new HLCTimestamp(header.CommitTimestampNode, header.CommitTimestampPhysical, header.CommitTimestampCounter),
        (KeyValueState)m.State,
        m.ValueNull ? null : m.Value.ToByteArray(),
        m.BucketNull ? null : m.Bucket,
        m.Revision,
        new HLCTimestamp(m.ExpiresNode, m.ExpiresPhysical, m.ExpiresCounter),
        m.NoRevision,
        m.BaseRevision, (KeyValueState)m.BaseState,
        header is null
            ? new HLCTimestamp(m.RecoveryDeadlineNode, m.RecoveryDeadlinePhysical, m.RecoveryDeadlineCounter)
            : new HLCTimestamp(header.RecoveryDeadlineNode, header.RecoveryDeadlinePhysical, header.RecoveryDeadlineCounter),
        (PreparedIntentResolution)m.Resolution);

    // ── durable snapshot ──────────────────────────────────────────────────────────

    /// <summary>Atomically rewrites this partition's on-disk intent snapshot. Returns true (durable) so the WAL
    /// checkpoint may discard the covered tail; false on write failure gates the checkpoint. No-op (true) when
    /// persistence or the resolver is not configured.</summary>
    public bool PersistSnapshot(int partitionId)
    {
        if (snapshotDirectory is null || snapshotPrefix is null || resolvePartition is null)
            return true;

        // Unchanged since this partition's last durable write: the file already holds exactly this content, so
        // the checkpoint may proceed without scanning or rewriting anything. The stamp is captured before the
        // scan and recorded only after a successful write, so a failed write or a mutation racing the scan
        // always leaves the partition due for a rewrite.
        long observedVersion = Interlocked.Read(ref version);
        if (persistedVersion.TryGetValue(partitionId, out long lastPersisted) && lastPersisted == observedVersion)
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

            persistedVersion[partitionId] = observedVersion;
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
            Interlocked.Add(ref totalBytes, IntentBytes(incoming));
            Interlocked.Increment(ref version);
            return;
        }

        if (existing.TransactionId != incoming.TransactionId || existing.Epoch != incoming.Epoch)
        {
            logger?.LogError("Conflicting prepared intents on load for key {Key}: {A} vs {B}",
                incoming.Key, existing.TransactionId, incoming.TransactionId);
            return;
        }

        if (existing.IsPending && incoming.IsResolved)
        {
            intents[incoming.Key] = incoming;
            Interlocked.Add(ref totalBytes, IntentBytes(incoming) - IntentBytes(existing));
            Interlocked.Increment(ref version);
        }
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

    /// <summary>Intents whose key belongs to <paramref name="bucket"/> (its parent prefix) — the set a bucket scan
    /// (<c>GetByBucket</c>) reconciles against. Uses the intent's own bucket, which the freeze sources from the key
    /// (its parent prefix), so an intent-only committed key is included/overridden/excluded in a bucket scan exactly
    /// as it would be in the equivalent range scan.</summary>
    public IReadOnlyList<PreparedIntent> SnapshotBucket(string? bucket)
    {
        List<PreparedIntent> result = [];

        foreach (PreparedIntent intent in intents.Values)
        {
            if (string.Equals(intent.Bucket, bucket, StringComparison.Ordinal))
                result.Add(intent);
        }

        return result;
    }

    /// <summary>Intents covering a scan page's window with the scan's own boundary semantics: start-exclusivity for a
    /// continuation cursor (<paramref name="startInclusive"/> false skips an intent exactly at <paramref name="startKey"/>,
    /// which the prior page already emitted) and end-inclusivity (<paramref name="endInclusive"/> true keeps an intent
    /// exactly at <paramref name="endKey"/>). This is distinct from <see cref="SnapshotRange"/>'s fixed half-open
    /// <c>[start, end)</c> so the overlaid intent set matches exactly the window the scan's KV rows were drawn from —
    /// without it a boundary intent is re-emitted across pages or an inclusive-end intent is missed.</summary>
    public IReadOnlyList<PreparedIntent> SnapshotScanWindow(string? startKey, bool startInclusive, string? endKey, bool endInclusive)
    {
        List<PreparedIntent> result = [];

        foreach (PreparedIntent intent in intents.Values)
        {
            if (startKey is not null)
            {
                int cmpStart = string.CompareOrdinal(intent.Key, startKey);
                if (cmpStart < 0 || (!startInclusive && cmpStart == 0))
                    continue;
            }

            if (endKey is not null)
            {
                int cmpEnd = string.CompareOrdinal(intent.Key, endKey);
                if (cmpEnd > 0 || (!endInclusive && cmpEnd == 0))
                    continue;
            }

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

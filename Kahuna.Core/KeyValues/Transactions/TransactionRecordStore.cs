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

    // Monotonic tick source for the dirty stamps below: each mutation mints one tick, so stamps taken
    // from it order mutations against the pre-scan capture in <see cref="PersistSnapshot"/>.
    private long version;

    // Per-partition dirty stamp: the tick of the last mutation whose anchor routed to that partition. A
    // partition's checkpoint snapshot is skipped when neither its stamp, nor <see cref="allPartitionsVersion"/>,
    // nor the routing stamp moved since its last durable write — the file already holds exactly this content —
    // which turns the common quiet checkpoint from a full scan + serialize + rewrite into a counter comparison.
    // With the previous single global stamp, one record change anywhere re-dirtied every partition, so a busy
    // checkpoint rescanned the whole set once per partition.
    private readonly ConcurrentDictionary<int, long> partitionVersion = new();

    // Tick of the last mutation that could not be attributed to one partition: no resolver attached yet
    // (load-time merges), a removal whose anchor is unknown, or a bulk sweep. Dirties every partition;
    // over-dirty is safe.
    private long allPartitionsVersion;

    // Stamps captured just before that partition's last successful snapshot write: the mutation tick it
    // covered and the routing stamp it routed with. Captured before the scan, so a mutation or a routing
    // change racing the scan leaves a stamp ahead and the next checkpoint rewrites the file. The routing
    // stamp is what keeps a skip from hiding a record that silently moved partitions when the range map
    // changed: until every partition has re-persisted under the new routing, none of them compares equal.
    private readonly ConcurrentDictionary<int, PersistedStamp> persistedVersion = new();

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

    /// <summary>Wires the anchor-key → data-partition resolver once the locator exists (manager construction).
    /// <paramref name="routingVersion"/> reports a stamp that changes whenever the resolver's routing may have
    /// changed (the range-map version); null means the routing is fixed for the store's lifetime.</summary>
    public void AttachAnchorResolver(Func<string, (int PartitionId, long Generation)> resolver, Func<long>? routingVersion = null)
    {
        resolveAnchorPartition = resolver;
        this.routingVersion = routingVersion;
    }

    // Monotonic stamp of the routing the resolver reads (RangeMapStore.MapVersion), or null when routing is
    // fixed for the store's lifetime (tests, memory-only configuration). Pulled by the checkpoint guard.
    private Func<long>? routingVersion;

    // Marks the partition owning <paramref name="anchorKey"/> dirty for the checkpoint guard. Mutators call
    // this after the dictionary write, so a stamp equal to a pre-scan capture implies the scan saw the
    // mutation. Without a resolver, or without a known anchor, the mutation cannot be attributed, so every
    // partition is marked instead.
    //
    // This runs on the replicated apply/restore path, so it must never throw: routing can legitimately fail
    // there (a restart replays data-partition entries before the meta partition has rebuilt the range map,
    // and the resolver throws on an uncovered key-range key). An unattributable mutation falls back to the
    // all-partitions stamp — over-dirty is safe, a failed apply is not.
    private void StampDirty(string? anchorKey)
    {
        long tick = Interlocked.Increment(ref version);

        Func<string, (int PartitionId, long Generation)>? resolver = resolveAnchorPartition;
        if (resolver is not null && anchorKey is not null)
        {
            try
            {
                partitionVersion.AddOrUpdate(resolver(anchorKey).PartitionId, static (_, t) => t, static (_, prev, t) => Math.Max(prev, t), tick);
                return;
            }
            catch
            {
                // Fall through to the all-partitions stamp.
            }
        }

        StampMax(ref allPartitionsVersion, tick);
    }

    // Marks every partition dirty: for bulk sweeps whose per-key attribution would cost more than the
    // over-inclusive rewrite it avoids.
    private void StampAllDirty() => StampMax(ref allPartitionsVersion, Interlocked.Increment(ref version));

    // Monotonic max-write: a stamp must never move backward, or two racing mutators could leave the stamp
    // equal to a checkpoint's pre-scan capture while the later mutation was missed by its scan.
    private static void StampMax(ref long location, long tick)
    {
        long observed = Interlocked.Read(ref location);
        while (tick > observed)
        {
            long prior = Interlocked.CompareExchange(ref location, tick, observed);
            if (prior == observed)
                return;
            observed = prior;
        }
    }

    // The pair of stamps a partition's snapshot file was written under: the mutation tick it covered and the
    // routing stamp it routed with. A checkpoint skips the rewrite only when both still match.
    private readonly record struct PersistedStamp(long Version, long RoutingVersion);

    /// <summary>The slice key used when an apply cannot be attributed to a partition's log (the in-memory
    /// configuration of the unit tests); mirrors <see cref="PreparedIntentStore.UnattributedPartition"/> so the
    /// gate and the ledger it judges against agree on the slice.</summary>
    internal const int UnattributedPartition = PreparedIntentStore.UnattributedPartition;

    /// <summary>Judges a one-phase bundled commit at its apply position on a partition's log: whether this
    /// transaction's live intent holds every bundled key, and — when the command asks for apply-time
    /// validation — whether every co-bundled validated base and carried read dependency still holds against the
    /// partition's committed-head ledger. Wired to <see cref="PreparedIntentStore.JudgeBundledCommit"/>.</summary>
    internal delegate BundledCommitJudgement BundledCommitJudge(int partitionId, CommitTransactionCommand commit);

    // Consulted by the bundled commit gate below. Reading the intent store here is deterministic: both stores
    // apply on the same ordered per-partition path, so at the log position where a commit transition applies,
    // the bundled prepare (earlier in the same atomic batch) has already been applied or rejected identically
    // on every replica, and the partition's ledger slice holds exactly the heads that log prefix fed.
    private BundledCommitJudge? bundledCommitJudge;

    /// <summary>Wires the prepared-intent judge used to validate one-phase bundled commits (manager
    /// construction). A gated commit applying without a judge attached is rejected — the store fails closed
    /// rather than durably committing a mutation whose prepare it cannot verify.</summary>
    public void AttachBundledCommitJudge(BundledCommitJudge judge) => bundledCommitJudge = judge;

    // Leader-local, advisory: the verdict kind of each bundled commit this node's apply rejected, keyed by
    // the attempt's operation id, for the finalizer that proposed it to classify its outcome (a stale base or
    // read is a truthful conflict abort; a missing prepare is a retry through the two-phase path). Taken once
    // by that finalizer; entries no finalizer ever takes (follower applies) are bounded by the cap.
    private readonly ConcurrentDictionary<(HLCTimestamp TransactionId, long Epoch, HLCTimestamp OpId), BundledCommitVerdict> gatedRejectionVerdicts = new();

    private const int GatedRejectionVerdictsMax = 4_096;

    /// <summary>Takes (and forgets) the verdict kind under which this node's apply rejected the bundled commit
    /// attempt <paramref name="opId"/> of the transaction, or false when this node recorded none — the deadline
    /// gate, not the bundled commit gate, kept the record Undecided.</summary>
    internal bool TryTakeGatedRejectionVerdict(HLCTimestamp transactionId, long epoch, HLCTimestamp opId, out BundledCommitVerdict verdict) =>
        gatedRejectionVerdicts.TryRemove((transactionId, epoch, opId), out verdict);

    /// <summary>Applies one transition to the record it targets and reflects the result in the map. This
    /// overload cannot attribute the apply to a partition's log, so a bundled commit gate it runs judges against
    /// the <see cref="UnattributedPartition"/> ledger slice; production applies go through
    /// <see cref="Replicate"/>/<see cref="Restore"/>, which carry the log's partition.</summary>
    public TransactionRecordApplyResult Apply(TransactionRecordCommand command) => Apply(command, UnattributedPartition);

    /// <summary>Applies one transition that arrived through <paramref name="partitionId"/>'s log and reflects
    /// the result in the map. This is the single apply entry point shared by local proposal apply, follower
    /// replication, and restore.</summary>
    public TransactionRecordApplyResult Apply(TransactionRecordCommand command, int partitionId)
    {
        (HLCTimestamp, long) key = KeyOf(command);

        lock (applyGate)
        {
            records.TryGetValue(key, out TransactionRecord? existing);

            // One-phase bundled commit: the decision shared an atomic batch with its own prepared-intent group
            // and could not be withheld when that prepare was rejected, so its legality is decided here instead —
            // the Undecided → Commit transition applies only if this transaction's live intent holds every bundled
            // key, and (when the command asks for apply-time validation) only if every co-bundled validated base
            // and carried read dependency still holds against the partition's committed-head ledger. A bundle
            // surfacing late (a stalled proposal committing after a partition heals) whose keys were meanwhile
            // taken, or whose base or read a competitor's settled commit moved past, is rejected: the record stays
            // Undecided and the proposing finalizer drives the truthful outcome, instead of durably reporting
            // Commit for a mutation that was never durably prepared or that would discard another writer's
            // commit. Replays against an already-terminal record skip the gate (idempotence is the state
            // machine's to judge; settlement may have removed the intent by then).
            //
            // A rejection is memoed on the record and persisted with it: a replay of the same attempt (WAL
            // restore, or the entries after an installed snapshot's boundary) is rejected by the memo without
            // re-judging, because the ledger it was judged against may since have moved past what the live
            // apply saw, and a replay that re-judged could admit what the live apply refused — forking this
            // replica's record from its peers'.
            if (command is CommitTransactionCommand { BundledPrepareKeys.Count: > 0 } bundledCommit &&
                existing is { Decision: TransactionDecision.Undecided })
            {
                if (existing.WasBundledCommitRejected(bundledCommit.OpId))
                    return new(TransactionApplyOutcome.Rejected, existing, "bundled commit already rejected at its first apply");

                BundledCommitJudgement judgement = bundledCommitJudge is null
                    ? new(BundledCommitVerdict.PrepareMissing, "no bundled commit judge attached")
                    : bundledCommitJudge(partitionId, bundledCommit);

                if (!judgement.IsAdmit)
                {
                    switch (judgement.Verdict)
                    {
                        case BundledCommitVerdict.StaleBase:
                            DurableTransactionMetrics.OnePhaseGatedCommitStaleBaseRejected();
                            break;
                        case BundledCommitVerdict.StaleRead:
                            DurableTransactionMetrics.OnePhaseGatedCommitStaleReadRejected();
                            break;
                        default:
                            DurableTransactionMetrics.OnePhaseGatedCommitRejections.Add(1);
                            break;
                    }

                    List<HLCTimestamp> rejected = existing.RejectedBundledCommitOpIds is { } prior ? [.. prior] : [];
                    rejected.Add(bundledCommit.OpId);
                    TransactionRecord memoed = existing with { RejectedBundledCommitOpIds = rejected };
                    records[key] = memoed;
                    StampDirty(memoed.RecordAnchorKey);

                    if (gatedRejectionVerdicts.Count < GatedRejectionVerdictsMax)
                        gatedRejectionVerdicts[(bundledCommit.TransactionId, bundledCommit.Epoch, bundledCommit.OpId)] = judgement.Verdict;

                    logger?.LogWarning(
                        "Bundled commit of transaction {TransactionId} rejected at apply on partition {PartitionId}: {Reason}",
                        bundledCommit.TransactionId, partitionId, judgement.Reason);

                    return new(TransactionApplyOutcome.Rejected, memoed, judgement.Reason);
                }
            }

            TransactionRecordApplyResult result = TransactionRecordStateMachine.Apply(existing, command);

            if (result.Outcome == TransactionApplyOutcome.Applied && result.Record is not null)
            {
                records[key] = result.Record;
                StampDirty(result.Record.RecordAnchorKey);
            }
            else if (result.Outcome == TransactionApplyOutcome.Removed)
            {
                records.TryRemove(key, out TransactionRecord? removed);
                StampDirty(removed?.RecordAnchorKey ?? existing?.RecordAnchorKey);
            }

            return result;
        }
    }

    public TransactionRecord? Get(HLCTimestamp transactionId, long epoch) =>
        records.TryGetValue((transactionId, epoch), out TransactionRecord? record) ? record : null;

    public IReadOnlyCollection<TransactionRecord> Snapshot() => records.Values.ToArray();

    public int Count => records.Count;

    // ── replication ─────────────────────────────────────────────────────────────

    public bool Restore(int partitionId, RaftLog log) => ApplyLog(log, partitionId);

    public bool Replicate(int partitionId, RaftLog log) => ApplyLog(log, partitionId);

    // The proposer's decoded commands, keyed by the exact byte array handed to Raft, budgeted for one
    // take per co-hosted node. See ProposedDeltaCache for the reuse and lifetime contract; reusing the
    // producer's instances is safe only because commands, records, and the participant list are immutable.
    private static readonly ProposedDeltaCache<TransactionRecordCommand> locallyProposedDeltas = new();

    private bool ApplyLog(RaftLog log, int partitionId)
    {
        if (log.LogType != ReplicationTypes.TransactionRecord || log.LogData is null)
            return true;

        if (locallyProposedDeltas.TryTake(log.LogData, out TransactionRecordCommand[]? proposed))
        {
            foreach (TransactionRecordCommand command in proposed)
                Apply(command, partitionId);

            return true;
        }

        TransactionRecordDeltaMessage delta = ReplicationSerializer.UnserializeTransactionRecordDeltaMessage(log.LogData);

        foreach (TransactionRecordCommandMessage message in delta.Commands)
            Apply(ToCommand(message), partitionId);

        return true;
    }

    /// <summary>Serializes a batch of transitions for one atomic data-partition log entry. The produced bytes
    /// remember their decoded commands so the local apply of this same entry can skip re-parsing them.</summary>
    public static byte[] SerializeDelta(IEnumerable<TransactionRecordCommand> commands)
    {
        TransactionRecordCommand[] batch = commands as TransactionRecordCommand[] ?? [.. commands];

        TransactionRecordDeltaMessage delta = new();
        foreach (TransactionRecordCommand command in batch)
            delta.Commands.Add(ToProto(command));

        byte[] data = ReplicationSerializer.Serialize(delta);
        locallyProposedDeltas.Register(data, batch);

        return data;
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
            {
                TransactionRecordCommandMessage m = new()
                {
                    Kind = TransactionRecordCommandKindMessage.TransactionRecordCommit,
                    TransactionIdNode = c.TransactionId.N, TransactionIdPhysical = c.TransactionId.L, TransactionIdCounter = c.TransactionId.C,
                    Epoch = c.Epoch, ManifestHash = c.ManifestHash,
                    OpIdNode = c.OpId.N, OpIdPhysical = c.OpId.L, OpIdCounter = c.OpId.C,
                    AttemptNode = c.AttemptHlc.N, AttemptPhysical = c.AttemptHlc.L, AttemptCounter = c.AttemptHlc.C
                };

                if (c.BundledPrepareKeys is not null)
                    foreach (string bundledKey in c.BundledPrepareKeys)
                        m.BundledPrepareKeys.Add(bundledKey);

                m.ApplyTimeValidation = c.ApplyTimeValidation;

                if (c.BundledReadDependencies is not null)
                    foreach (BundledReadDependency read in c.BundledReadDependencies)
                        m.BundledReadDependencies.Add(new BundledReadDependencyMessage
                        {
                            Key = read.Key,
                            ObservedRevision = read.ObservedRevision,
                            ObservedExists = read.ObservedExists
                        });

                return m;
            }

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
            {
                BundledReadDependency[]? reads = null;
                if (m.BundledReadDependencies.Count > 0)
                {
                    reads = new BundledReadDependency[m.BundledReadDependencies.Count];
                    for (int i = 0; i < reads.Length; i++)
                    {
                        BundledReadDependencyMessage read = m.BundledReadDependencies[i];
                        reads[i] = new BundledReadDependency(read.Key, read.ObservedRevision, read.ObservedExists);
                    }
                }

                return new CommitTransactionCommand(txId, m.Epoch, m.ManifestHash, opId,
                    new HLCTimestamp(m.AttemptNode, m.AttemptPhysical, m.AttemptCounter),
                    m.BundledPrepareKeys.Count > 0 ? m.BundledPrepareKeys.ToArray() : null,
                    m.ApplyTimeValidation,
                    reads);
            }

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

        // Unchanged since this partition's last durable write — no mutation routed here, no bulk sweep, and
        // no routing change: the file already holds exactly this content, so the checkpoint may proceed
        // without scanning or rewriting anything. All three stamps are captured before the scan and recorded
        // only after a successful write, so a failed write, a mutation racing the scan, or a range-map swap
        // racing the scan always leaves the partition due for a rewrite.
        long observedVersion = Math.Max(
            Interlocked.Read(ref allPartitionsVersion),
            partitionVersion.TryGetValue(partitionId, out long dirtyTick) ? dirtyTick : 0);
        long observedRouting = routingVersion?.Invoke() ?? 0;

        if (persistedVersion.TryGetValue(partitionId, out PersistedStamp last)
            && last.Version == observedVersion && last.RoutingVersion == observedRouting)
            return true;

        string path = Path.Combine(snapshotDirectory, $"{snapshotPrefix}_p{partitionId}.snapshot");

        try
        {
            // Entries stream straight into the temp file through one reused entry message (participant
            // sub-messages included), producing the same bytes as serializing a whole
            // TransactionRecordSnapshotMessage: each entry is written length-delimited under the repeated
            // field's tag. Materializing one protobuf object per retained record plus one byte[] for the
            // whole set made every checkpoint's allocation proportional to the store size, which dominated
            // the node's allocation profile whenever the retained set was large.
            lock (fileLock)
            {
                string tmp = path + ".tmp";

                using (FileStream file = new(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 64 * 1024))
                using (CodedOutputStream output = new(file))
                {
                    TransactionRecordSnapshotEntry entry = new();
                    List<TransactionParticipantRefMessage> participantPool = [];

                    foreach (TransactionRecord record in records.Values)
                    {
                        if (resolveAnchorPartition(record.RecordAnchorKey).PartitionId != partitionId)
                            continue;

                        FillSnapshotEntry(entry, participantPool, record);
                        output.WriteTag(TransactionRecordSnapshotMessage.RecordsFieldNumber, WireFormat.WireType.LengthDelimited);
                        output.WriteMessage(entry);
                    }
                }

                File.Move(tmp, path, overwrite: true);
            }

            persistedVersion[partitionId] = new PersistedStamp(observedVersion, observedRouting);
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
            StampDirty(incoming.RecordAnchorKey);
            return;
        }

        if (existing.IsTerminal && incoming.IsTerminal && existing.Decision != incoming.Decision)
        {
            logger?.LogError("Conflicting terminal transaction records on load for {TxId}/{Epoch}: {A} vs {B}",
                incoming.TransactionId, incoming.Epoch, existing.Decision, incoming.Decision);
            return;
        }

        if (!existing.IsTerminal && incoming.IsTerminal)
        {
            records[key] = incoming;
            StampDirty(incoming.RecordAnchorKey);
            return;
        }

        // Both Undecided: keep the union of their rejected-bundled-commit memos, so a rejection recorded in
        // one overlapping per-partition file is not lost to the other file's older copy of the record.
        if (!existing.IsTerminal && incoming.RejectedBundledCommitOpIds is { Count: > 0 } incomingRejected)
        {
            List<HLCTimestamp> merged = existing.RejectedBundledCommitOpIds is { } prior ? [.. prior] : [];
            foreach (HLCTimestamp opId in incomingRejected)
                if (!existing.WasBundledCommitRejected(opId))
                    merged.Add(opId);

            if (merged.Count != (existing.RejectedBundledCommitOpIds?.Count ?? 0))
            {
                records[key] = existing with { RejectedBundledCommitOpIds = merged };
                StampDirty(existing.RecordAnchorKey);
            }
        }
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

    /// <summary>
    /// Drops every record whose anchor satisfies <paramref name="shouldRemoveAnchor"/> and returns how many
    /// were removed. This is the un-host purge: when this node stops being a replica of the anchors'
    /// partition, the canonical decisions live on the partition's replicas (and return in a seeding snapshot
    /// on any re-gain), so the local copies are dead retention. Bumps the change stamp so the next per-
    /// partition snapshot rewrites the emptied slice.
    /// </summary>
    public int PurgeWhere(Func<string, bool> shouldRemoveAnchor)
    {
        lock (applyGate)
        {
            List<(HLCTimestamp TransactionId, long Epoch)>? toRemove = null;

            foreach (KeyValuePair<(HLCTimestamp TransactionId, long Epoch), TransactionRecord> kv in records)
            {
                if (shouldRemoveAnchor(kv.Value.RecordAnchorKey))
                    (toRemove ??= []).Add(kv.Key);
            }

            if (toRemove is null)
                return 0;

            foreach ((HLCTimestamp TransactionId, long Epoch) key in toRemove)
                records.TryRemove(key, out _);

            StampAllDirty();
            return toRemove.Count;
        }
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
        TransactionRecordSnapshotEntry entry = new();
        FillSnapshotEntry(entry, [], r);
        return entry;
    }

    // Fills a possibly-reused entry message from a record. Every field is assigned on every call and the
    // participant list is rebuilt, so no value from a previously filled record can leak into this one.
    // Participant sub-messages come from the caller's pool (grown on demand), so a caller that serializes
    // each filled entry immediately can stream an arbitrarily large record set through one entry object.
    private static void FillSnapshotEntry(
        TransactionRecordSnapshotEntry entry, List<TransactionParticipantRefMessage> participantPool, TransactionRecord r)
    {
        entry.TransactionIdNode = r.TransactionId.N; entry.TransactionIdPhysical = r.TransactionId.L; entry.TransactionIdCounter = r.TransactionId.C;
        entry.Epoch = r.Epoch; entry.CoordinatorKey = r.CoordinatorKey; entry.RecordAnchorKey = r.RecordAnchorKey;
        entry.CommitTimestampNode = r.CommitTimestamp.N; entry.CommitTimestampPhysical = r.CommitTimestamp.L; entry.CommitTimestampCounter = r.CommitTimestamp.C;
        entry.DecisionDeadlineNode = r.DecisionDeadline.N; entry.DecisionDeadlinePhysical = r.DecisionDeadline.L; entry.DecisionDeadlineCounter = r.DecisionDeadline.C;
        entry.ManifestHash = r.ManifestHash; entry.ManifestPresent = r.ManifestPresent;
        entry.Decision = r.Decision switch
        {
            TransactionDecision.Commit => TransactionDecisionMessage.TransactionDecisionCommit,
            TransactionDecision.Abort => TransactionDecisionMessage.TransactionDecisionAbort,
            _ => TransactionDecisionMessage.TransactionDecisionUndecided
        };
        entry.AbortClass = (int)r.AbortClass;
        entry.WinningOpIdNode = r.WinningOpId.N; entry.WinningOpIdPhysical = r.WinningOpId.L; entry.WinningOpIdCounter = r.WinningOpId.C;
        entry.CreatedAtNode = r.CreatedAt.N; entry.CreatedAtPhysical = r.CreatedAt.L; entry.CreatedAtCounter = r.CreatedAt.C;
        entry.DecidedAtNode = r.DecidedAt.N; entry.DecidedAtPhysical = r.DecidedAt.L; entry.DecidedAtCounter = r.DecidedAt.C;

        entry.Participants.Clear();
        int pooled = 0;
        foreach (TransactionParticipantRef p in r.Participants)
        {
            if (pooled >= participantPool.Count)
                participantPool.Add(new TransactionParticipantRefMessage());

            TransactionParticipantRefMessage m = participantPool[pooled++];
            m.Key = p.Key;
            m.Durability = (int)p.Durability;
            entry.Participants.Add(m);
        }

        entry.RejectedBundledCommits.Clear();
        if (r.RejectedBundledCommitOpIds is { Count: > 0 } rejected)
        {
            foreach (HLCTimestamp opId in rejected)
                entry.RejectedBundledCommits.Add(new TransactionRecordRejectedBundledCommitMessage
                {
                    OpIdNode = opId.N, OpIdPhysical = opId.L, OpIdCounter = opId.C
                });
        }
    }

    private static TransactionRecord FromSnapshotEntry(TransactionRecordSnapshotEntry e)
    {
        List<TransactionParticipantRef> participants = new(e.Participants.Count);
        foreach (TransactionParticipantRefMessage p in e.Participants)
            participants.Add(new TransactionParticipantRef(p.Key, (KeyValueDurability)p.Durability));

        List<HLCTimestamp>? rejected = null;
        if (e.RejectedBundledCommits.Count > 0)
        {
            rejected = new(e.RejectedBundledCommits.Count);
            foreach (TransactionRecordRejectedBundledCommitMessage r in e.RejectedBundledCommits)
                rejected.Add(new HLCTimestamp(r.OpIdNode, r.OpIdPhysical, r.OpIdCounter));
        }

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
            new HLCTimestamp(e.DecidedAtNode, e.DecidedAtPhysical, e.DecidedAtCounter))
        {
            RejectedBundledCommitOpIds = rejected
        };
    }
}

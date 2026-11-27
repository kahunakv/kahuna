using System.Collections.Concurrent;
using Google.Protobuf;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
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
    // A sharded map: at the record count of a loaded node, one concurrent dictionary replaces a bucket array
    // of megabytes, and makes a new node for every record, at each doubling on the apply path. Every writer
    // holds applyGate, so one lock for each shard is enough.
    private readonly ShardedConcurrentMap<RecordKey, TransactionRecord, KeyHashShardHasher<RecordKey>> records =
        new(RecordMapShards, concurrencyLevel: 1);

    /// <summary>
    /// The key of the record map: a transaction id and an epoch in 24 bytes. A tuple of
    /// <see cref="HLCTimestamp"/> and a long is 32 bytes, and the map holds one key for each record.
    /// </summary>
    private readonly struct RecordKey : IEquatable<RecordKey>
    {
        private readonly PackedTimestamp transactionId;

        private readonly long epoch;

        public RecordKey(HLCTimestamp transactionId, long epoch)
        {
            this.transactionId = new(transactionId);
            this.epoch = epoch;
        }

        public bool Equals(RecordKey other) => epoch == other.epoch && transactionId.Equals(other.transactionId);

        public override bool Equals(object? obj) => obj is RecordKey other && Equals(other);

        public override int GetHashCode() => TransactionKeyHash.Combine(transactionId.Value, epoch);
    }

    // At 560,000 records a shard holds about 4,400 of them, and its bucket array is 67 KB.
    private const int RecordMapShards = 128;

    // Number of resident records. The sharded map keeps no count. Maintained with every insert and remove.
    private int recordCount;

    // Running estimate of the heap the resident records retain (see TransactionRecord.EstimateBytes), maintained
    // at every insert/replace/remove so the retention memory budget reads a counter instead of scanning.
    private long estimatedBytes;

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

    // Gives the anchor and the participant keys of a decoded record the string instances that the other
    // stores of the node hold for the same keys.
    private readonly SharedKeyStrings keyStrings;

    /// <summary>The shared key strings this store decodes its records with.</summary>
    internal SharedKeyStrings KeyStrings => keyStrings;

    /// <summary>In-memory configuration (unit tests): no persistence.</summary>
    public TransactionRecordStore() => keyStrings = new SharedKeyStrings();

    /// <summary>Durable configuration: a per-partition on-disk snapshot under <paramref name="storagePath"/> is
    /// loaded on construction and rewritten by <see cref="PersistSnapshot"/> before the WAL checkpoint discards
    /// the log tail. A parse failure fails closed rather than silently starting without a committed decision.</summary>
    /// <param name="keyStrings">The shared key strings of the node, so the records hold the same string for a
    /// key as the completion receipts and the settled identities. Null gives the store a cache of its own.</param>
    public TransactionRecordStore(string? storagePath, string? storageRevision, ILogger<IKahuna>? logger, SharedKeyStrings? keyStrings = null)
    {
        this.logger = logger;
        this.keyStrings = keyStrings ?? new SharedKeyStrings();
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
    /// validation — whether every co-bundled validated base, staged revision and carried read dependency still
    /// holds against the partition's committed-head ledger. Wired to <see cref="PreparedIntentStore.JudgeBundledCommit"/>.</summary>
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
    public TransactionRecordApplyResult Apply(TransactionRecordCommand command, int partitionId) => Apply(command, partitionId, logTerm: 0);

    /// <summary>
    /// As <see cref="Apply(TransactionRecordCommand, int)"/>, with the Raft term of the log entry that carries
    /// the command (<paramref name="logTerm"/>), which the bundled commit gate compares against
    /// <see cref="CommitTransactionCommand.LockGrantTerm"/>. Zero means the apply has no log entry behind it
    /// (a direct apply, or a local projection of a forwarded delta), so there is no term to compare and that
    /// part of the gate does not run.
    /// </summary>
    public TransactionRecordApplyResult Apply(TransactionRecordCommand command, int partitionId, long logTerm)
    {
        RecordKey key = KeyOf(command);

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

                // The term comparison comes first and needs no judge: it reads only the command and the log
                // entry that carries it, both identical on every replica.
                BundledCommitJudgement judgement =
                    logTerm != 0 && bundledCommit.LockGrantTerm != 0 && logTerm != bundledCommit.LockGrantTerm
                        ? new(BundledCommitVerdict.LeaderChanged,
                            $"the bundle was proposed in term {logTerm}, and the transaction's locks on the partition were granted in term {bundledCommit.LockGrantTerm}")
                    : bundledCommitJudge is null
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
                        case BundledCommitVerdict.LeaderChanged:
                            DurableTransactionMetrics.OnePhaseGatedCommitLeaderChangeRejections.Add(1);
                            break;
                        default:
                            DurableTransactionMetrics.OnePhaseGatedCommitRejections.Add(1);
                            break;
                    }

                    List<HLCTimestamp> rejected = existing.RejectedBundledCommitOpIds is { } prior ? [.. prior] : [];
                    rejected.Add(bundledCommit.OpId);
                    TransactionRecord memoed = existing with { RejectedBundledCommitOpIds = rejected };
                    records[key] = memoed;
                    AccountReplaced(existing, memoed);
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
                AccountReplaced(existing, result.Record);
                StampDirty(result.Record.RecordAnchorKey);
            }
            else if (result.Outcome == TransactionApplyOutcome.Removed)
            {
                if (records.TryRemove(key, out TransactionRecord? removed))
                    AccountReplaced(removed, null);
                StampDirty(removed?.RecordAnchorKey ?? existing?.RecordAnchorKey);
            }

            return result;
        }
    }

    public TransactionRecord? Get(HLCTimestamp transactionId, long epoch) =>
        records.TryGetValue(new RecordKey(transactionId, epoch), out TransactionRecord? record) ? record : null;

    /// <summary>Every record, as one copy that is atomic across records: the copy is taken under the apply
    /// gate, which every writer holds. A caller that runs on a timer uses <see cref="EnumerateRecords"/>.</summary>
    public IReadOnlyCollection<TransactionRecord> Snapshot()
    {
        lock (applyGate)
        {
            List<TransactionRecord> snapshot = new(Count);

            foreach (KeyValuePair<RecordKey, TransactionRecord> kv in records)
                snapshot.Add(kv.Value);

            return snapshot;
        }
    }

    /// <summary>
    /// Walks every record without a copy of the map. <see cref="Snapshot"/> copies the map twice into arrays of
    /// one reference for each record, 9 MiB at half a million records; a caller that runs on a timer uses this
    /// walk. The walk is not a point-in-time cut: a record present for the whole walk is returned once, in a
    /// state it had at some instant of the walk, and a record created or removed during the walk is returned or
    /// not. The caller may await between two records.
    /// </summary>
    public IEnumerable<TransactionRecord> EnumerateRecords()
    {
        foreach (KeyValuePair<RecordKey, TransactionRecord> kv in records)
            yield return kv.Value;
    }

    public int Count => Volatile.Read(ref recordCount);

    /// <summary>Estimated heap bytes retained by the resident records (see <see cref="TransactionRecord.EstimateBytes"/>).
    /// A running counter, so the retention budget can read it every tick without a scan.</summary>
    public long EstimatedBytes => Math.Max(0, Interlocked.Read(ref estimatedBytes));

    // Adjusts the running estimate for one dictionary slot going from <paramref name="before"/> to
    // <paramref name="after"/> (either may be null: insert, replace, remove).
    private void AccountReplaced(TransactionRecord? before, TransactionRecord? after)
    {
        if (before is null && after is not null)
            Interlocked.Increment(ref recordCount);
        else if (before is not null && after is null)
            Interlocked.Decrement(ref recordCount);

        long delta = (after?.EstimateBytes() ?? 0) - (before?.EstimateBytes() ?? 0);
        if (delta != 0)
            Interlocked.Add(ref estimatedBytes, delta);
    }

    // ── replication ─────────────────────────────────────────────────────────────

    public bool Restore(int partitionId, RaftLog log) => ApplyLog(log, partitionId);

    public bool Replicate(int partitionId, RaftLog log) => ApplyLog(log, partitionId);

    // The proposer's decoded commands, keyed by the exact byte array handed to Raft, budgeted for one
    // take per co-hosted node. See ProposedDeltaCache for the reuse and lifetime contract; reusing the
    // producer's instances is safe only because commands, records, and the participant list are immutable.
    private static readonly ProposedDeltaCache<TransactionRecordCommand[]> locallyProposedDeltas = new();

    private bool ApplyLog(RaftLog log, int partitionId)
    {
        if (log.LogType != ReplicationTypes.TransactionRecord || log.LogData is null)
            return true;

        locallyProposedDeltas.TryTake(log.LogData, out TransactionRecordCommand[]? proposed);
        ApplyDelta(partitionId, log.Term, proposed, log.LogData);
        return true;
    }

    /// <summary>
    /// Applies one record delta that a log entry of <paramref name="partitionId"/> carries, in the term of that entry:
    /// the <paramref name="commands"/> that this node decoded when it serialized the delta, or a decode of
    /// <paramref name="delta"/> when it has none. A part of a one-phase bundle entry applies through here, from a span
    /// of the entry.
    /// </summary>
    internal void ApplyDelta(int partitionId, long logTerm, TransactionRecordCommand[]? commands, ReadOnlySpan<byte> delta)
    {
        if (commands is not null)
        {
            foreach (TransactionRecordCommand command in commands)
                Apply(command, partitionId, logTerm);

            return;
        }

        TransactionRecordDeltaMessage message = ReplicationSerializer.UnserializeTransactionRecordDeltaMessage(delta);

        foreach (TransactionRecordCommandMessage command in message.Commands)
            Apply(ToCommand(command, keyStrings), partitionId, logTerm);
    }

    /// <summary>The commands that this node registered for <paramref name="delta"/> when it serialized it. The
    /// registration is removed: the caller puts <paramref name="delta"/> into an entry that replaces it in the log and
    /// registers the commands with that entry.</summary>
    internal static bool TryClaimProposedCommands(byte[] delta, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out TransactionRecordCommand[]? commands) =>
        locallyProposedDeltas.TryClaim(delta, out commands);

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

    private static RecordKey KeyOf(TransactionRecordCommand command) => command switch
    {
        InitializeTransactionCommand i => new(i.TransactionId, i.Epoch),
        CommitTransactionCommand c => new(c.TransactionId, c.Epoch),
        AbortTransactionCommand a => new(a.TransactionId, a.Epoch),
        ExtendDecisionDeadlineCommand e => new(e.TransactionId, e.Epoch),
        PurgeTransactionCommand p => new(p.TransactionId, p.Epoch),
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
                m.LockGrantTerm = c.LockGrantTerm;

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

            case ExtendDecisionDeadlineCommand e:
                return new()
                {
                    Kind = TransactionRecordCommandKindMessage.TransactionRecordExtendDeadline,
                    TransactionIdNode = e.TransactionId.N, TransactionIdPhysical = e.TransactionId.L, TransactionIdCounter = e.TransactionId.C,
                    Epoch = e.Epoch, ManifestHash = e.ManifestHash,
                    OpIdNode = e.OpId.N, OpIdPhysical = e.OpId.L, OpIdCounter = e.OpId.C,
                    AttemptNode = e.AttemptHlc.N, AttemptPhysical = e.AttemptHlc.L, AttemptCounter = e.AttemptHlc.C,
                    DecisionDeadlineNode = e.NewDeadline.N, DecisionDeadlinePhysical = e.NewDeadline.L, DecisionDeadlineCounter = e.NewDeadline.C
                };

            default:
                throw new ArgumentOutOfRangeException(nameof(command), command.GetType().Name, "unknown transaction-record command");
        }
    }

    private static TransactionRecordCommand ToCommand(TransactionRecordCommandMessage m, SharedKeyStrings? keyStrings)
    {
        HLCTimestamp txId = new(m.TransactionIdNode, m.TransactionIdPhysical, m.TransactionIdCounter);
        HLCTimestamp opId = new(m.OpIdNode, m.OpIdPhysical, m.OpIdCounter);

        switch (m.Kind)
        {
            case TransactionRecordCommandKindMessage.TransactionRecordInitialize:
            {
                return new InitializeTransactionCommand(
                    txId, m.Epoch, m.CoordinatorKey, Shared(keyStrings, m.RecordAnchorKey),
                    new HLCTimestamp(m.CommitTimestampNode, m.CommitTimestampPhysical, m.CommitTimestampCounter),
                    new HLCTimestamp(m.DecisionDeadlineNode, m.DecisionDeadlinePhysical, m.DecisionDeadlineCounter),
                    m.ManifestHash, DecodeParticipants(m.Participants, keyStrings), opId,
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
                    reads,
                    m.LockGrantTerm);
            }

            case TransactionRecordCommandKindMessage.TransactionRecordAbort:
                return new AbortTransactionCommand(txId, m.Epoch, m.ManifestHash, (TransactionAbortClass)m.AbortClass, opId,
                    new HLCTimestamp(m.AttemptNode, m.AttemptPhysical, m.AttemptCounter),
                    Shared(keyStrings, m.RecordAnchorKey),
                    new HLCTimestamp(m.CommitTimestampNode, m.CommitTimestampPhysical, m.CommitTimestampCounter),
                    new HLCTimestamp(m.DecisionDeadlineNode, m.DecisionDeadlinePhysical, m.DecisionDeadlineCounter),
                    new HLCTimestamp(m.CreatedAtNode, m.CreatedAtPhysical, m.CreatedAtCounter));

            case TransactionRecordCommandKindMessage.TransactionRecordPurge:
                return new PurgeTransactionCommand(txId, m.Epoch);

            case TransactionRecordCommandKindMessage.TransactionRecordExtendDeadline:
                return new ExtendDecisionDeadlineCommand(txId, m.Epoch, m.ManifestHash,
                    new HLCTimestamp(m.DecisionDeadlineNode, m.DecisionDeadlinePhysical, m.DecisionDeadlineCounter),
                    opId,
                    new HLCTimestamp(m.AttemptNode, m.AttemptPhysical, m.AttemptCounter));

            default:
                throw new ArgumentOutOfRangeException(nameof(m), m.Kind, "unknown transaction-record command kind");
        }
    }

    private static string Shared(SharedKeyStrings? keyStrings, string key) =>
        keyStrings is null ? key : keyStrings.Share(key);

    // The participants of a decoded record, as an array of the exact size: the record keeps them for the
    // retention window, and a list adds an object and spare capacity to each record. Each key goes through the
    // shared key strings, so the anchor, which is the key of one participant, and the receipts and settled
    // identities of the same keys hold the same string.
    private static TransactionParticipantRef[] DecodeParticipants(
        Google.Protobuf.Collections.RepeatedField<TransactionParticipantRefMessage> messages, SharedKeyStrings? keyStrings)
    {
        if (messages.Count == 0)
            return [];

        TransactionParticipantRef[] participants = new TransactionParticipantRef[messages.Count];

        for (int i = 0; i < participants.Length; i++)
        {
            TransactionParticipantRefMessage p = messages[i];
            participants[i] = new TransactionParticipantRef(Shared(keyStrings, p.Key), (KeyValueDurability)p.Durability);
        }

        return participants;
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
            //
            // The map is walked directly. Its Values property copies every record reference into one list
            // under every bucket lock: 4.5 MiB at 560,000 records in one allocation, with every apply of a
            // record waiting for the copy. The walk is not a point-in-time cut, and the capture does not need
            // one: a record applied before the capture began is in the map for the whole walk, so it is
            // written in a state at or after that apply, and the entries applied during the walk are above
            // the position this capture certifies, so a restart replays them on top of the file.
            lock (fileLock)
            {
                string tmp = path + ".tmp";

                using (FileStream file = new(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 64 * 1024))
                using (CodedOutputStream output = new(file))
                {
                    TransactionRecordSnapshotEntry entry = new();
                    List<TransactionParticipantRefMessage> participantPool = [];

                    foreach (KeyValuePair<RecordKey, TransactionRecord> kv in records)
                    {
                        TransactionRecord record = kv.Value;

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

        // Each file is decoded one record at a time straight off the file and merged as it is read: a partition's
        // record set can be hundreds of megabytes on a busy durable workload, and a restarting node loading it as
        // one array plus one protobuf object per record plus the records themselves held all three at once. A
        // failure part-way through leaves a partial set, which is harmless: the constructor throws and the node
        // refuses to start, exactly as it did when the whole file failed to parse up front.
        foreach (string path in files)
        {
            try
            {
                lock (fileLock)
                {
                    using FileStream file = new(path, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024);

                    foreach (TransactionRecord record in ReadRecords(file, keyStrings))
                        MergeLoad(record);
                }
            }
            catch (InvalidProtocolBufferException ex)
            {
                throw new InvalidDataException($"Corrupt transaction-record snapshot {path}; refusing to start empty and lose a committed decision", ex);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                throw new IOException($"Failed to read transaction-record snapshot {path}; refusing to start with a possibly incomplete decision set", ex);
            }
        }
    }

    // Load-time merge across (possibly overlapping) per-partition files: a terminal decision is authoritative
    // over an Undecided one; two terminal records for one identity must agree (immutable decision), so the first
    // wins and a conflict is logged rather than silently overwriting.
    private void MergeLoad(TransactionRecord incoming)
    {
        RecordKey key = new(incoming.TransactionId, incoming.Epoch);
        if (!records.TryGetValue(key, out TransactionRecord? existing))
        {
            records[key] = incoming;
            AccountReplaced(null, incoming);
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
            AccountReplaced(existing, incoming);
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
                TransactionRecord mergedRecord = existing with { RejectedBundledCommitOpIds = merged };
                records[key] = mergedRecord;
                AccountReplaced(existing, mergedRecord);
                StampDirty(existing.RecordAnchorKey);
            }
        }
    }

    // ── state transfer (split/merge) ────────────────────────────────────────────────

    /// <summary>Records whose anchor currently routes into <c>[startKey, endKey)</c> (ordinal, half-open) — the
    /// set a range split/merge hands to the destination partition. One cut that is atomic across records: the
    /// walk runs under the apply gate, which every writer holds.</summary>
    public IReadOnlyList<TransactionRecord> SnapshotRange(string? startKey, string? endKey)
    {
        List<TransactionRecord> result = [];

        lock (applyGate)
        {
            foreach (KeyValuePair<RecordKey, TransactionRecord> kv in records)
            {
                TransactionRecord record = kv.Value;
                string anchor = record.RecordAnchorKey;
                if (startKey is not null && string.CompareOrdinal(anchor, startKey) < 0)
                    continue;
                if (endKey is not null && string.CompareOrdinal(anchor, endKey) >= 0)
                    continue;
                result.Add(record);
            }
        }

        return result;
    }

    /// <summary>
    /// Writes the records whose anchor satisfies <paramref name="isOwnedAnchor"/> to <paramref name="output"/> in the
    /// <see cref="SerializeRecords"/> wire format and returns how many were written. The whole-partition export
    /// streams a partition's slice through this instead of copying the store: the filter runs inside one lock-free
    /// walk of the map and each owned record goes through one reused entry message, so the cost is the walk plus
    /// the owned rows — never a copy of every record the node holds, and never one protobuf object per record.
    ///
    /// <para>The walk is not a point-in-time cut: a record present for the whole walk is written in a state it
    /// really had at some instant during the walk; one created or removed during the walk may or may not be. That
    /// matches the export's at-least contract — every transition applied before the export began is reflected,
    /// and the log entries after that are replayed on the receiver above the snapshot boundary through the same
    /// deterministic state machine, which folds an already-reflected transition idempotently.</para>
    /// </summary>
    public int WritePartitionRecords(Stream output, Func<string, bool> isOwnedAnchor)
    {
        int written = 0;

        using CodedOutputStream coded = new(output, leaveOpen: true);
        TransactionRecordSnapshotEntry entry = new();
        List<TransactionParticipantRefMessage> participantPool = [];

        foreach (KeyValuePair<RecordKey, TransactionRecord> kv in records)
        {
            TransactionRecord record = kv.Value;

            if (!isOwnedAnchor(record.RecordAnchorKey))
                continue;

            FillSnapshotEntry(entry, participantPool, record);
            coded.WriteTag(TransactionRecordSnapshotMessage.RecordsFieldNumber, WireFormat.WireType.LengthDelimited);
            coded.WriteMessage(entry);
            written++;
        }

        return written;
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
            List<RecordKey>? toRemove = null;

            foreach (KeyValuePair<RecordKey, TransactionRecord> kv in records)
            {
                if (shouldRemoveAnchor(kv.Value.RecordAnchorKey))
                    (toRemove ??= []).Add(kv.Key);
            }

            if (toRemove is null)
                return 0;

            foreach (RecordKey key in toRemove)
            {
                if (records.TryRemove(key, out TransactionRecord? removed))
                    AccountReplaced(removed, null);
            }

            StampAllDirty();
            return toRemove.Count;
        }
    }

    /// <summary>Folds transferred records into this partition's set (idempotent by identity + terminal-decision
    /// authority), for whole-partition state transfer that repairs a below-floor node or a split/merge cutover.
    /// Enumerates <paramref name="incoming"/> once, folding each record before reading the next, so a lazily
    /// decoded source (<see cref="ReadRecords"/>) is never materialised.</summary>
    public void ImportRecords(IEnumerable<TransactionRecord> incoming)
    {
        // The gate is taken for each record and released for the decode of the next one: every writer of the
        // map holds it, which is what makes a copy taken under it atomic, and a decode can read a file.
        foreach (TransactionRecord record in incoming)
        {
            lock (applyGate)
                MergeLoad(record);
        }
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
            result.Add(FromSnapshotEntry(entry, keyStrings: null));

        return result;
    }

    /// <summary>
    /// Decodes a <see cref="SerializeRecords"/> payload one record at a time, reading it off
    /// <paramref name="payload"/> as the caller enumerates. Unlike <see cref="DeserializeRecords"/>, neither the
    /// payload bytes nor the decoded set is ever held whole: each entry is parsed into a fresh message that is
    /// garbage as soon as its record is yielded, so the live memory of a caller that folds each record into the
    /// store (<see cref="ImportRecords"/>) is the store itself plus one entry. The whole-partition install and the
    /// startup load of the per-partition snapshot files use this, since a partition's record set can be hundreds of
    /// megabytes under a busy durable workload.
    /// A malformed payload throws <see cref="InvalidProtocolBufferException"/> at the entry that fails to parse.
    /// </summary>
    /// <param name="keyStrings">The shared key strings of the store that keeps the records (its
    /// <see cref="KeyStrings"/>), so each record holds the strings the node already holds for its keys. Null
    /// for a caller that does not keep the records.</param>
    public static IEnumerable<TransactionRecord> ReadRecords(Stream payload, SharedKeyStrings? keyStrings = null)
    {
        CodedInputStream input = new(payload, leaveOpen: true);

        uint tag;
        while ((tag = input.ReadTag()) != 0)
        {
            if (WireFormat.GetTagFieldNumber(tag) != TransactionRecordSnapshotMessage.RecordsFieldNumber
                || WireFormat.GetTagWireType(tag) != WireFormat.WireType.LengthDelimited)
            {
                input.SkipLastField();
                continue;
            }

            TransactionRecordSnapshotEntry entry = new();
            input.ReadMessage(entry);
            yield return FromSnapshotEntry(entry, keyStrings);
        }
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

        // By index: a foreach over the list's interface allocates an enumerator for each record, and a capture
        // fills one entry for each record of the store.
        IReadOnlyList<TransactionParticipantRef> participants = r.Participants;
        for (int i = 0; i < participants.Count; i++)
        {
            TransactionParticipantRef p = participants[i];

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

    private static TransactionRecord FromSnapshotEntry(TransactionRecordSnapshotEntry e, SharedKeyStrings? keyStrings)
    {
        TransactionParticipantRef[] participants = DecodeParticipants(e.Participants, keyStrings);

        TransactionDecision decision = e.Decision switch
        {
            TransactionDecisionMessage.TransactionDecisionCommit => TransactionDecision.Commit,
            TransactionDecisionMessage.TransactionDecisionAbort => TransactionDecision.Abort,
            _ => TransactionDecision.Undecided
        };

        List<HLCTimestamp>? rejected = null;
        if (e.RejectedBundledCommits.Count > 0)
        {
            rejected = new(e.RejectedBundledCommits.Count);
            foreach (TransactionRecordRejectedBundledCommitMessage r in e.RejectedBundledCommits)
                rejected.Add(new HLCTimestamp(r.OpIdNode, r.OpIdPhysical, r.OpIdCounter));
        }

        return new TransactionRecord(
            new HLCTimestamp(e.TransactionIdNode, e.TransactionIdPhysical, e.TransactionIdCounter),
            e.Epoch,
            // A decided record keeps no coordinator key (see TransactionRecordStateMachine). A snapshot that a
            // version before that rule wrote still carries one.
            decision == TransactionDecision.Undecided ? e.CoordinatorKey : string.Empty,
            Shared(keyStrings, e.RecordAnchorKey),
            new HLCTimestamp(e.CommitTimestampNode, e.CommitTimestampPhysical, e.CommitTimestampCounter),
            new HLCTimestamp(e.DecisionDeadlineNode, e.DecisionDeadlinePhysical, e.DecisionDeadlineCounter),
            e.ManifestHash, participants, e.ManifestPresent,
            decision,
            (TransactionAbortClass)e.AbortClass,
            new HLCTimestamp(e.WinningOpIdNode, e.WinningOpIdPhysical, e.WinningOpIdCounter),
            new HLCTimestamp(e.CreatedAtNode, e.CreatedAtPhysical, e.CreatedAtCounter),
            new HLCTimestamp(e.DecidedAtNode, e.DecidedAtPhysical, e.DecidedAtCounter))
        {
            RejectedBundledCommitOpIds = rejected
        };
    }
}


using System.Collections.Concurrent;

using Nixie;

using Kommander;
using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The KeyValueRestorer class is responsible for restoring key-value data from a Raft log during
/// the state recovery process. It processes and interprets the log entries to update the
/// key-value storage accordingly, ensuring system consistency.
/// </summary>
internal sealed class KeyValueRestorer
{
    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IRaft raft;

    private readonly CompletionReceiptStore completionReceiptStore;

    private readonly UnflushedKeyValueWritesIndex? unflushedWrites;

    private readonly PartitionDurabilityTracker? durabilityTracker;

    private readonly ILogger<IKahuna> logger;

    // The node's prepared-intent store, restored from its own snapshot and from the replayed prepare deltas
    // that precede a by-reference materialization record in the same partition's log. Null (bare
    // direct-construction tests) makes every by-reference record a reported miss.
    private readonly PreparedIntentStore? preparedIntentStore;

    // A synchronous point read of this node's durable row for a key: the last proof, once no intent can be
    // found for a by-reference materialization, that its value is already durable here (a second producer's
    // duplicate whose first copy flushed before the crash) rather than missing. Consulted only on that path,
    // which is empty on a healthy restart, so its cost never touches the replay's throughput. Null (bare
    // direct-construction tests) proves nothing, and every such materialization is reported as missing.
    private readonly Func<string, KeyValueEntry?>? readDurableRow;

    // The data partition a key currently routes to. A materialization the replay cannot resolve for a key this
    // partition no longer owns — its range moved out after the floor, and the un-host purge took the key's rows
    // and intents with it — is not a value this node is missing, and must not gate the partition. Null (bare
    // direct-construction tests) or a resolver that throws (the range map not yet rebuilt when a data partition
    // replays) means ownership is unknown, and the miss counts.
    private readonly Func<string, int>? keyOwner;

    public KeyValueRestorer(IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter, IRaft raft, CompletionReceiptStore completionReceiptStore, ILogger<IKahuna> logger, UnflushedKeyValueWritesIndex? unflushedWrites = null, PartitionDurabilityTracker? durabilityTracker = null, PreparedIntentStore? preparedIntentStore = null, Func<string, KeyValueEntry?>? readDurableRow = null, Func<string, int>? keyOwner = null)
    {
        this.preparedIntentStore = preparedIntentStore;
        this.backgroundWriter = backgroundWriter;
        this.raft = raft;
        this.completionReceiptStore = completionReceiptStore;
        this.logger = logger;
        this.unflushedWrites = unflushedWrites;
        this.durabilityTracker = durabilityTracker;
        this.readDurableRow = readDurableRow;
        this.keyOwner = keyOwner;
    }

    // ── per-restart accounting of by-reference materializations ─────────────────────────────────────
    //
    // A restart's replay is the one moment a node can lose committed values silently: a by-reference record (or
    // a materializing resolve) names an intent, and if nothing on the node can produce that intent the value is
    // gone here until something rewrites the key. Each partition's replay therefore keeps a tally of where every
    // by-reference materialization was resolved from and which ones could not be, and hands it to the
    // restore-finished hook, which logs one summary line and gates the partition when anything is unresolved.

    /// <summary>The bound on distinct keys the unresolved tally remembers; beyond it only the count grows.</summary>
    internal const int UnresolvedKeysCap = 100_000;

    private sealed class RestoreTally
    {
        public long FromLive;
        public long FromHistory;
        public long FromRetained;
        public long Durable;
        public long Foreign;
        public long Unresolved;
        public long FirstUnresolvedLogIndex = -1;
        public long LastUnresolvedLogIndex = -1;
        public readonly HashSet<string> UnresolvedKeys = new(StringComparer.Ordinal);
    }

    /// <summary>What one partition's restart replay did with its by-reference materializations: how many it
    /// resolved from each source, how many named a value already durable here, how many named a key the
    /// partition no longer owns, and how many it left unresolved (with their entry range and distinct keys).</summary>
    internal readonly record struct RestoreSummary(
        long FromLive, long FromHistory, long FromRetained, long Durable, long Foreign, long Unresolved,
        long FirstUnresolvedLogIndex, long LastUnresolvedLogIndex, int UnresolvedKeys)
    {
        public long Records => FromLive + FromHistory + FromRetained + Durable + Foreign + Unresolved;
    }

    private readonly ConcurrentDictionary<int, RestoreTally> tallies = new();

    private static readonly KeyValuePair<string, object?> SourceLive = new("source", "live");
    private static readonly KeyValuePair<string, object?> SourceHistory = new("source", "history");
    private static readonly KeyValuePair<string, object?> SourceRetained = new("source", "retained");
    private static readonly KeyValuePair<string, object?> SourceDurable = new("source", "durable");

    private RestoreTally TallyOf(int partitionId) => tallies.GetOrAdd(partitionId, static _ => new RestoreTally());

    /// <summary>Closes the tally of <paramref name="partitionId"/>'s replay and returns it. An empty summary
    /// means the replay reached no by-reference materialization.</summary>
    internal RestoreSummary CompleteRestore(int partitionId)
    {
        if (!tallies.TryRemove(partitionId, out RestoreTally? tally))
            return default;

        return new RestoreSummary(
            tally.FromLive, tally.FromHistory, tally.FromRetained, tally.Durable, tally.Foreign, tally.Unresolved,
            tally.FirstUnresolvedLogIndex, tally.LastUnresolvedLogIndex, tally.UnresolvedKeys.Count);
    }

    /// <summary>Whether a row at (<paramref name="rowRevision"/>, <paramref name="rowLastModified"/>) holds the
    /// materialization at (<paramref name="revision"/>, <paramref name="lastModified"/>) or a later one — the
    /// overlay's newest-head order, revision first and commit HLC as the same-revision tiebreak, so a set and the
    /// delete that reuses its revision number are told apart.</summary>
    private static bool RowCovers(long rowRevision, HLCTimestamp rowLastModified, long revision, HLCTimestamp lastModified) =>
        rowRevision > revision || (rowRevision == revision && rowLastModified >= lastModified);

    /// <summary>Whether <paramref name="key"/> is known to route to a partition other than <paramref name="partitionId"/>.</summary>
    private bool IsForeign(int partitionId, string key)
    {
        Func<string, int>? owner = keyOwner;
        if (owner is null)
            return false;

        try
        {
            return owner(key) != partitionId;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>Tallies a materialization no source could resolve and whose row is not durable here: unresolved
    /// (true) unless the key routes elsewhere, in which case the miss is the un-host purge's doing (false).</summary>
    private bool CountMiss(int partitionId, long logIndex, string key)
    {
        RestoreTally tally = TallyOf(partitionId);

        if (IsForeign(partitionId, key))
        {
            tally.Foreign++;
            return false;
        }

        tally.Unresolved++;
        if (tally.FirstUnresolvedLogIndex < 0)
            tally.FirstUnresolvedLogIndex = logIndex;
        tally.LastUnresolvedLogIndex = logIndex;
        if (tally.UnresolvedKeys.Count < UnresolvedKeysCap)
            tally.UnresolvedKeys.Add(key);

        Transactions.DurableTransactionMetrics.MaterializationIntentMissing.Add(1);
        Transactions.DurableTransactionMetrics.RestoreByReferenceUnresolved.Add(1);
        return true;
    }

    /// <summary>
    /// A replayed materializing resolve found no intent to install from (see
    /// <see cref="IResolvedIntentInstaller.NoteUnresolvedOnReplay"/>). The row's last-modified is the commit
    /// timestamp of the commit that wrote it and a key's commits are HLC-ordered, so a row queued or durable here
    /// at or after this commit's timestamp holds its value or a later one: the settle applied before the crash and
    /// the row flushed before the snapshot dropped the retained copy. Anything else is a value this restart left
    /// missing.
    /// </summary>
    internal void NoteUnresolvedMaterializingResolve(int partitionId, long logIndex, HLCTimestamp transactionId, long epoch, string key, HLCTimestamp commitTimestamp)
    {
        if (unflushedWrites is not null
            && unflushedWrites.TryGet(key, out UnflushedKeyValueWrite pending)
            && pending.LastModified >= commitTimestamp)
        {
            TallyOf(partitionId).Durable++;
            Transactions.DurableTransactionMetrics.RestoreByReferenceResolved.Add(1, SourceDurable);
            return;
        }

        if (readDurableRow?.Invoke(key) is { } row && row.LastModified >= commitTimestamp)
        {
            TallyOf(partitionId).Durable++;
            Transactions.DurableTransactionMetrics.RestoreByReferenceResolved.Add(1, SourceDurable);
            return;
        }

        if (!CountMiss(partitionId, logIndex, key))
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "KeyValueRestorer: materializing resolve for key {Key} (transaction {TransactionId} epoch {Epoch}) has no source on the restart replay of partition {PartitionId} (log entry {LogIndex}); the key routes to another partition",
                    key, transactionId, epoch, partitionId, logIndex);
            return;
        }

        logger.LogError(
            "KeyValueRestorer: materializing resolve for key {Key} (transaction {TransactionId} epoch {Epoch}, committed at {CommitTimestamp}) found no intent to install from on the restart replay (log entry {LogIndex}); the committed value is missing on this node",
            key, transactionId, epoch, commitTimestamp, logIndex);
    }

    /// <summary>
    /// Restores key-value data from the provided Raft log for a given partition.
    /// It processes the log to ensure the key-value storage is updated correctly and system consistency is maintained.
    /// </summary>
    /// <param name="partitionId">The ID of the partition where the log data is being restored.</param>
    /// <param name="log">The Raft log containing key-value data to be restored.</param>
    /// <returns>
    /// Returns <c>true</c> if the restoration succeeds or if the log is empty;
    /// otherwise, returns <c>false</c> if an error occurs during restoration.
    /// </returns>
    public bool Restore(int partitionId, RaftLog log)
    {
        if (log.LogData is null || log.LogData.Length == 0)
            return true;

        try
        {
            KeyValueMessage keyValueMessage = ReplicationSerializer.UnserializeKeyValueMessage(log.LogData);

            KeyValueState state;
            byte[]? messageValue;

            switch (KeyValueMessageDecoder.Classify(KeyValueMessageDecoder.RecordType(keyValueMessage)))
            {
                case KeyValueRecordKind.ByReferenceMutation:
                {
                    // A by-reference record carries no value: the mutation comes from the prepared intent it
                    // names. Replay reaches it in the same order a live replica does — the prepare delta applies
                    // first on this partition, and the settle that removes the intent applies later. When the
                    // replay window starts at or below the prepare, the replayed prepare either re-installs the
                    // intent (above the checkpoint's certified position) or, inside the history window below it,
                    // installs nothing live and is kept as replay history for exactly this record. When the
                    // window starts above the prepare (the durability floor certified the prepare through an
                    // earlier intent snapshot) the intent comes from the snapshot: as a live intent if the settle
                    // had not applied when the snapshot was written, otherwise as a settled intent the store
                    // retained because this record's row was still queued for the flush — the only place the
                    // committed value survives once the intent is settled and the row is not yet in the backend.
                    if (!TryResolveIntentForRestore(partitionId, keyValueMessage, log.Id, out PreparedIntent? intent))
                        return true;

                    state = intent!.State;
                    messageValue = intent.Value;
                    break;
                }

                case KeyValueRecordKind.ValueMutation:
                    (state, messageValue) = KeyValueMessageDecoder.Decode(keyValueMessage);
                    break;

                default:
                    logger.LogError("KeyValueRestorer: Unknown restore message type: {Type}", keyValueMessage.Type);
                    return true;
            }

            CommittedKeyValueMutation mutation = CommittedKeyValueMutation.FromRecord(keyValueMessage, messageValue, state);

            // A replayed transactional entry re-derives a completion receipt below, so it must
            // register on Flush AND Receipts — the floor may not pass it until the flushed row and
            // a receipt snapshot covering the rebuilt receipt are both durable. A single-shot entry
            // (zero transaction id) derives no receipt and registers on Flush alone.
            //
            // Register before enqueueing: the partition's durability floor must not pass this
            // replayed entry until its durable artifacts land. Replay runs in log-id order, so the
            // registration always precedes any watermark advance over this index.
            if (mutation.IsTransactional)
                durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Flush, DurabilityChannel.Receipts);
            else
                durabilityTracker?.RegisterPending(partitionId, log.Id, DurabilityChannel.Flush);

            // Raise the Receipts resolve ceiling over this entry after its receipt is recorded, so a snapshot
            // capture that samples the raised ceiling always finds the receipt already in the store.
            if (QueueCommittedMutation(partitionId, log.Id, mutation))
                durabilityTracker?.MarkApplied(partitionId, log.Id, DurabilityChannel.Receipts);

            return true;
        }
        catch (Exception ex) when (Diagnostics.ProcessFaults.Survivable(ex, "KeyValueRestorer.Restore"))
        {
            logger.LogError(ex, "KeyValueRestorer: Error processing replication message");
            return false;
        }
    }

    /// <summary>
    /// Replays a materializing resolve's install of a committed prepared intent at <paramref name="logIndex"/>:
    /// the same durable state a replayed materialization record of the intent rebuilds. The entry is already
    /// pending on the prepared-intent channel for the whole replay of its delta, and it may install several rows,
    /// so each row is counted on the Flush channel before it is queued and the Receipts channel is added to the
    /// entry; <see cref="CompleteResolvedIntentEntry"/> raises the Receipts ceiling after the last row.
    /// </summary>
    internal void RestoreResolvedIntent(int partitionId, long logIndex, PreparedIntent intent)
    {
        CountResolvedIntentSource(partitionId, intent);

        CommittedKeyValueMutation mutation = CommittedKeyValueMutation.FromIntent(intent);

        if (durabilityTracker is not null)
        {
            durabilityTracker.AddPendingFlushRow(partitionId, logIndex);
            if (mutation.IsTransactional)
                durabilityTracker.AddPending(partitionId, logIndex, DurabilityChannel.Receipts);
        }

        QueueCommittedMutation(partitionId, logIndex, mutation);
    }

    // Attributes a replayed materializing resolve's install to the source the store took the intent from, in the
    // order the store consults them: the intent is still live at install time (the install precedes the resolve's
    // own apply), or it was kept as replay history, or it was retained after its settle.
    private void CountResolvedIntentSource(int partitionId, PreparedIntent intent)
    {
        RestoreTally tally = TallyOf(partitionId);

        if (preparedIntentStore is null || preparedIntentStore.GetByIdentity(intent.TransactionId, intent.Epoch, intent.Key) is not null)
        {
            tally.FromLive++;
            Transactions.DurableTransactionMetrics.RestoreByReferenceResolved.Add(1, SourceLive);
        }
        else if (preparedIntentStore.TryGetReplayHistoryIntent(partitionId, intent.TransactionId, intent.Epoch, intent.Key, out _))
        {
            tally.FromHistory++;
            Transactions.DurableTransactionMetrics.RestoreByReferenceResolved.Add(1, SourceHistory);
        }
        else
        {
            tally.FromRetained++;
            Transactions.DurableTransactionMetrics.RestoreByReferenceResolved.Add(1, SourceRetained);
        }
    }

    /// <summary>Raises the Receipts resolve ceiling over a replayed materializing resolve's entry, after every
    /// receipt its installs derived is recorded (see <see cref="RestoreResolvedIntent"/>).</summary>
    internal void CompleteResolvedIntentEntry(int partitionId, long logIndex) =>
        durabilityTracker?.MarkApplied(partitionId, logIndex, DurabilityChannel.Receipts);

    /// <summary>
    /// Rebuilds the durable state of one replayed committed mutation: records it in the unflushed overlay so
    /// reads observe it before the flush lands, queues the flush that carries <paramref name="logIndex"/>, and
    /// rebuilds the completion receipt so a re-commit after a cold restart or leader change resolves Committed
    /// rather than MustRetry. Returns whether a receipt was recorded. Durability registration is the caller's.
    /// </summary>
    private bool QueueCommittedMutation(int partitionId, long logIndex, in CommittedKeyValueMutation mutation)
    {
        unflushedWrites?.Record(mutation.Key, mutation.Value, mutation.Revision,
            mutation.Expires, mutation.LastUsed, mutation.LastModified, mutation.State, mutation.NoRevision);

        backgroundWriter.Send(BackgroundWriteRequestPool.Rent(
            BackgroundWriteType.QueueStoreKeyValue,
            partitionId,
            mutation.Key,
            mutation.Value,
            mutation.Revision,
            mutation.Expires,
            mutation.LastUsed,
            mutation.LastModified,
            (int)mutation.State,
            mutation.NoRevision,
            logIndex: logIndex
        ));

        if (!mutation.IsTransactional)
            return false;

        completionReceiptStore.Record(mutation.TransactionId, mutation.Key, mutation.RecordAnchorKey, KeyValueDurability.Persistent);
        return true;
    }

    /// <summary>
    /// Resolves the prepared intent a by-reference record names, and reports whether the replay may apply it.
    /// The sources, in order: the live set (the intent's settle had not applied when the snapshot was written),
    /// the replay history (the prepare lay inside the history window below the checkpoint's certified position,
    /// where the fence installs nothing live but keeps the prepare for exactly this reader), and the settled
    /// intents retained until their row is durable (the replay window starts above the prepare and the record's
    /// row was still queued at the crash). False means the record contributes nothing here — because the value
    /// is already queued or durable (a second producer's duplicate, proven from the overlay or the backend row)
    /// or because the intent is genuinely absent, which is the correctness alarm this restart is graded on.
    /// </summary>
    private bool TryResolveIntentForRestore(int partitionId, KeyValueMessage keyValueMessage, long logIndex, out PreparedIntent? intent)
    {
        HLCTimestamp transactionId = new(
            keyValueMessage.TransactionIdNode, keyValueMessage.TransactionIdPhysical, keyValueMessage.TransactionIdCounter);

        intent = preparedIntentStore?.GetByIdentity(transactionId, keyValueMessage.Epoch, keyValueMessage.Key);

        if (intent is not null)
        {
            if (intent.Revision == keyValueMessage.Revision)
            {
                TallyOf(partitionId).FromLive++;
                Transactions.DurableTransactionMetrics.RestoreByReferenceResolved.Add(1, SourceLive);
                return true;
            }

            // The record and the intent name two different mutations; applying the intent would restore the
            // wrong revision.
            Transactions.DurableTransactionMetrics.MaterializationIntentMissing.Add(1);
            logger.LogError(
                "KeyValueRestorer: by-reference record for key {Key} (transaction {TransactionId} epoch {Epoch}) names revision {Revision}, but the restored intent stands at revision {IntentRevision} (log entry {LogIndex})",
                keyValueMessage.Key, transactionId, keyValueMessage.Epoch, keyValueMessage.Revision, intent.Revision, logIndex);

            intent = null;
            return false;
        }

        if (preparedIntentStore is not null)
        {
            if (preparedIntentStore.TryGetReplayHistoryIntent(partitionId, transactionId, keyValueMessage.Epoch, keyValueMessage.Key, out PreparedIntent? history)
                && history!.Revision == keyValueMessage.Revision)
            {
                intent = history;
                TallyOf(partitionId).FromHistory++;
                Transactions.DurableTransactionMetrics.RestoreByReferenceResolved.Add(1, SourceHistory);
                return true;
            }

            if (preparedIntentStore.TryGetSettledIntentAwaitingFlush(transactionId, keyValueMessage.Epoch, keyValueMessage.Key, out PreparedIntent? settled)
                && settled!.Revision == keyValueMessage.Revision)
            {
                intent = settled;
                TallyOf(partitionId).FromRetained++;
                Transactions.DurableTransactionMetrics.RestoreByReferenceResolved.Add(1, SourceRetained);
                return true;
            }
        }

        // Already replayed by an earlier copy of the same materialization (queued here), or flushed before the
        // crash by a copy below the replay window: the value is this node's, and the record is a duplicate.
        HLCTimestamp recordLastModified = new(keyValueMessage.LastModifiedNode, keyValueMessage.LastModifiedPhysical, keyValueMessage.LastModifiedCounter);

        if ((unflushedWrites is not null
                && unflushedWrites.TryGet(keyValueMessage.Key, out UnflushedKeyValueWrite pending)
                && RowCovers(pending.Revision, pending.LastModified, keyValueMessage.Revision, recordLastModified))
            || (readDurableRow?.Invoke(keyValueMessage.Key) is { } row
                && RowCovers(row.Revision, row.LastModified, keyValueMessage.Revision, recordLastModified)))
        {
            TallyOf(partitionId).Durable++;
            Transactions.DurableTransactionMetrics.RestoreByReferenceResolved.Add(1, SourceDurable);
            return false;
        }

        if (!CountMiss(partitionId, logIndex, keyValueMessage.Key))
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug(
                    "KeyValueRestorer: by-reference record for key {Key} at revision {Revision} (transaction {TransactionId} epoch {Epoch}) has no source on the restart replay of partition {PartitionId} (log entry {LogIndex}); the key routes to another partition",
                    keyValueMessage.Key, keyValueMessage.Revision, transactionId, keyValueMessage.Epoch, partitionId, logIndex);
            return false;
        }

        logger.LogError(
            "KeyValueRestorer: by-reference record for key {Key} at revision {Revision} found no restored intent for transaction {TransactionId} epoch {Epoch} (log entry {LogIndex}), and this node's durable state is below that revision: the committed value is missing on this node",
            keyValueMessage.Key, keyValueMessage.Revision, transactionId, keyValueMessage.Epoch, logIndex);

        return false;
    }
}

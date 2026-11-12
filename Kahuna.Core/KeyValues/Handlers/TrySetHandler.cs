
using Google.Protobuf;
using Nixie;
using Kommander;
using Kommander.Time;

using Kahuna.Utils;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Executes the TrySet operation in the key-value store.
/// </summary>
internal sealed class TrySetHandler : BaseHandler
{
    public TrySetHandler(KeyValueContext context) : base(context)
    {

    }

    public async ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        // Transactional set (a transaction id is present) stages an MVCC entry and returns without a Raft
        // proposal — a different lifecycle from the non-transactional direct write below.
        if (message.TransactionId != HLCTimestamp.Zero)
            return await ExecuteTransactional(message);

        (KeyValueResponse? terminal, KeyValueEntry? entry, KeyValueProposal? proposal, HLCTimestamp currentTime) =
            await BuildNonTransactionalSet(message);

        if (terminal is not null)
            return terminal;

        if (message.Durability == KeyValueDurability.Persistent)
            return CreateProposal(message, entry!, proposal!, currentTime);

        return ApplyEphemeralSet(message, entry!, proposal!);
    }

    /// <summary>
    /// Non-transactional set path used by <see cref="Execute"/>: loads/creates the entry, runs the
    /// lock/intent/flag validation, and builds the proposal. Returns a non-null <c>Terminal</c> when the set
    /// resolves without
    /// a proposal (WaitingForReplication on a live replication intent, MustRetry on a conflicting lock, NotSet
    /// on a failed conditional); otherwise <c>Terminal</c> is null and the entry/proposal/currentTime are set.
    /// </summary>
    private async ValueTask<(KeyValueResponse? Terminal, KeyValueEntry? Entry, KeyValueProposal? Proposal, HLCTimestamp CurrentTime)>
        BuildNonTransactionalSet(KeyValueRequest message)
    {
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        (KeyValueResponse? terminal, KeyValueEntry entry, bool exists, bool readFromBackend) = await LoadAndValidateEntry(message, currentTime);
        if (terminal is not null)
            return (terminal, null, null, currentTime);

        // A backend read is not charged to the commit admission window, and the load can raise the head to a
        // committed intent stamped on a faster clock: stamp after both.
        currentTime = StampAfter(StampAfterBackendRead(currentTime, readFromBackend), entry.LastModified);

        HLCTimestamp newExpires = message.ExpiresMs > 0 ? (currentTime + message.ExpiresMs) : HLCTimestamp.Zero;

        // Check if the value must not be changed according to flags
        if (FailsCondition(message, exists, entry.Value, entry.Revision))
            return (new(KeyValueResponseType.NotSet, entry.Revision, entry.LastModified), null, null, currentTime);

        // Operation type is always TrySet here (this handler only serves sets). Use the literal rather than
        // message.Type so the proposal the aggregator carries records a TrySet, which is what CompleteProposal
        // and the replicated log record expect.
        KeyValueProposal proposal = new(
            KeyValueRequestType.TrySet,
            message.Key,
            message.Value,
            entry.Revision + 1,
            (message.Flags & KeyValueFlags.SetNoRevision) != 0,
            newExpires,
            currentTime,
            currentTime,
            KeyValueState.Set,
            message.Durability
        );

        return (null, entry, proposal, currentTime);
    }

    /// <summary>
    /// Loads the entry (from the store or, for persistent keys, the backend) creating an Undefined placeholder
    /// on a miss, then runs the shared pre-write validation: an in-flight replication intent (retry), a
    /// foreign exclusive/prefix/range lock (MustRetry). Returns a terminal response when the caller must not
    /// proceed; otherwise fills the entry and whether it currently exists.
    /// </summary>
    /// <param name="preloadedHeads">
    /// Rows a batch already read from the backend for keys that were not resident, keyed by key (a null row is a
    /// key the backend does not have). A miss on one of these keys takes its row from here instead of awaiting the
    /// read scheduler inside the turn. Null when nothing was preloaded.
    /// </param>
    /// <param name="takeOverYielding">
    /// Whether a stealable yielding intent of another transaction is taken over here. A check that stages nothing
    /// passes false: the intent stays in place and counts as no conflict, and the stage pass that follows in the
    /// same actor turn takes it over. Otherwise a batch refused after the check would have cost its owner the key
    /// for nothing.
    /// </param>
    private async ValueTask<(KeyValueResponse? Terminal, KeyValueEntry Entry, bool Exists, bool ReadFromBackend)> LoadAndValidateEntry(
        KeyValueRequest message, HLCTimestamp currentTime, Dictionary<string, ReadOnlyKeyValueEntry?>? preloadedHeads = null, bool takeOverYielding = true)
    {
        bool exists = true;
        bool readFromBackend = false;

        if (!context.Store.TryGetValue(message.Key, out KeyValueEntry? entry))
        {
            exists = false;
            KeyValueEntry? newEntry = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Durability == KeyValueDurability.Persistent)
            {
                ReadOnlyKeyValueEntry? row;

                if (preloadedHeads is not null && preloadedHeads.TryGetValue(message.Key, out ReadOnlyKeyValueEntry? preloadedRow))
                    row = preloadedRow;
                else
                {
                    row = await context.BackendReadScheduler.EnqueueBatchableTask(message.PartitionId, message.Key, context.PointReadExecutor);
                    readFromBackend = true;
                }

                newEntry = KeyValueEntry.FromRow(row);
                if (newEntry is not null)
                {
                    newEntry.FlushedRevision = newEntry.Revision; // already persisted
                    if (newEntry.State is KeyValueState.Deleted or KeyValueState.Undefined)
                    {
                        newEntry.Value = null;
                        exists = false;
                    }
                    else
                    {
                        if (newEntry.Expires != HLCTimestamp.Zero && newEntry.Expires - currentTime < TimeSpan.Zero)
                        {
                            newEntry.State = KeyValueState.Deleted;
                            newEntry.Value = null;
                            exists = false;
                        }
                        else
                        {
                            exists = true;
                        }
                    }
                }
            }

            newEntry ??= new() { Bucket = GetBucket(message.Key), State = KeyValueState.Undefined, Revision = -1 };

            entry = newEntry;

            context.InsertStoreEntry(message.Key, newEntry);
        }
        else
        {
            if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
            {
                entry.State = KeyValueState.Deleted;
                context.EnqueueTombstone(message.Key);
            }

            if (entry.State is KeyValueState.Deleted or KeyValueState.Undefined)
            {
                entry.Value = null;
                exists = false;
            }
        }

        // Deferred-settlement writer visibility: a foreign durable prepared intent covering this key may hold a
        // committed value not yet materialized into local MVCC. Resolve its canonical outcome before the write
        // proceeds — materialize a committed value into the entry (so the new revision and conditional flags are
        // based on it), treat an undecided intent as a live conflict, and ignore an aborted one. No-op off the
        // durable-intent path (null store).
        //
        // Every denial below names the transaction the writer is waiting behind, so a caller that reads the holder
        // can apply wait-die instead of waiting out its whole deadline against a writer that will not yield. The
        // answer type says what kind of wait it is: an undecided durable intent is lag — its coordinator is inside
        // its decision window and holds no further waits, so a deadlock through it is impossible — and answers
        // WaitingForReplication; a live lock conflict answers MustRetry, which a client bounds with its deadlock
        // budget.
        KeyValueEntry? resolvedEntry = entry;
        if (ForeignIntentWriteResolver.Resolve(
                context, message.Key, message.TransactionId, ref resolvedEntry, ApplyCommittedHead,
                out HLCTimestamp blockingIntentOwner, message.ForeignDecisionHint)
            == ForeignIntentWriteDecision.MustRetry)
            return (KeyValueResponse.Denied(KeyValueResponseType.WaitingForReplication, blockingIntentOwner, WriteDenialCause.DurableIntent), entry, exists, readFromBackend);

        entry = resolvedEntry!;
        exists = entry.State is not (KeyValueState.Deleted or KeyValueState.Undefined);

        // Converge a head parked behind an in-flight operation before staging anything: a write
        // that assigns its revision from a stale resident head would collide with the committed
        // history the parked head carries.
        if (entry.PendingCommittedHead is not null)
        {
            TryDrainPendingCommittedHead(message.Key, entry, currentTime);
            exists = entry.State is not (KeyValueState.Deleted or KeyValueState.Undefined);
        }

        // Validate if there's an active replication enty on the key/value entry
        // clients must retry operations to make sure the entry is fully replicated
        // before modifying the entry
        if (entry.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return (KeyValueStaticResponses.WaitingForReplicationResponse, entry, exists, readFromBackend);

            entry.ReplicationIntent = null;
        }

        // Validate if there's an exclusive key acquired on the lock and whether it is expired
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (entry.WriteIntent != null)
        {
            if (entry.WriteIntent.TransactionId != message.TransactionId)
            {
                if (KeyValueWriteIntentLease.IsLive(context, message.Key, entry.WriteIntent, currentTime))
                {
                    // A holder whose transaction is already durably decided is not a live conflict: under deferred
                    // settlement its intent only survives until the resolution clears it, so the writer waits for
                    // that settlement instead of spending its deadlock budget against a lock nobody is holding.
                    if (IsAwaitingSettlement(message.Key, entry.WriteIntent.TransactionId, message.ForeignDecisionHint))
                        return (KeyValueResponse.Denied(KeyValueResponseType.WaitingForReplication, entry.WriteIntent.TransactionId, WriteDenialCause.SettlingWriteIntent), entry, exists, readFromBackend);

                    if (RequesterMayTakeOverYieldingIntent(message) && entry.WriteIntent.Yielding)
                    {
                        if (entry.WriteIntent.Pinned)
                        {
                            Transactions.DurableTransactionMetrics.PinnedYieldingWaits.Add(1);
                            return (KeyValueStaticResponses.WaitingForReplicationResponse, entry, exists, readFromBackend);
                        }

                        if (!IsStealableYieldingIntent(message.Key, entry.WriteIntent))
                            return (RefuseForLiveIntent(message, entry), entry, exists, readFromBackend);

                        if (takeOverYielding)
                            TakeOverYieldingIntent(message, message.Key, entry, currentTime);
                    }
                    else
                    {
                        return (RefuseForLiveIntent(message, entry), entry, exists, readFromBackend);
                    }
                }
                else
                {
                    entry.WriteIntent = null;
                }
            }
        }

        // Validate if there's a prefix lock acquired on the bucket
        // if we find expired write intents we can remove it to allow new transactions to proceed
        if (entry.Bucket is not null && context.LocksByPrefix.Count > 0 && context.LocksByPrefix.TryGetValue(entry.Bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (KeyValueWriteIntentLease.IsLive(context, entry.Bucket, intent, currentTime))
                    return (KeyValueResponse.Denied(KeyValueResponseType.MustRetry, intent.TransactionId), entry, exists, readFromBackend);

                context.LocksByPrefix.Remove(entry.Bucket);
            }
        }

        // Validate if the key falls within any active range lock from another transaction
        if (RangeLockChecks.KeyCoveredByForeignRangeLock(context, message.Key, entry.Bucket, message.TransactionId, currentTime, out HLCTimestamp rangeLockHolder))
            return (KeyValueResponse.Denied(KeyValueResponseType.MustRetry, rangeLockHolder), entry, exists, readFromBackend);

        return (null, entry, exists, readFromBackend);
    }

    /// <summary>
    /// The answer to a write refused by the live write intent of another transaction that the writer cannot take
    /// over: <c>MustRetry</c> with the holder, so the caller can wait for the holder's outcome. A transactional
    /// write that asked for it (<see cref="KeyValueFlags.AbortWhenDoomed"/>) answers <c>Aborted</c> instead when it
    /// can no longer commit: its transaction read the key and staged nothing there, so its read pin is below the
    /// holder's write. If the holder commits, the pin is stale and the write aborts at its next attempt; only a
    /// rollback of the holder lets it through. Waiting for that outcome costs the holder's whole commit and almost
    /// always ends in the same abort.
    /// </summary>
    private static KeyValueResponse RefuseForLiveIntent(KeyValueRequest message, KeyValueEntry entry)
    {
        KeyValueWriteIntent holder = entry.WriteIntent!;

        if ((message.Flags & KeyValueFlags.AbortWhenDoomed) != 0
            && message.TransactionId != HLCTimestamp.Zero
            && entry.MvccEntries is not null
            && entry.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? own)
            && own.StagedUnder is null)
        {
            IntentWaitMetrics.DoomedSetAborts.Add(1);
            return KeyValueStaticResponses.AbortedResponse;
        }

        return KeyValueResponse.Denied(KeyValueResponseType.MustRetry, holder.TransactionId);
    }

    /// <summary>
    /// Whether a transaction older than <paramref name="writer"/> has read the key and is still running: it holds a
    /// read pin on the entry, staged nothing there, its session is open in this process, and this actor recorded
    /// no outcome for it. A writer that already staged the key is past this check. See
    /// <see cref="KeyValueFlags.YieldToOlderReaders"/>.
    /// </summary>
    private bool HasOlderLiveReader(KeyValueEntry entry, HLCTimestamp writer)
    {
        if (entry.MvccEntries is null)
            return false;

        if (entry.MvccEntries.TryGetValue(writer, out KeyValueMvccEntry? own) && own.StagedUnder is not null)
            return false;

        foreach ((HLCTimestamp reader, KeyValueMvccEntry pin) in entry.MvccEntries)
        {
            if (pin.StagedUnder is not null || reader.CompareTo(writer) >= 0)
                continue;

            if (Transactions.LiveTransactions.IsOpen(reader) && !context.WasCommittedHere(reader) && !context.WasRolledBackHere(reader))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Applies an ephemeral (non-persistent) set inline: no Raft proposal, mutate the in-memory entry and
    /// return. Split out so the persistent and ephemeral tails of <see cref="Execute"/> stay readable.
    /// </summary>
    private KeyValueResponse ApplyEphemeralSet(KeyValueRequest message, KeyValueEntry entry, KeyValueProposal proposal)
    {
        ApplyCommittedHead(entry, proposal, message.TransactionId);
        entry.FlushedRevision = entry.Revision; // ephemeral: no disk, always clean

        return new(KeyValueResponseType.Set, entry.Revision);
    }

    /// <summary>
    /// Transactional set (an MVCC-staged write under an open transaction): stamps the write from this node's
    /// clock and stages it.
    /// </summary>
    private ValueTask<KeyValueResponse> ExecuteTransactional(KeyValueRequest message)
    {
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.ReceiveEvent(context.Raft.GetLocalNodeId(), message.TransactionId);

        return ExecuteTransactional(message, currentTime, null);
    }

    /// <summary>
    /// Whether <paramref name="message"/>'s condition flags refuse the write over a key that <paramref name="exists"/>
    /// with <paramref name="value"/> and <paramref name="revision"/> in the writer's view.
    /// </summary>
    private static bool FailsCondition(KeyValueRequest message, bool exists, byte[]? value, long revision) =>
        (message.Flags & KeyValueFlags.SetIfExists) != 0 && !exists ||
        (message.Flags & KeyValueFlags.SetIfNotExists) != 0 && exists ||
        (message.Flags & KeyValueFlags.SetIfEqualToValue) != 0 && !((ReadOnlySpan<byte>)value).SequenceEqual(message.CompareValue) ||
        (message.Flags & KeyValueFlags.SetIfEqualToRevision) != 0 && revision != message.CompareRevision;

    /// <summary>
    /// Evaluates a transactional set without staging it: every refusal the staged write would meet (a key this
    /// yielding transaction already lost, a live replication intent, a foreign lock or intent, a committed head past
    /// the transaction's copy, a failed condition) is answered exactly as <see cref="ExecuteTransactional(KeyValueRequest, HLCTimestamp, Dictionary{string, ReadOnlyKeyValueEntry?}?)"/>
    /// answers it, and null means the write would be staged. Touches no MVCC entry and no write intent, so a batch
    /// can check every item before it stages any; a stealable yielding intent counts as no conflict and is taken
    /// over by the stage that follows in the same turn.
    /// </summary>
    internal async ValueTask<KeyValueResponse?> CheckTransactional(KeyValueRequest message, HLCTimestamp currentTime, Dictionary<string, ReadOnlyKeyValueEntry?>? preloadedHeads)
    {
        if (context.HasYieldedIntent(message.Key, message.TransactionId))
        {
            Transactions.DurableTransactionMetrics.RecordYieldAbortAtFollowUp();
            return KeyValueStaticResponses.AbortedResponse;
        }

        (KeyValueResponse? terminal, KeyValueEntry entry, _, _) = await LoadAndValidateEntry(message, currentTime, preloadedHeads, takeOverYielding: false);
        if (terminal is not null)
            return terminal;

        // The transaction's own view of the key: its staged copy when it has one, else the committed head. The
        // staged write would copy the head into a new MVCC entry, so the two views evaluate the same way.
        byte[]? value;
        long revision;
        HLCTimestamp expires;
        HLCTimestamp lastModified;
        KeyValueState state;

        if (entry.MvccEntries is not null && entry.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? mvccEntry))
        {
            if (entry.Revision > mvccEntry.Revision) // early conflict detection
                return KeyValueStaticResponses.AbortedResponse;

            value = mvccEntry.Value;
            revision = mvccEntry.Revision;
            expires = mvccEntry.Expires;
            lastModified = mvccEntry.LastModified;
            state = mvccEntry.State;
        }
        else
        {
            value = entry.Value;
            revision = entry.Revision;
            expires = entry.Expires;
            lastModified = entry.LastModified;
            state = entry.State;
        }

        bool exists = state is not (KeyValueState.Deleted or KeyValueState.Undefined)
            && !(expires != HLCTimestamp.Zero && expires - currentTime < TimeSpan.Zero);

        if (!exists)
            value = null;

        if (FailsCondition(message, exists, value, revision))
            return new(KeyValueResponseType.NotSet, revision, lastModified);

        return null;
    }

    /// <summary>
    /// Stages a transactional set stamped from <paramref name="currentTime"/>: shares only the entry load + lock/intent
    /// validation with the non-transactional path. A batch passes one stamp for every item it stages in its turn, so
    /// each item is evaluated at the time its check ran.
    /// </summary>
    internal async ValueTask<KeyValueResponse> ExecuteTransactional(KeyValueRequest message, HLCTimestamp currentTime, Dictionary<string, ReadOnlyKeyValueEntry?>? preloadedHeads)
    {
        // A yielding transaction that already lost this key learns of it here instead of at the finalize pin,
        // and never re-stages a value or recreates the intent for a key it can no longer commit.
        if (context.HasYieldedIntent(message.Key, message.TransactionId))
        {
            Transactions.DurableTransactionMetrics.RecordYieldAbortAtFollowUp();
            return KeyValueStaticResponses.AbortedResponse;
        }

        (KeyValueResponse? terminal, KeyValueEntry entry, bool exists, _) = await LoadAndValidateEntry(message, currentTime, preloadedHeads);
        if (terminal is not null)
            return terminal;

        if ((message.Flags & KeyValueFlags.YieldToOlderReaders) != 0 && HasOlderLiveReader(entry, message.TransactionId))
        {
            IntentWaitMetrics.OlderReaderYields.Add(1);
            return KeyValueStaticResponses.AbortedResponse;
        }

        // Temporarily store the value in the MVCC entry
        exists = true;
        entry.MvccEntries ??= new();

        if (!entry.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? mvccEntry))
        {
            bool mvccDictJustCreated = entry.MvccEntries.Count == 0;
            mvccEntry = new()
            {
                Value = entry.Value,
                Revision = entry.Revision,
                Expires = entry.Expires,
                LastUsed = entry.LastUsed,
                LastModified = entry.LastModified,
                State = entry.State
            };

            entry.MvccEntries.Add(message.TransactionId, mvccEntry);
            context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.MvccEntryAddedBytes(mvccDictJustCreated, mvccEntry.Value));
        }

        if (entry.Revision > mvccEntry.Revision) // early conflict detection
            return KeyValueStaticResponses.AbortedResponse;

        // The staged stamp becomes the commit's lower bound (the coordinator mints the commit timestamp above
        // it), so it must be later than the revision the transaction replaces, whatever clock stamped that.
        currentTime = StampAfter(currentTime, StagedBaseLastModified(entry, mvccEntry));

        HLCTimestamp newExpires = message.ExpiresMs > 0 ? (currentTime + message.ExpiresMs) : HLCTimestamp.Zero;

        if (mvccEntry.State is KeyValueState.Deleted or KeyValueState.Undefined)
        {
            mvccEntry.Value = null;
            exists = false;
        }
        else
        {
            if (mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero)
            {
                mvccEntry.State = KeyValueState.Deleted;
                mvccEntry.Value = null;
                exists = false;
            }
        }

        // Check if the value must not be changed according to flags
        if (FailsCondition(message, exists, mvccEntry.Value, mvccEntry.Revision))
            return new(KeyValueResponseType.NotSet, mvccEntry.Revision, mvccEntry.LastModified);

        mvccEntry.Value = message.Value;
        mvccEntry.Expires = newExpires;
        mvccEntry.Revision++;
        mvccEntry.LastUsed = currentTime;
        mvccEntry.LastModified = currentTime;
        mvccEntry.State = KeyValueState.Set;
        mvccEntry.NoRevision = (message.Flags & KeyValueFlags.SetNoRevision) != 0;

        // Set write intent so concurrent transactions detect this pending write during ValidateReadSet.
        // Without this, optimistic transactions writing different keys can both pass validation and commit,
        // producing write skew. The intent is cleared by TryReleaseExclusiveLock on abort or
        // overwritten by TryPrepareMutationsHandler on the commit path.
        entry.WriteIntent ??= new()
        {
            TransactionId = message.TransactionId,
            Expires = currentTime + context.Configuration.StagedWriteIntentLeaseMs,
            AcquiredAt = currentTime,
            Yielding = message.ConflictPolicy == TransactionConflictPolicy.Yield
        };

        // The intent is also what makes a snapshot read wait for this staged write; the commit-time probe
        // proves it held the key from here on (see KeyValueMvccEntry.StagedUnder).
        mvccEntry.StagedUnder ??= entry.WriteIntent;

        return new(KeyValueResponseType.Set, mvccEntry.Revision, currentTime);
    }
}

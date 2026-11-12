
using Nixie;
using Kommander.Time;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Represents a handler for attempting to retrieve key-value pairs.
/// </summary>
internal sealed class TryGetHandler : BaseHandler
{
    public TryGetHandler(KeyValueContext context) : base(context) { }

    public async ValueTask<KeyValueResponse> Execute(KeyValueRequest message)
    {
        // ── By-revision path ─────────────────────────────────────────────────────────────
        // Requires an entry loaded from cache/disk before checking; detached via the async resumable-read path.
        if (message.CompareRevision > -1)
            return ExecuteByRevision(message);

        // A statement read without a timestamp has no snapshot to read at and would silently become a pinning
        // latest read, so the request is refused rather than reinterpreted.
        if (!KeyValueReadModeChecks.IsWellFormed(message.ReadMode, message.ReadTimestamp))
            return KeyValueStaticResponses.InvalidInputResponse;

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock
            .TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        long intentWaitDeadline = IntentWaitDeadline(message);

        PointReadStep step = await ResolvePointRead(message, currentTime, preloadedHeads: null, intentWaitDeadline);

        return step.Detach switch
        {
            PointReadDetach.None => step.ToResponse()!,
            PointReadDetach.IntentWait => ParkFirstRead(message, intentWaitDeadline, currentTime),
            PointReadDetach.LatestMiss => DispatchLatestMiss(message, currentTime),
            PointReadDetach.SnapshotHydrate => await DispatchSnapshotRead(message, KeyValueResponseType.Get, currentTime, hydrate: true, asOfCeiling: -1),
            _ => await DispatchSnapshotRead(message, KeyValueResponseType.Get, currentTime, hydrate: false, asOfCeiling: step.AsOfCeiling),
        };
    }

    /// <summary>
    /// Stage 1 of a latest or snapshot point read (not by revision): every in-memory rule that decides the answer
    /// for <c>message.Key</c> on this actor, run on the actor thread. It either answers, or says which backend
    /// read the key needs (see <see cref="PointReadDetach"/>) and leaves the dispatch of that read to the caller, so
    /// a single read and a batched probe apply exactly the same rules to a key. It never defers a reply itself.
    /// <para>A transactional latest read, and a read that the durable-intent overlay answers, load a non-resident
    /// head inline. <paramref name="preloadedHeads"/> holds heads the caller already loaded from the backend for
    /// that purpose; a head is taken from it at most once.</para>
    /// <para><paramref name="intentWaitDeadline"/> is the <see cref="Environment.TickCount64"/> until which the read
    /// may park behind a live write intent of another transaction (see <see cref="MustParkBehindIntent"/>), or zero
    /// when the caller cannot park it. A read that must park answers <see cref="PointReadDetach.IntentWait"/> and
    /// changes nothing on the key.</para>
    /// </summary>
    internal async ValueTask<PointReadStep> ResolvePointRead(
        KeyValueRequest message,
        HLCTimestamp currentTime,
        Dictionary<string, ReadOnlyKeyValueEntry?>? preloadedHeads,
        long intentWaitDeadline = 0)
    {
        // ── Stage 1: in-memory pre-checks (actor thread, synchronous) ────────────────────
        bool inCache = context.Store.TryGetValue(message.Key, out KeyValueEntry? entry);

        // Converge a head that was parked behind an in-flight operation before serving anything:
        // this read is gated by a quorum-confirmed leadership check, so a resident entry behind
        // the node's own applied committed state must not be treated as authoritative.
        if (entry?.PendingCommittedHead is not null)
            TryDrainPendingCommittedHead(message.Key, entry, currentTime);

        // Durable-intent read visibility: a foreign prepared intent covering this key may have a committed value
        // that has not yet materialized locally (deferred settlement). Consult the canonical outcome before the
        // ordinary read paths. Empty store (durable-intent path disabled) ⇒ null ⇒ no effect.
        //
        // But a transaction that has already snapshotted or written this key in its own MVCC must see that view
        // (read-your-own-write / snapshot isolation), never a concurrent foreign intent: otherwise a
        // committed-but-unsettled intent from another transaction would preempt this transaction's own staged
        // write and serve the prior value. The transactional MVCC path below is authoritative in that case.
        //
        // A transactional latest read without an MVCC entry yet skips the overlay too: the MVCC path records its
        // pin from the committed intent (DurableSnapshotSource), which the overlay would answer from and leave no
        // pin behind — and without a pin a later read or write of the key cannot detect a competing commit early.
        //
        // A statement read (one timestamp per statement, see KeyValueReadMode.StatementSnapshot) owns only its
        // staged writes: a read pin an earlier statement left on the key is neither served nor checked, so for it
        // the overlay and the snapshot rules below run at the read timestamp exactly as for a key it never read.
        KeyValueMvccEntry? ownMvcc = null;
        if (message.TransactionId != HLCTimestamp.Zero && entry?.MvccEntries is { } readerMvcc)
            readerMvcc.TryGetValue(message.TransactionId, out ownMvcc);

        bool statementRead = KeyValueReadModeChecks.IsStatementRead(message.ReadMode, message.TransactionId, message.ReadTimestamp);
        bool readerHasOwnMvcc = ownMvcc is not null && (!statementRead || ownMvcc.StagedUnder is not null);
        bool transactionalLatestRead = message.TransactionId != HLCTimestamp.Zero && message.ReadTimestamp.IsNull();

        PreparedIntent? foreignIntent = null;
        ReadVisibilityAction foreignAction = ReadVisibilityAction.UseExisting;
        if (!readerHasOwnMvcc
            && !transactionalLatestRead
            && context.PreparedIntentStore?.Get(message.Key) is { } candidateIntent
            && candidateIntent.TransactionId != message.TransactionId)
        {
            HLCTimestamp readTs = message.ReadTimestamp.IsNull() ? HLCTimestamp.Zero : message.ReadTimestamp;
            ReadVisibilityAction action = DurableReadVisibility.Resolve(context, candidateIntent, readTs, message.ForeignDecisionHint);

            // A later committed write may have moved the head past a committed intent (a non-transactional write
            // proceeds over a committed-but-unsettled intent, which lingers until settlement). On a persistent cache
            // miss only the backend knows the head, so load it before serving the intent. The intent covers the
            // committed history up to its own revision, so a row behind it is ordinary settlement lag.
            if (action == ReadVisibilityAction.UseIntentValue
                && !inCache && message.Durability == KeyValueDurability.Persistent)
            {
                (KeyValueEntry? hydrated, bool stale) = await HydrateForRead(message.Key, currentTime, candidateIntent.Revision, preloadedHeads);
                if (stale)
                    return KeyValueStaticResponses.MustRetryResponse;

                if (hydrated is not null)
                {
                    entry = hydrated;
                    inCache = true;
                }
            }

            if (!ResidentHeadSupersedesIntent(entry, candidateIntent, readTs))
            {
                foreignIntent = candidateIntent;
                foreignAction = action;
            }
        }

        if (foreignIntent is not null)
        {
            switch (foreignAction)
            {
                case ReadVisibilityAction.Retry:
                    return KeyValueStaticResponses.WaitingForReplicationResponse;

                case ReadVisibilityAction.UseIntentValue:
                    // The committed intent is visible to this snapshot, but a third transaction's staged write
                    // may still commit at or below it: wait for that writer exactly as the ordinary path does,
                    // instead of answering now and differently once it commits.
                    if (SnapshotMustWaitBehindOverlay(message, entry, foreignIntent, currentTime))
                        return KeyValueStaticResponses.WaitingForReplicationResponse;

                    // A committed delete, or a committed value whose TTL has elapsed, is not live — treat it as
                    // does-not-exist, matching the ordinary read's expiry filter rather than serving a dead value.
                    if (foreignIntent.State == KeyValueState.Deleted
                        || PreparedIntentVisibility.IsExpired(foreignIntent, currentTime))
                        return KeyValueStaticResponses.DoesNotExistContextResponse;

                    return PointReadStep.Serve(new ReadOnlyKeyValueEntry(
                        foreignIntent.Value, foreignIntent.Revision, foreignIntent.Expires,
                        currentTime, foreignIntent.CommitTimestamp, foreignIntent.State));

                case ReadVisibilityAction.UseExisting:
                default:
                    break; // fall through to the ordinary read paths
            }
        }

        // A reader holding its own MVCC view skipped the overlay above; its pin must still not outlive a committed
        // head that is waiting for settlement (see PinIsBehindUnsettledCommit).
        if (readerHasOwnMvcc && PinIsBehindUnsettledCommit(message, entry))
            return KeyValueStaticResponses.AbortedResponse;

        // Validate active replication intent — client must retry until the entry is
        // fully replicated before reading it.
        if (entry?.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.WaitingForReplicationResponse;

            entry.ReplicationIntent = null;
        }

        // A live write intent from another transaction is not a blocking condition for reads
        // UNLESS the read carries a snapshot timestamp and the intent's pending commit ts is fixed
        // at-or-before that snapshot (safe-time wait). A commit that can still be moved is pushed
        // above the snapshot instead, and the read falls through to the committed head.
        if (entry?.WriteIntent != null)
        {
            if (entry.WriteIntent.TransactionId != message.TransactionId)
            {
                if (!KeyValueWriteIntentLease.IsLive(context, message.Key, entry.WriteIntent, currentTime))
                    entry.WriteIntent = null;
                else if (!message.ReadTimestamp.IsNull()
                         && KeyValueWriteIntentSafeTime.MustWait(entry, entry.WriteIntent, message.ReadTimestamp))
                    return KeyValueStaticResponses.WaitingForReplicationResponse;
                else if (intentWaitDeadline != 0
                         && MustParkBehindIntent(message.Key, message.TransactionId, message.IntentWait, message.ForeignDecisionHint, entry, currentTime, intentWaitDeadline))
                {
                    parkEntry = entry;
                    return PointReadStep.Detached(PointReadDetach.IntentWait, -1);
                }
                // live write intent from another tx and outside our snapshot window: fall through
            }
        }

        // Prefix lock check. The empty-table guard also skips deriving the bucket substring —
        // prefix locks are rare, and the common workload never pays for the probe.
        if (context.LocksByPrefix.Count > 0)
        {
            string? bucket = entry is not null ? entry.Bucket : GetBucket(message.Key);

            if (bucket is not null && context.LocksByPrefix.TryGetValue(bucket, out KeyValueWriteIntent? intent))
            {
                if (intent.TransactionId != message.TransactionId)
                {
                    if (KeyValueWriteIntentLease.IsLive(context, bucket, intent, currentTime))
                        return new KeyValueResponse(KeyValueResponseType.MustRetry, 0);

                    context.LocksByPrefix.Remove(bucket);
                }
            }
        }

        // ── Transactional MVCC path (never detaches) ────────────────────────────
        // Own-write visibility and read-your-writes are served from in-memory MVCC state.
        if (message.TransactionId != HLCTimestamp.Zero && message.ReadTimestamp.IsNull())
        {
            if (!inCache && message.Durability == KeyValueDurability.Persistent)
            {
                // Transactional reads must never detach: load synchronously so the MVCC
                // snapshot always reflects the committed state of the key.
                (KeyValueEntry? hydrated, bool stale) = await HydrateForRead(
                    message.Key, currentTime,
                    CommittedForeignIntentRevision(message.Key, message.TransactionId, message.ForeignDecisionHint),
                    preloadedHeads);

                // A hydrated row below this node's committed-head memory must not become a
                // transaction's MVCC base: the missing committed writes would be silently
                // discarded by the read-modify-write built on it. Refuse and let the scheduled
                // convergence repair advance the local state before the client's retry. A committed
                // foreign intent the read pins instead covers the history up to its revision.
                if (stale)
                    return KeyValueStaticResponses.MustRetryResponse;

                if (hydrated is not null)
                    entry = hydrated;
            }

            KeyValueMvccEntry? mvccEntry = null;
            if (entry?.MvccEntries is null || !entry.MvccEntries.TryGetValue(message.TransactionId, out mvccEntry))
            {
                // First read of the key by this transaction: pin the committed state it observes. A committed
                // but unsettled foreign intent supersedes a resident head that does not include it yet, and an
                // undecided one means the read must wait for the decision.
                SnapshotDecision decision = DurableSnapshotSource.Resolve(
                    context, message.Key, message.TransactionId, entry, currentTime,
                    out KeyValueMvccEntry intentSnapshot, message.ForeignDecisionHint);

                if (decision == SnapshotDecision.Retry)
                    return KeyValueStaticResponses.WaitingForReplicationResponse;

                if (entry is null)
                {
                    entry = new() { Bucket = GetBucket(message.Key), State = KeyValueState.Undefined, Revision = -1 };
                    context.InsertStoreEntry(message.Key, entry);
                }

                mvccEntry = decision == SnapshotDecision.UseIntent
                    ? intentSnapshot
                    : new()
                    {
                        Value = entry.Value,
                        Revision = entry.Revision,
                        Expires = entry.Expires,
                        LastUsed = entry.LastUsed,
                        LastModified = entry.LastModified,
                        State = entry.State
                    };

                entry.MvccEntries ??= new();
                bool mvccDictJustCreated = entry.MvccEntries.Count == 0;
                entry.MvccEntries.Add(message.TransactionId, mvccEntry);
                context.AdjustEstimatedEntryBytes(entry,
                    KeyValueStoreAccounting.MvccEntryAddedBytes(mvccDictJustCreated, mvccEntry.Value));
            }

            if (entry.Revision > mvccEntry.Revision)
                return KeyValueStaticResponses.AbortedResponse;

            if (mvccEntry.State is KeyValueState.Undefined or KeyValueState.Deleted
                || (mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            // For the read floor of the transaction (see TransactionReadFloors): how long the served revision has
            // been the committed head, as far as this actor can vouch. A pin taken from a committed intent that
            // the resident head does not include yet, or a staged write of the transaction, states nothing.
            long servedTicks = System.Diagnostics.Stopwatch.GetTimestamp();
            long headSeenTicks = 0;

            if (mvccEntry.StagedUnder is not null)
            {
                headSeenTicks = ReadOnlyKeyValueEntry.OwnStagedWrite;
            }
            else if (mvccEntry.Revision == entry.Revision)
            {
                // Stamped at the head's install; an entry whose stamp belongs to another revision states now.
                if (entry.HeadSeenRevision != entry.Revision)
                {
                    entry.HeadSeenRevision = entry.Revision;
                    entry.HeadSeenTicks = servedTicks;
                }

                headSeenTicks = entry.HeadSeenTicks;
            }

            return PointReadStep.Serve(new ReadOnlyKeyValueEntry(
                mvccEntry.Value, mvccEntry.Revision, mvccEntry.Expires,
                mvccEntry.LastUsed, mvccEntry.LastModified, mvccEntry.State)
            {
                HeadSeenTicks = headSeenTicks,
                ServedTicks = servedTicks
            });
        }

        // ── Statement read of a key this transaction staged a write on ──────────────────
        // The staged write is the transaction's own view of the key whatever the read timestamp says, and it is
        // answered exactly as the latest path answers it: a head that moved past the staged base means the
        // transaction already lost the key, which is better learned here than at commit. Only a staged write
        // qualifies (StagedUnder is set by every staged set, tombstone and extend); a pin was skipped above and
        // the key reads as committed state at the timestamp below.
        if (statementRead && ownMvcc is { StagedUnder: not null })
        {
            if (entry!.Revision > ownMvcc.Revision)
                return KeyValueStaticResponses.AbortedResponse;

            if (ownMvcc.State is KeyValueState.Undefined or KeyValueState.Deleted
                || (ownMvcc.Expires != HLCTimestamp.Zero && ownMvcc.Expires - currentTime < TimeSpan.Zero))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            return PointReadStep.Serve(new ReadOnlyKeyValueEntry(
                ownMvcc.Value, ownMvcc.Revision, ownMvcc.Expires,
                ownMvcc.LastUsed, ownMvcc.LastModified, ownMvcc.State));
        }

        // ── Snapshot visibility: serve the revision at-or-before readTimestamp ──────────
        // Anything the snapshot needs from the persisted history — the head on a cache miss, or the
        // archived revision the in-memory archive no longer holds — is read off the actor via
        // SnapshotReadContinuation. The history read walks a chain that grows with every write to
        // the key; done inline it parked every key on this actor behind one cold read.
        if (!message.ReadTimestamp.IsNull())
        {
            if (!inCache && message.Durability == KeyValueDurability.Persistent)
                return PointReadStep.Detached(PointReadDetach.SnapshotHydrate, -1);

            if (entry is not null && entry.LastModified > message.ReadTimestamp)
            {
                if (!entry.TryGetRevisionAtOrBefore(message.ReadTimestamp,
                        out long snapRevision, out KeyValueRevisionEntry snapshot))
                {
                    // A head jump skipped revisions this entry never archived, and their flush
                    // requests may still be queued: the persisted history cannot answer for the
                    // skipped window yet. Fail closed — MustRetry is safe to retry and the window
                    // closes when the queued flushes are acknowledged.
                    if (entry.SnapshotAtRiskFromUnflushedGap(message.ReadTimestamp))
                        return KeyValueStaticResponses.MustRetryResponse;

                    // A revision below the head that the archive no longer holds may still be queued for
                    // the background writer, in which case the persisted history would answer with an
                    // older row now and the committed one after the flush. Fail closed until it lands.
                    if (UnflushedHistoryFence.HistoryMayLag(context.UnflushedWrites, message.Key, entry.Revisions, entry.Revision - 1))
                        return KeyValueStaticResponses.MustRetryResponse;

                    // In-memory archive trimmed the as-of revision; the persisted revision history
                    // answers. Correct because trimming drops the lowest revision numbers, so an
                    // in-memory miss means the true as-of answer (if any) is older and only on disk.
                    return PointReadStep.Detached(PointReadDetach.SnapshotHistory, entry.Revision - 1);
                }

                if (snapshot.State is KeyValueState.Deleted or KeyValueState.Undefined
                    || (snapshot.Expires != HLCTimestamp.Zero
                        && snapshot.Expires - currentTime < TimeSpan.Zero))
                    return KeyValueStaticResponses.DoesNotExistContextResponse;

                return PointReadStep.Serve(new ReadOnlyKeyValueEntry(
                    snapshot.Value, snapRevision, snapshot.Expires,
                    currentTime, snapshot.LastModified, snapshot.State));
            }
        }

        // ── Non-transactional persistent cache miss → the caller reads the backend ──────────
        if (entry is null && message.Durability == KeyValueDurability.Persistent)
            return PointReadStep.Detached(PointReadDetach.LatestMiss, -1);

        // ── In-memory result: state/expiry check then reply ───────────────────────────────
        if (entry is null
            || entry.State is KeyValueState.Undefined or KeyValueState.Deleted
            || (entry.Expires != HLCTimestamp.Zero
                && entry.Expires - currentTime < TimeSpan.Zero))
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        context.TouchEntry(entry, currentTime);

        // A resident hit with no live published record: the record was dropped behind a pending apply that then
        // changed nothing, or was never published. Publish it at the end of this turn so the next read of the key
        // can be answered off the actor.
        if (entry.Published is null)
            context.QueueHeadPublication(entry);

        return PointReadStep.Serve(new ReadOnlyKeyValueEntry(
            entry.Value, entry.Revision, entry.Expires,
            entry.LastUsed, entry.LastModified, entry.State));
    }

    // The entry the last ResolvePointRead that answered IntentWait parks on, and the exit the last wait decision
    // recorded. Actor thread only: read right after the call that set them, in the same turn.
    private KeyValueEntry? parkEntry;

    private IntentWaitExit? lastWaitExit;

    /// <summary>
    /// The <see cref="Environment.TickCount64"/> until which <paramref name="message"/> may park behind a live
    /// write intent of another transaction, or zero when it cannot park: the read did not ask to wait, the server
    /// turned the wait off, it is not a transactional latest read, or it is served inline from another request's
    /// turn (whose reply is not this read's to defer).
    /// </summary>
    internal long IntentWaitDeadline(KeyValueRequest message)
    {
        int limitMs = Math.Min(message.IntentWait.LimitMs, context.Configuration.MaxIntentWaitMs);

        if (limitMs <= 0
            || message.TransactionId == HLCTimestamp.Zero
            || !message.ReadTimestamp.IsNull()
            || message.Inline is not null
            || context.ActorContext is null
            || !context.ActorContext.Reply.HasValue)
            return 0;

        return Environment.TickCount64 + limitMs;
    }

    /// <summary>
    /// Whether the first transactional latest read of <paramref name="key"/> by <paramref name="transactionId"/>
    /// parks behind the entry's write intent instead of pinning the committed head below it. It parks when the
    /// intent is another transaction's, live, not yielding (a yielding holder gives the key up to a writer, so the
    /// reader is not doomed by it), and not already decided (a decided holder is answered from its decision, as
    /// before), and the transaction has no pin or staged write on the key. Three exits serve the read as if it had
    /// not asked to wait, and are counted: the wait reached its deadline, a younger requester that holds locks
    /// (wait-die), and no room under the bounds.
    /// </summary>
    internal bool MustParkBehindIntent(
        string key,
        HLCTimestamp transactionId,
        KeyValueIntentWait wait,
        Transactions.Data.ForeignDecisionHint hint,
        KeyValueEntry? entry,
        HLCTimestamp currentTime,
        long deadline)
    {
        lastWaitExit = null;

        if (deadline == 0 || entry?.WriteIntent is not { } intent || intent.TransactionId == transactionId)
            return false;

        if (entry.MvccEntries is not null && entry.MvccEntries.ContainsKey(transactionId))
            return false;

        if (intent.Yielding
            || !KeyValueWriteIntentLease.IsLive(context, key, intent, currentTime)
            || IsAwaitingSettlement(key, intent.TransactionId, hint))
            return false;

        IntentWaitExit exit;

        if (Environment.TickCount64 >= deadline)
            exit = IntentWaitExit.Limit;
        else if (wait.HoldsLocks && transactionId.CompareTo(intent.TransactionId) > 0)
            exit = IntentWaitExit.WaitDie;
        else if (!context.IntentWaits.HasRoom(entry))
            exit = IntentWaitExit.Bound;
        else
            return true;

        lastWaitExit = exit;
        IntentWaitMetrics.RecordExit(exit);
        return false;
    }

    /// <summary>
    /// Parks a read that stage 1 found behind a live foreign write intent, the first time: the read gets its own copy
    /// of the request, its reply is deferred, and the actor goes on with its mailbox.
    /// </summary>
    private KeyValueResponse ParkFirstRead(KeyValueRequest message, long deadline, HLCTimestamp currentTime)
    {
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext = context.ActorContext;

        KeyValueRequest copy = new(
            KeyValueRequestType.TryGet, message.TransactionId, HLCTimestamp.Zero, message.Key, null, null, -1,
            KeyValueFlags.None, 0, HLCTimestamp.Zero, message.Durability, 0, message.PartitionId, default)
        {
            ReadMode = message.ReadMode,
            ForeignDecisionHint = message.ForeignDecisionHint,
            IntentWait = message.IntentWait
        };

        ParkedIntentRead read = new(copy, KeyValueReplyRef.From(actorContext.Reply!.Value), deadline, actorContext.Self.Send);

        context.IntentWaits.Park(read, parkEntry!, currentTime);
        parkEntry = null;

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    /// <summary>
    /// Runs a woken point read again from the start. Returns the answer, or null when the read parked again (the
    /// intent is still another live writer's, or a new one is, and the deadline allows).
    /// </summary>
    internal async ValueTask<KeyValueResponse?> ResumeParkedRead(ParkedIntentRead read)
    {
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        ClearWaitExit();

        PointReadStep step = await ResolvePointRead(read.Request, currentTime, preloadedHeads: null, read.DeadlineTick);

        if (step.Detach == PointReadDetach.IntentWait)
        {
            context.IntentWaits.Park(read, parkEntry!, currentTime);
            parkEntry = null;
            return null;
        }

        // A transactional latest read never detaches to the backend; anything else is retried by the caller.
        return step.Detach == PointReadDetach.None ? step.ToResponse() : KeyValueStaticResponses.MustRetryResponse;
    }

    /// <summary>The exit the last wait decision recorded, or null when it recorded none.</summary>
    internal IntentWaitExit? LastWaitExit => lastWaitExit;

    /// <summary>Forgets the exit of an earlier decision, before a read runs that may make none.</summary>
    internal void ClearWaitExit() => lastWaitExit = null;

    /// <summary>
    /// Loads a non-resident persistent head for a read that must compare against it before answering: from
    /// <paramref name="preloadedHeads"/> when the caller already read it from the backend, otherwise from the
    /// backend now. Either way the stale-hydration rule and the install are those of <see cref="BaseHandler.HydratePersistentHead"/>.
    /// </summary>
    private ValueTask<(KeyValueEntry? Entry, bool Stale)> HydrateForRead(
        string key, HLCTimestamp currentTime, long coveredThrough, Dictionary<string, ReadOnlyKeyValueEntry?>? preloadedHeads)
    {
        if (preloadedHeads is not null && preloadedHeads.Remove(key, out ReadOnlyKeyValueEntry? preloaded))
            return new(InstallHydratedHead(key, preloaded, currentTime, coveredThrough));

        return HydratePersistentHead(key, currentTime, coveredThrough);
    }

    /// <summary>
    /// Stage 2 dispatch of a non-transactional latest read whose key is not resident. Attaches the caller to a
    /// backend read already in flight for the key, or registers a new one, and defers the reply.
    /// </summary>
    private KeyValueResponse DispatchLatestMiss(KeyValueRequest message, HLCTimestamp currentTime)
    {
        // If a backend read for this key is already in flight, attach the caller's Promise to
        // the existing continuation (single-flight coalescing) instead of issuing a second
        // identical read. The first caller's ContinueWith will drive ResumeRead for everyone.
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext =
            context.ActorContext;

        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        KeyValueReplyRef promise = KeyValueReplyRef.From(actorContext.Reply.Value);

        // Coalesce: attach to the in-flight read if one exists for this key.
        // (key, -1, false) = latest-point TryGet; by-revision or TryExists reads use
        // a different key and will not coalesce onto this continuation.
        (string, long, bool) pointKey = (message.Key, -1L, false);
        if (context.PendingReads.TryGetValue(pointKey, out ReadContinuation? inflight))
        {
            if (!inflight.AddWaiter(promise))
                return KeyValueStaticResponses.MustRetryResponse;
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.WaitingForReplicationResponse;
        }

        // No read in flight — register and dispatch a new one.
        PointReadContinuation cont = new(message.Key, KeyValueResponseType.Get, promise);
        ArmReadDeadline(cont, currentTime);
        context.PendingReads[pointKey] = cont;
        int partitionId = ResolvePartition(message.Key);

        Task<ReadOnlyKeyValueEntry?> readTask;
        try
        {
            readTask = context.BackendReadScheduler.EnqueueBatchableTask(
                partitionId,
                message.Key,
                context.PointReadExecutor);
        }
        catch (Exception ex)
        {
            // Backpressure or scheduler rejection: clean up the registration, resolve all
            // waiters (just the primary here since no coalescing can have happened yet) with
            // MustRetry, and bypass Nixie's auto-reply.
            context.PendingReads.Remove(pointKey);
            context.Logger.LogWarning(
                "KeyValueActor/TryGet: read scheduler rejected enqueue for key {Key}: {Ex}",
                message.Key, ex.Message);
            cont.Resolve(KeyValueStaticResponses.MustRetryResponse);
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.MustRetryResponse;
        }

        _ = readTask.ContinueWith(t =>
        {
            if (!t.IsCompletedSuccessfully)
            {
                // Mark as faulted so Execute (stage 3) resolves all waiters with the fault's response:
                // MustRetry for a transient fault, StorageCorrupt for storage damage. We deliberately
                // still route through ResumeRead rather than calling Resolve / PendingReads.Remove
                // here: this callback runs on a thread-pool thread and PendingReads is actor-owned
                // state (no lock).
                cont.SetFaulted(t.Exception);
            }
            else
            {
                cont.DiskResult = t.Result;
            }
            actorContext.Self.Send(
                KeyValueRequestPool.RentResumeRead(cont));
        }, TaskScheduler.Default);

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }

    private KeyValueResponse ExecuteByRevision(KeyValueRequest message)
    {
        // Deferred-settlement by-revision visibility: the requested revision may be a foreign committed intent
        // that has not yet materialized (so it is absent from the store and disk). Serve it when committed and its
        // revision is exactly the one requested, retry while undecided, and ignore an aborted intent (its revision
        // was never committed). No-op off the durable-intent path.
        if (context.PreparedIntentStore?.Get(message.Key) is { } foreignIntent
            && foreignIntent.TransactionId != message.TransactionId
            && foreignIntent.Revision == message.CompareRevision)
        {
            switch (DurableReadVisibility.Resolve(context, foreignIntent, HLCTimestamp.Zero, message.ForeignDecisionHint))
            {
                case ReadVisibilityAction.Retry:
                    return KeyValueStaticResponses.WaitingForReplicationResponse;

                case ReadVisibilityAction.UseIntentValue:
                    return foreignIntent.State == KeyValueState.Deleted
                        ? KeyValueStaticResponses.DoesNotExistContextResponse
                        : new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                            foreignIntent.Value, foreignIntent.Revision, foreignIntent.Expires,
                            HLCTimestamp.Zero, foreignIntent.CommitTimestamp, foreignIntent.State));

                case ReadVisibilityAction.UseExisting:
                default:
                    break; // aborted: fall through to the ordinary store/disk lookup
            }
        }

        // ── Stage 1: in-memory shortcuts (no disk, no detach) ────────────────────────────────
        // Historical revisions are immutable. A cache-resident entry whose current Revision
        // already equals the request, or whose Revisions dict holds the target, can be served
        // immediately without a backend round-trip.
        context.Store.TryGetValue(message.Key, out KeyValueEntry? entry);

        if (entry is not null && entry.Revision == message.CompareRevision)
            return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                entry.Value, message.CompareRevision,
                HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set));

        if (entry?.Revisions != null &&
            entry.Revisions.TryGetValue(message.CompareRevision, out KeyValueRevisionEntry revisionEntry))
            return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                revisionEntry.Value, message.CompareRevision,
                HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set));

        if (message.Durability != KeyValueDurability.Persistent)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        // ── Stage 2 dispatch: detach GetKeyValueRevision off the actor mailbox ────────────────
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext =
            context.ActorContext;

        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        KeyValueReplyRef promise = KeyValueReplyRef.From(actorContext.Reply.Value);

        // (key, revision, false) = by-revision TryGet. The false flag keeps this slot separate
        // from a TryExists for the same (key, revision), which uses (key, revision, true).
        (string, long, bool) revKey = (message.Key, message.CompareRevision, false);

        if (context.PendingReads.TryGetValue(revKey, out ReadContinuation? inflight))
        {
            if (!inflight.AddWaiter(promise))
                return KeyValueStaticResponses.MustRetryResponse;
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.WaitingForReplicationResponse;
        }

        ByRevisionReadContinuation cont = new(
            message.Key, message.CompareRevision, KeyValueResponseType.Get, promise);
        ArmReadDeadline(cont, context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId()));
        context.PendingReads[revKey] = cont;
        int partitionId = ResolvePartition(message.Key);

        // Copy into locals before capturing: the deadline can resolve the continuation (and
        // complete the caller, which returns the pooled request) before the scheduler runs the
        // closure — capturing the request itself would read a cleared or re-rented message.
        string key = message.Key;
        long compareRevision = message.CompareRevision;

        Task<ReadOnlyKeyValueEntry?> readTask;
        try
        {
            readTask = context.BackendReadScheduler.EnqueueTask(
                partitionId,
                () => context.PersistenceBackend.GetKeyValueRevision(key, compareRevision));
        }
        catch (Exception ex)
        {
            context.PendingReads.Remove(revKey);
            context.Logger.LogWarning(
                "KeyValueActor/TryGetByRevision: read scheduler rejected enqueue for key {Key} rev {Rev}: {Ex}",
                message.Key, message.CompareRevision, ex.Message);
            cont.Resolve(KeyValueStaticResponses.MustRetryResponse);
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.MustRetryResponse;
        }

        _ = readTask.ContinueWith(t =>
        {
            if (!t.IsCompletedSuccessfully) cont.SetFaulted(t.Exception);
            else cont.DiskResult = t.Result;
            actorContext.Self.Send(
                KeyValueRequestPool.RentResumeRead(cont));
        }, TaskScheduler.Default);

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }
}

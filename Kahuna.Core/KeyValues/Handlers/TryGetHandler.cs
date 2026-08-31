
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

        // ── Stage 1: in-memory pre-checks (actor thread, synchronous) ────────────────────
        bool inCache = context.Store.TryGetValue(message.Key, out KeyValueEntry? entry);

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock
            .TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

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
        bool readerHasOwnMvcc = message.TransactionId != HLCTimestamp.Zero
            && entry?.MvccEntries is { } readerMvcc && readerMvcc.ContainsKey(message.TransactionId);

        if (!readerHasOwnMvcc
            && context.PreparedIntentStore?.Get(message.Key) is { } foreignIntent
            && foreignIntent.TransactionId != message.TransactionId)
        {
            HLCTimestamp readTs = message.ReadTimestamp.IsNull() ? HLCTimestamp.Zero : message.ReadTimestamp;
            switch (DurableReadVisibility.Resolve(context, foreignIntent, readTs, message.ForeignDecisionHint))
            {
                case ReadVisibilityAction.Retry:
                    return KeyValueStaticResponses.WaitingForReplicationResponse;

                case ReadVisibilityAction.UseIntentValue:
                    // A committed delete, or a committed value whose TTL has elapsed, is not live — treat it as
                    // does-not-exist, matching the ordinary read's expiry filter rather than serving a dead value.
                    return foreignIntent.State == KeyValueState.Deleted
                           || PreparedIntentVisibility.IsExpired(foreignIntent, currentTime)
                        ? KeyValueStaticResponses.DoesNotExistContextResponse
                        : new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                            foreignIntent.Value, foreignIntent.Revision, foreignIntent.Expires,
                            currentTime, foreignIntent.CommitTimestamp, foreignIntent.State));

                case ReadVisibilityAction.UseExisting:
                default:
                    break; // fall through to the ordinary read paths
            }
        }

        // Validate active replication intent — client must retry until the entry is
        // fully replicated before reading it.
        if (entry?.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.WaitingForReplicationResponse;

            entry.ReplicationIntent = null;
        }

        // A live write intent from another transaction is not a blocking condition for reads
        // UNLESS the read carries a snapshot timestamp and the intent's pending commit ts could
        // land at-or-before that snapshot (safe-time wait).
        if (entry?.WriteIntent != null)
        {
            if (entry.WriteIntent.TransactionId != message.TransactionId)
            {
                if (!KeyValueWriteIntentLease.IsLive(context, message.Key, entry.WriteIntent, currentTime))
                    entry.WriteIntent = null;
                else if (!message.ReadTimestamp.IsNull()
                         && KeyValueWriteIntentSafeTime.MayCommitAtOrBefore(entry.WriteIntent, message.ReadTimestamp))
                    return KeyValueStaticResponses.WaitingForReplicationResponse;
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
                        return new(KeyValueResponseType.MustRetry, 0);

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
                KeyValueEntry? diskEntry = await context.BackendReadScheduler.EnqueueBatchableTask(
                    ResolvePartition(message.Key),
                    message.Key,
                    context.PointReadExecutor);

                // A hydrated row below this node's committed-head memory must not become a
                // transaction's MVCC base: the missing committed writes would be silently
                // discarded by the read-modify-write built on it. Refuse and let the scheduled
                // convergence repair advance the local state before the client's retry.
                if (HydratedRowProvablyStale(message.Key, diskEntry))
                    return KeyValueStaticResponses.MustRetryResponse;

                if (diskEntry is not null)
                {
                    diskEntry.FlushedRevision = diskEntry.Revision;
                    diskEntry.LastUsed = currentTime;
                    context.InsertStoreEntry(message.Key, diskEntry);
                    entry = diskEntry;
                }
            }

            if (entry is null)
            {
                entry = new() { Bucket = GetBucket(message.Key), State = KeyValueState.Undefined, Revision = -1 };
                context.InsertStoreEntry(message.Key, entry);
            }

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
                context.AdjustEstimatedEntryBytes(entry,
                    KeyValueStoreAccounting.MvccEntryAddedBytes(mvccDictJustCreated, mvccEntry.Value));
            }

            if (entry.Revision > mvccEntry.Revision)
                return KeyValueStaticResponses.AbortedResponse;

            if (mvccEntry.State is KeyValueState.Undefined or KeyValueState.Deleted
                || (mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                mvccEntry.Value, mvccEntry.Revision, mvccEntry.Expires,
                mvccEntry.LastUsed, mvccEntry.LastModified, mvccEntry.State));
        }

        // ── Snapshot visibility: serve the revision at-or-before readTimestamp ──────────
        // If the entry is not resident and we have a snapshot timestamp, we need the disk
        // value to apply snapshot logic (PointReadContinuation carries no snapshot state),
        // so load synchronously in this case.
        if (!inCache && message.Durability == KeyValueDurability.Persistent
            && !message.ReadTimestamp.IsNull())
        {
            KeyValueEntry? diskEntry = await context.BackendReadScheduler.EnqueueBatchableTask(
                ResolvePartition(message.Key),
                message.Key,
                context.PointReadExecutor);

            // A snapshot read resolves at-or-before against the hydrated head; a head below the
            // committed-head memory is missing newer revisions and can resolve the wrong as-of
            // version. Refuse and let the convergence repair land before the retry.
            if (HydratedRowProvablyStale(message.Key, diskEntry))
                return KeyValueStaticResponses.MustRetryResponse;

            if (diskEntry is not null)
            {
                diskEntry.FlushedRevision = diskEntry.Revision;
                diskEntry.LastUsed = currentTime;
                context.InsertStoreEntry(message.Key, diskEntry);
                entry = diskEntry;
            }
        }

        if (!message.ReadTimestamp.IsNull() && entry is not null
            && entry.LastModified > message.ReadTimestamp)
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

                // In-memory archive trimmed the as-of revision; fall back to the persisted
                // revision history. This is correct because trimming drops the lowest revision
                // numbers, so an in-memory miss means the true as-of answer (if any) is older
                // and only on disk.
                KeyValueEntry? diskSnapshot = context.PersistenceBackend.GetKeyValueRevisionAtOrBefore(
                    message.Key, entry.Revision - 1, message.ReadTimestamp);

                if (diskSnapshot is null
                    || diskSnapshot.State is KeyValueState.Deleted or KeyValueState.Undefined
                    || (diskSnapshot.Expires != HLCTimestamp.Zero
                        && diskSnapshot.Expires - currentTime < TimeSpan.Zero))
                    return KeyValueStaticResponses.DoesNotExistContextResponse;

                return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                    diskSnapshot.Value, diskSnapshot.Revision, diskSnapshot.Expires,
                    currentTime, diskSnapshot.LastModified, diskSnapshot.State));
            }

            if (snapshot.State is KeyValueState.Deleted or KeyValueState.Undefined
                || (snapshot.Expires != HLCTimestamp.Zero
                    && snapshot.Expires - currentTime < TimeSpan.Zero))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
                snapshot.Value, snapRevision, snapshot.Expires,
                currentTime, snapshot.LastModified, snapshot.State));
        }

        // ── Non-transactional persistent cache miss → detach or coalesce ────────────────────
        // If a backend read for this key is already in flight, attach the caller's Promise to
        // the existing continuation (single-flight coalescing) instead of issuing a second
        // identical read. The first caller's ContinueWith will drive ResumeRead for everyone.
        if (entry is null && message.Durability == KeyValueDurability.Persistent)
        {
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

            Task<KeyValueEntry?> readTask;
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
                    // Mark as faulted so Execute (stage 3) resolves all waiters with MustRetry.
                    // We deliberately still route through ResumeRead rather than calling
                    // Resolve / PendingReads.Remove here: this callback runs on a thread-pool
                    // thread and PendingReads is actor-owned state (no lock).
                    cont.SetFaulted();
                }
                else
                {
                    cont.DiskResult = t.Result;
                }
                actorContext.Self.Send(
                    new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = cont });
            }, TaskScheduler.Default);

            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.WaitingForReplicationResponse;
        }

        // ── In-memory result: state/expiry check then reply ───────────────────────────────
        if (entry is null
            || entry.State is KeyValueState.Undefined or KeyValueState.Deleted
            || (entry.Expires != HLCTimestamp.Zero
                && entry.Expires - currentTime < TimeSpan.Zero))
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        context.TouchEntry(entry, currentTime);

        return new(KeyValueResponseType.Get, new ReadOnlyKeyValueEntry(
            entry.Value, entry.Revision, entry.Expires,
            entry.LastUsed, entry.LastModified, entry.State));
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

        Task<KeyValueEntry?> readTask;
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
            if (!t.IsCompletedSuccessfully) cont.SetFaulted();
            else cont.DiskResult = t.Result;
            actorContext.Self.Send(
                new KeyValueRequest(KeyValueRequestType.ResumeRead) { Continuation = cont });
        }, TaskScheduler.Default);

        actorContext.ByPassReply = true;
        return KeyValueStaticResponses.WaitingForReplicationResponse;
    }
}

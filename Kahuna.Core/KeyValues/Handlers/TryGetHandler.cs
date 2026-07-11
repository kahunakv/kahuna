
using Nixie;
using Kommander.Time;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.KeyValues.Ranges;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Represents a handler for attempting to retrieve key-value pairs.
/// </summary>
internal sealed class TryGetHandler : BaseHandler
{
    public TryGetHandler(KeyValueContext context) : base(context) { }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        // ── By-revision path ─────────────────────────────────────────────────────────────
        // Requires an entry loaded from cache/disk before checking; detached via the async resumable-read path.
        if (message.CompareRevision > -1)
            return ExecuteByRevision(message);

        // ── Stage 1: in-memory pre-checks (actor thread, synchronous) ────────────────────
        bool inCache = context.Store.TryGetValue(message.Key, out KeyValueEntry? entry);

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock
            .TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

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
                if (entry.WriteIntent.Expires - currentTime <= TimeSpan.Zero)
                    entry.WriteIntent = null;
                else if (!message.ReadTimestamp.IsNull())
                {
                    HLCTimestamp commitTs = entry.WriteIntent.CommitTimestamp;
                    if (commitTs.IsNull() || commitTs.CompareTo(message.ReadTimestamp) <= 0)
                        return KeyValueStaticResponses.WaitingForReplicationResponse;
                }
                // live write intent from another tx and outside our snapshot window: fall through
            }
        }

        // Prefix lock check.
        string? bucket = entry is not null ? entry.Bucket : GetBucket(message.Key);

        if (bucket is not null && context.LocksByPrefix.TryGetValue(bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (intent.Expires - currentTime > TimeSpan.Zero)
                    return new(KeyValueResponseType.MustRetry, 0);

                context.LocksByPrefix.Remove(bucket);
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
                KeyValueEntry? diskEntry = await context.Raft.ReadScheduler.EnqueueTask(
                    ResolvePartition(message.Key),
                    () => context.PersistenceBackend.GetKeyValue(message.Key));

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
            KeyValueEntry? diskEntry = await context.Raft.ReadScheduler.EnqueueTask(
                ResolvePartition(message.Key),
                () => context.PersistenceBackend.GetKeyValue(message.Key));

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

            TaskCompletionSource<KeyValueResponse?> promise = actorContext.Reply.Value.Promise;

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
                readTask = context.Raft.ReadScheduler.EnqueueTask(
                    partitionId,
                    () => context.PersistenceBackend.GetKeyValue(message.Key));
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

        TaskCompletionSource<KeyValueResponse?> promise = actorContext.Reply.Value.Promise;

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

        Task<KeyValueEntry?> readTask;
        try
        {
            readTask = context.Raft.ReadScheduler.EnqueueTask(
                partitionId,
                () => context.PersistenceBackend.GetKeyValueRevision(message.Key, message.CompareRevision));
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

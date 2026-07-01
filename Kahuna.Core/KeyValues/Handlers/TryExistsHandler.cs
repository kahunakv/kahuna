
using Nixie;
using Kommander;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles the processing of requests to check the existence of a key-value entry in the store.
/// </summary>
internal sealed class TryExistsHandler : BaseHandler
{
    public TryExistsHandler(KeyValueContext context) : base(context) { }

    public async Task<KeyValueResponse> Execute(KeyValueRequest message)
    {
        // By-revision path dispatches GetKeyValueRevision off the actor; no blocking load.
        if (message.CompareRevision > -1)
            return ExecuteByRevision(message);

        // ── Stage 1: in-memory pre-checks (actor thread, synchronous) ────────────────────────
        bool inCache = context.Store.TryGetValue(message.Key, out KeyValueEntry? entry);

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock
            .TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        if (entry?.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.WaitingForReplicationResponse;

            entry.ReplicationIntent = null;
        }

        // When a snapshot readTimestamp is in play, a live foreign write intent whose pending
        // commit could land at-or-before T must be awaited.
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
            }
        }

        string? bucket = entry is not null ? entry.Bucket : GetBucket(message.Key);

        if (bucket is not null && context.LocksByPrefix.TryGetValue(bucket, out KeyValueWriteIntent? intent))
        {
            if (intent.TransactionId != message.TransactionId)
            {
                if (intent.Expires - currentTime > TimeSpan.Zero)
                    return KeyValueStaticResponses.MustRetryResponse;

                context.LocksByPrefix.Remove(bucket);
            }
        }

        // ── Transactional MVCC path (never detaches per spec) ────────────────────────────────
        if (message.TransactionId != HLCTimestamp.Zero && message.ReadTimestamp.IsNull())
        {
            if (!inCache && message.Durability == KeyValueDurability.Persistent)
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
                context.AdjustEstimatedEntryBytes(entry, KeyValueStoreAccounting.MvccEntryAddedBytes(mvccDictJustCreated, mvccEntry.Value));
            }

            if (mvccEntry.State is KeyValueState.Undefined or KeyValueState.Deleted
                || (mvccEntry.Expires != HLCTimestamp.Zero && mvccEntry.Expires - currentTime < TimeSpan.Zero))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            if (entry.Revision > mvccEntry.Revision)
                return KeyValueStaticResponses.AbortedResponse;

            return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueEntry(
                null, mvccEntry.Revision, mvccEntry.Expires,
                mvccEntry.LastUsed, mvccEntry.LastModified, mvccEntry.State));
        }

        // ── Snapshot visibility: load synchronously if not cached, then serve as-of view ─────
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
                // revision history. Mirrors TryGetHandler's fallback — same reasoning applies.
                KeyValueEntry? diskSnapshot = context.PersistenceBackend.GetKeyValueRevisionAtOrBefore(
                    message.Key, entry.Revision - 1, message.ReadTimestamp);

                if (diskSnapshot is null
                    || diskSnapshot.State is KeyValueState.Deleted or KeyValueState.Undefined
                    || (diskSnapshot.Expires != HLCTimestamp.Zero
                        && diskSnapshot.Expires - currentTime < TimeSpan.Zero))
                    return KeyValueStaticResponses.DoesNotExistContextResponse;

                return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueEntry(
                    null, diskSnapshot.Revision, diskSnapshot.Expires,
                    currentTime, diskSnapshot.LastModified, diskSnapshot.State));
            }

            if (snapshot.State is KeyValueState.Deleted or KeyValueState.Undefined
                || (snapshot.Expires != HLCTimestamp.Zero
                    && snapshot.Expires - currentTime < TimeSpan.Zero))
                return KeyValueStaticResponses.DoesNotExistContextResponse;

            return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueEntry(
                null, snapRevision, snapshot.Expires,
                currentTime, snapshot.LastModified, snapshot.State));
        }

        // ── Non-transactional persistent cache miss → detach or coalesce ─────────────────────
        // (key, -1, true) = latest-point TryExists. Separate from TryGet's (key, -1, false)
        // so the two shapes do not cross-coalesce onto a single PointReadContinuation.
        if (entry is null && message.Durability == KeyValueDurability.Persistent)
        {
            IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext =
                context.ActorContext;

            if (!actorContext.Reply.HasValue)
                return KeyValueStaticResponses.ErroredResponse;

            TaskCompletionSource<KeyValueResponse?> promise = actorContext.Reply.Value.Promise;

            (string, long, bool) pointKey = (message.Key, -1L, true);
            if (context.PendingReads.TryGetValue(pointKey, out ReadContinuation? inflight))
            {
                inflight.AddWaiter(promise);
                actorContext.ByPassReply = true;
                return KeyValueStaticResponses.WaitingForReplicationResponse;
            }

            PointReadContinuation cont = new(message.Key, KeyValueResponseType.Exists, promise);
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
                context.PendingReads.Remove(pointKey);
                context.Logger.LogWarning(
                    "KeyValueActor/TryExists: read scheduler rejected enqueue for key {Key}: {Ex}",
                    message.Key, ex.Message);
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

        // ── In-memory result ─────────────────────────────────────────────────────────────────
        if (entry is null || entry.State is KeyValueState.Undefined or KeyValueState.Deleted
            || (entry.Expires != HLCTimestamp.Zero
                && entry.Expires - currentTime < TimeSpan.Zero))
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        context.TouchEntry(entry, currentTime);

        return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueEntry(
            null, entry.Revision, entry.Expires,
            entry.LastUsed, entry.LastModified, entry.State));
    }

    private KeyValueResponse ExecuteByRevision(KeyValueRequest message)
    {
        // ── Stage 1: in-memory shortcuts (no disk, no detach) ────────────────────────────────
        context.Store.TryGetValue(message.Key, out KeyValueEntry? entry);

        if (entry is not null && entry.Revision == message.CompareRevision)
            return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueEntry(
                null, message.CompareRevision,
                HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set));

        if (entry?.Revisions != null && entry.Revisions.ContainsKey(message.CompareRevision))
            return new(KeyValueResponseType.Exists, new ReadOnlyKeyValueEntry(
                null, message.CompareRevision,
                HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set));

        if (message.Durability != KeyValueDurability.Persistent)
            return KeyValueStaticResponses.DoesNotExistContextResponse;

        // ── Stage 2 dispatch: detach GetKeyValueRevision off the actor mailbox ────────────────
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext =
            context.ActorContext;

        if (!actorContext.Reply.HasValue)
            return KeyValueStaticResponses.ErroredResponse;

        TaskCompletionSource<KeyValueResponse?> promise = actorContext.Reply.Value.Promise;

        // (key, revision, true) = by-revision TryExists; distinct from TryGet's (key, rev, false).
        (string, long, bool) revKey = (message.Key, message.CompareRevision, true);

        if (context.PendingReads.TryGetValue(revKey, out ReadContinuation? inflight))
        {
            inflight.AddWaiter(promise);
            actorContext.ByPassReply = true;
            return KeyValueStaticResponses.WaitingForReplicationResponse;
        }

        ByRevisionReadContinuation cont = new(
            message.Key, message.CompareRevision, KeyValueResponseType.Exists, promise);
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
                "KeyValueActor/TryExistsByRevision: read scheduler rejected enqueue for key {Key} rev {Rev}: {Ex}",
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

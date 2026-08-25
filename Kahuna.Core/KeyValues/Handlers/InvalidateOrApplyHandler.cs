
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Handles an <c>InvalidateOrApply</c> message: when a committed Raft log entry arrives on a
/// follower, the replicator routes this message to the owning persistent actor so the resident
/// cache entry is updated to the newly committed revision. If the entry is not resident, the
/// message is a no-op — the next read will load the correct revision from disk.
///
/// Only the persistent router receives this message. Ephemeral writes are never replicated
/// via Raft, so this handler will never run inside an ephemeral actor.
///
/// Payload is carried in <see cref="InvalidateOrApplyData"/> on the request, not in the
/// general-purpose fields (CompareRevision, TransactionId, etc.).
/// </summary>
internal sealed class InvalidateOrApplyHandler : BaseHandler
{
    public InvalidateOrApplyHandler(KeyValueContext context) : base(context)
    {
    }

    // INVARIANT: every path in this handler is synchronous. It runs inside the KeyValueActor's single-threaded
    // message loop, and an await that queues on the backend read scheduler parks the whole mailbox behind one
    // disk read — under replicated-apply fan-out (seeding, recovery, split settlement) the backed-up mailbox
    // blows past the request batcher's deadline and expires entire batches. Any backend read a path needs must
    // be performed by the SENDER, off the actor, and handed in through InvalidateOrApplyData.BackendHydrated.
    public KeyValueResponse? Execute(KeyValueRequest message)
    {
        InvalidateOrApplyData data = message.InvalidateOrApplyData!;

        // Durable-intent resolution apply on the leader. A commit clears the committing transaction's write intent
        // and MVCC snapshot, applies the committed value, and persists it (inserting the entry when not resident).
        // An abort just clears the transaction's staged write intent and MVCC snapshot so the key is not blocked
        // until the intent expires (the analog of ApplyConfirmedRollback).
        if (data.ForceResident)
            return data.IsRollback ? ApplyDurableRollback(message.Key, data) : ApplyDurableCommit(message.Key, data);

        if (!context.Store.TryGetValue(message.Key, out KeyValueEntry? entry))
            return null;

        // Don't touch an entry whose apply is still owned by an in-flight operation: the owning
        // actor applies the committed value via CompleteProposal (direct write, ReplicationIntent) or
        // the durable-intent resolution (WriteIntent), which archives the correct superseded revision and
        // adjusts accounting exactly once. Advancing the entry here first would corrupt that archive.
        // An unrelated expired intent may be cleared before applying the authoritative committed value.
        if (entry.ReplicationIntent is not null || entry.WriteIntent is not null)
        {
            HLCTimestamp now = context.Raft.HybridLogicalClock
                .TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

            if (entry.ReplicationIntent is not null)
            {
                if (entry.ReplicationIntent.Expires - now > TimeSpan.Zero)
                    return null;
                entry.ReplicationIntent = null;
            }

            if (entry.WriteIntent is not null)
            {
                // A replicated kv record carrying the write intent's own transaction id IS that
                // transaction's committed materialization: persistent commits materialize only through
                // the durable-intent path (the manual persistent commit is rejected at the manager
                // boundary), and only those records carry a transaction id. The routed force-resident
                // apply reaches exactly one node — the partition leader at resolution time — so on any
                // other replica still holding this intent (an old leader that lost leadership
                // mid-transaction), this notification is the only signal that ever clears the intent and
                // advances the entry. Deferring here froze such an entry forever: reads kept serving the
                // superseded revision while the staged-base fence's committed-head memory advanced on the
                // replicated settle, refusing every later read-modify-write of the key. Apply it; when
                // the force-resident apply also runs on this single-threaded actor, whichever side runs
                // second degrades to an idempotent no-op through the head guards.
                if (data.TransactionId != HLCTimestamp.Zero && entry.WriteIntent.TransactionId == data.TransactionId)
                {
                    ApplyOwnCommittedMaterialization(message.Key, entry, data, now);
                    return null;
                }

                if (KeyValueWriteIntentLease.IsLive(entry.WriteIntent, now))
                    return null;
                entry.WriteIntent = null;
            }
        }

        bool exactHead = HeadMatches(entry, data);

        // Already at a strictly newer revision, or an exact replay of this committed head.
        if (IsStrictlyNewer(entry, data) || exactHead)
            return null;

        KeyValueProposal proposal = BuildProposal(message.Key, data);
        ApplyCommittedHead(entry, proposal, data.TransactionId);

        return null;
    }

    /// <summary>
    /// Applies the write-intent-owning transaction's own replicated materialization to a resident entry: clears
    /// the transaction's MVCC snapshot and write intent, archives the superseded revision, and advances the
    /// committed head. Persistence, the unflushed-overlay record, and the completion receipt are NOT enqueued
    /// here — the replicator already performed all three for this same log entry before routing this
    /// notification. Guarded so an entry that already advanced (the force-resident apply ran first) is left
    /// untouched apart from the transaction's now-settled staged state.
    /// </summary>
    private void ApplyOwnCommittedMaterialization(string key, KeyValueEntry entry, InvalidateOrApplyData data, HLCTimestamp now)
    {
        RemoveMvccEntry(entry, data.TransactionId);
        TrimExpiredMvccEntries(entry, now);
        entry.WriteIntent = null;

        if (IsStrictlyNewer(entry, data) || HeadMatches(entry, data))
        {
            context.RecordCommitted(data.TransactionId);
            return;
        }

        ApplyCommittedHead(entry, BuildProposal(key, data), data.TransactionId);
        context.RecordCommitted(data.TransactionId);
    }

    /// <summary>
    /// Applies a durable-intent resolution's committed value on the leader, then runs the shared
    /// confirmed-commit apply (clears the committing transaction's write intent and MVCC snapshot, archives the
    /// superseded revision, applies the value, persists, and records the decision).
    ///
    /// <para>When the key is not resident, the persisted row must be consulted before anything installs —
    /// a commit-apply can arrive late, after a whole-partition snapshot install or an un-host purge evicted the
    /// resident entry, and fabricating an empty base would install this (possibly superseded) mutation as the
    /// visible head, shadowing newer durable rows for every read until the entry heals. That read must NOT run
    /// here (see the no-I/O invariant on <see cref="Execute"/>): an un-hydrated non-resident apply answers
    /// MustRetry, the sender performs the point read off the actor, and re-asks with the result carried in
    /// <see cref="InvalidateOrApplyData.HydratedEntry"/>. The hydrated entry (or the fresh stub when no row is
    /// persisted) then lets the strictly-newer guard turn a late re-apply into the no-op it must be.
    /// Idempotent: a re-apply after the intent is already cleared and the revision is at or ahead is a no-op.</para>
    /// </summary>
    private KeyValueResponse ApplyDurableCommit(string key, InvalidateOrApplyData data)
    {
        if (!context.Store.TryGetValue(key, out KeyValueEntry? entry))
        {
            if (!data.BackendHydrated)
                return KeyValueStaticResponses.MustRetryResponse;

            if (data.HydratedEntry is not null)
            {
                entry = new()
                {
                    Bucket = GetBucket(key),
                    Value = data.HydratedEntry.Value,
                    Revision = data.HydratedEntry.Revision,
                    FlushedRevision = data.HydratedEntry.Revision,
                    Expires = data.HydratedEntry.Expires,
                    LastUsed = data.HydratedEntry.LastUsed,
                    LastModified = data.HydratedEntry.LastModified,
                    State = data.HydratedEntry.State
                };
            }
            else
                entry = new() { Bucket = GetBucket(key), State = KeyValueState.Undefined, Revision = -1 };

            context.InsertStoreEntry(key, entry);
        }

        bool ownsIntent = entry.WriteIntent is not null && entry.WriteIntent.TransactionId == data.TransactionId;

        if (!ownsIntent && IsStrictlyNewer(entry, data))
            return new(KeyValueResponseType.Committed);

        if (!ownsIntent && HeadMatches(entry, data))
        {
            bool appliedByActor = entry.LastAppliedTransactionId == data.TransactionId;
            bool restoredFromDurableApply = entry.FlushedRevision >= data.Revision
                && context.CompletionReceiptStore.Contains(
                    data.TransactionId, key, KeyValueDurability.Persistent);

            // Matching head metadata alone cannot prove that the superseded revision was archived.
            // Only an actor apply marker or a flushed committed record plus its durable receipt can
            // settle a replay. Otherwise recovery must retry while retained intent state is consulted.
            return appliedByActor || restoredFromDurableApply
                ? new(KeyValueResponseType.Committed)
                : KeyValueStaticResponses.MustRetryResponse;
        }

        KeyValueProposal proposal = BuildProposal(key, data);

        HLCTimestamp now = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        ApplyConfirmedCommit(entry, proposal, data.TransactionId, now, data.PartitionId, recordAnchorKey: null);

        return new(KeyValueResponseType.Committed);
    }

    /// <summary>
    /// Builds the proposal consumed by the single archival head-advance routine. Delete and extend
    /// records may reuse a revision number, so callers compare the complete terminal head rather than
    /// revision alone before deciding a notification is an idempotent replay.
    /// </summary>
    private static KeyValueProposal BuildProposal(string key, InvalidateOrApplyData data) => new(
        data.State == KeyValueState.Deleted ? KeyValueRequestType.TryDelete : KeyValueRequestType.TrySet,
        key,
        data.Value,
        data.Revision,
        data.NoRevision,
        data.Expires,
        data.LastUsed,
        data.LastModified,
        data.State,
        KeyValueDurability.Persistent);

    /// <summary>
    /// Compares every mutation field that determines the visible committed head. Value comparison is
    /// required for no-revision writes, while state and expiry distinguish delete and extend records
    /// that legitimately reuse the prior revision number.
    /// </summary>
    private static bool HeadMatches(KeyValueEntry entry, InvalidateOrApplyData data) =>
        entry.Revision == data.Revision
        && entry.State == data.State
        && entry.Expires == data.Expires
        && entry.LastModified == data.LastModified
        && ((entry.Value is null && data.Value is null)
            || (entry.Value is not null && data.Value is not null
                && entry.Value.AsSpan().SequenceEqual(data.Value)));

    /// <summary>
    /// Orders same-revision delete, extend, and no-revision records by their committed HLC. Revision
    /// comparison alone cannot distinguish those mutations, while accepting an older notification
    /// after a newer same-revision head would regress the cache.
    /// </summary>
    private static bool IsStrictlyNewer(KeyValueEntry entry, InvalidateOrApplyData data) =>
        entry.Revision > data.Revision
        || (entry.Revision == data.Revision && entry.LastModified > data.LastModified);

    /// <summary>
    /// Clears an aborted durable transaction's staged write intent and MVCC snapshot on the owning actor so the key
    /// is not blocked until the intent lease expires (the durable analog of ApplyConfirmedRollback). A no-op when
    /// the key is not resident or its live write intent belongs to a different transaction.
    /// </summary>
    private KeyValueResponse ApplyDurableRollback(string key, InvalidateOrApplyData data)
    {
        if (!context.Store.TryGetValue(key, out KeyValueEntry? entry) || entry is null
            || entry.WriteIntent is null || entry.WriteIntent.TransactionId != data.TransactionId)
            return new(KeyValueResponseType.RolledBack);

        HLCTimestamp now = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        ApplyConfirmedRollback(entry, data.TransactionId, now);

        return new(KeyValueResponseType.RolledBack);
    }
}

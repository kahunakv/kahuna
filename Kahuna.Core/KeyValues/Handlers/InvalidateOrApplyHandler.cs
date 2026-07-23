
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
        // A notification for the transaction that owns the write intent always defers, even if a finite
        // lease elapsed: the acknowledged force-resident apply owns intent cleanup and archival. An
        // unrelated expired intent may be cleared before applying the authoritative committed value.
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
                if (entry.WriteIntent.TransactionId == data.TransactionId)
                    return null;

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
    /// Applies a durable-intent resolution's committed value on the leader: inserts the entry when the key is not
    /// resident, then runs the shared confirmed-commit apply (clears the committing transaction's write intent and
    /// MVCC snapshot, archives the superseded revision, applies the value, persists, and records the decision).
    /// Idempotent: a re-apply after the intent is already cleared and the revision is at or ahead is a no-op.
    /// </summary>
    private KeyValueResponse ApplyDurableCommit(string key, InvalidateOrApplyData data)
    {
        if (!context.Store.TryGetValue(key, out KeyValueEntry? entry))
        {
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

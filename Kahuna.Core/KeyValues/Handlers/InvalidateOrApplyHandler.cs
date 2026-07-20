
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
        // and MVCC snapshot, applies the committed value, and persists it (the durable analog of CompletePhaseTwo,
        // inserting the entry when not resident). An abort just clears the transaction's staged write intent and
        // MVCC snapshot so the key is not blocked until the intent expires (the analog of ApplyConfirmedRollback).
        if (data.ForceResident)
            return data.IsRollback ? ApplyDurableRollback(message.Key, data) : ApplyDurableCommit(message.Key, data);

        if (!context.Store.TryGetValue(message.Key, out KeyValueEntry? entry))
            return null;

        // Don't touch an entry whose apply is still owned by a live in-flight operation: the owning
        // actor applies the committed value via CompleteProposal (direct write, ReplicationIntent) or
        // CompletePhaseTwo (2PC, WriteIntent), which archives the correct superseded revision and
        // adjusts accounting exactly once. Advancing the entry here first would corrupt that archive.
        // If an intent has expired (the node was leader, stamped the intent, then lost leadership before
        // the completion ran), clear it and fall through so the committed value is applied — it is
        // authoritative — matching the expiry-aware pattern used by TryGet, TrySet, TryDelete, etc.
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
                if (entry.WriteIntent.Expires - now > TimeSpan.Zero)
                    return null;
                entry.WriteIntent = null;
            }
        }

        // Already at or ahead of this revision — nothing to do.
        if (entry.Revision >= data.Revision)
            return null;

        int previousValueLength = entry.Value?.Length ?? 0;

        entry.Value        = data.Value;
        entry.Revision     = data.Revision;
        entry.Expires      = data.Expires;
        context.TouchEntry(entry, data.LastUsed);
        entry.LastModified = data.LastModified;
        entry.State        = data.State;

        context.AdjustEntryValueBytes(entry, previousValueLength, entry.Value?.Length ?? 0);
        context.EnqueueExpiry(message.Key, entry.Expires);
        if (data.State is KeyValueState.Deleted or KeyValueState.Undefined)
            context.EnqueueTombstone(message.Key);

        return null;
    }

    /// <summary>
    /// Applies a durable-intent resolution's committed value on the leader: inserts the entry when the key is not
    /// resident, then runs the shared confirmed-commit apply (clears the committing transaction's write intent and
    /// MVCC snapshot, archives the superseded revision, applies the value, persists, and records the decision).
    /// Idempotent: a re-apply after the intent is already cleared and the revision is at or ahead is a no-op.
    /// </summary>
    private KeyValueResponse? ApplyDurableCommit(string key, InvalidateOrApplyData data)
    {
        if (!context.Store.TryGetValue(key, out KeyValueEntry? entry))
        {
            entry = new() { Bucket = GetBucket(key), State = KeyValueState.Undefined, Revision = -1 };
            context.InsertStoreEntry(key, entry);
        }

        bool ownsIntent = entry.WriteIntent is not null && entry.WriteIntent.TransactionId == data.TransactionId;

        // Idempotent skip only when the resident entry already reflects this exact commit: a strictly newer
        // revision is resident, or the same revision AND the same terminal state AND the same expiry. Guarding on
        // revision alone drops mutations that reuse the base revision: a delete does not bump the revision (its
        // tombstone carries the base value's revision, changing the state), and an extend changes only the expiry
        // at the same revision and state. Both stage via MVCC with no owned write intent, so without the state and
        // expiry comparison the tombstone or the extended expiry would be skipped as already-applied.
        if (!ownsIntent
            && (entry.Revision > data.Revision
                || (entry.Revision == data.Revision && entry.State == data.State && entry.Expires == data.Expires)))
            return null; // already applied

        KeyValueProposal proposal = new(
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

        HLCTimestamp now = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        ApplyConfirmedCommit(entry, proposal, data.TransactionId, now, data.PartitionId, recordAnchorKey: null, embeddedDecision: null);

        return null;
    }

    /// <summary>
    /// Clears an aborted durable transaction's staged write intent and MVCC snapshot on the owning actor so the key
    /// is not blocked until the intent lease expires (the durable analog of ApplyConfirmedRollback). A no-op when
    /// the key is not resident or its live write intent belongs to a different transaction.
    /// </summary>
    private KeyValueResponse? ApplyDurableRollback(string key, InvalidateOrApplyData data)
    {
        if (!context.Store.TryGetValue(key, out KeyValueEntry? entry) || entry is null
            || entry.WriteIntent is null || entry.WriteIntent.TransactionId != data.TransactionId)
            return null;

        HLCTimestamp now = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        ApplyConfirmedRollback(entry, data.TransactionId, now);

        return null;
    }
}

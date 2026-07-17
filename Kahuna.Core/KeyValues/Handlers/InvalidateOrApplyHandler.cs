
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

        InvalidateOrApplyData data = message.InvalidateOrApplyData!;

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
}


using Kahuna.Server.Configuration;
using Nixie;
using Kommander;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

internal sealed class TryAcquireExclusiveRangeLockHandler : BaseHandler
{
    public TryAcquireExclusiveRangeLockHandler(KeyValueContext context) : base(context)
    {
    }

    public KeyValueResponse Execute(KeyValueRequest message)
    {
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        if (message.TransactionId == HLCTimestamp.Zero || message.ExpiresMs < 0)
            return KeyValueStaticResponses.ErroredResponse;

        // Prune abandoned expired range locks on the way in so they neither block a fresh acquire
        // nor accumulate on a hot key space; drop the bucket entirely once it holds no live locks.
        if (context.LocksByRange.TryGetValue(message.Key, out List<KeyValueRangeLock>? existingLocks)
            && RangeLockChecks.PruneExpired(context, message.Key, existingLocks, currentTime, int.MaxValue))
        {
            context.LocksByRange.Remove(message.Key);
            existingLocks = null;
        }

        if (existingLocks is not null)
        {
            // Idempotency / upgrade: same tx, same range bounds
            foreach (KeyValueRangeLock existing in existingLocks)
            {
                if (existing.TransactionId != message.TransactionId)
                    continue;
                if (existing.StartKey != message.StartKey
                    || existing.EndKey != message.EndKey
                    || existing.StartInclusive != message.StartInclusive
                    || existing.EndInclusive != message.EndInclusive)
                    continue;

                // S → X upgrade: must pass the same conflict gate as a fresh Exclusive acquire.
                // Another tx may hold an overlapping Shared lock (S∩S coexistence made that reachable).
                // Promoting without checking would leave X(tx1) ∩ S(tx2) — a matrix violation.
                if (message.RangeLockMode == RangeLockMode.Exclusive && existing.Mode == RangeLockMode.Shared)
                {
                    foreach (KeyValueRangeLock other in existingLocks)
                    {
                        if (other.TransactionId == message.TransactionId)
                            continue;
                        if (!RangeLockChecks.IsLive(other, currentTime, context.SessionOwnedIntentCeilingMs))
                            continue;
                        if (RangeLockChecks.RangesOverlap(message.StartKey, message.StartInclusive, message.EndKey, message.EndInclusive,
                                other.StartKey, other.StartInclusive, other.EndKey, other.EndInclusive))
                            return KeyValueResponse.Denied(KeyValueResponseType.AlreadyLocked, other.TransactionId);
                    }

                    // PlaceWriteIntents is atomic: a mid-loop replication conflict rolls back every
                    // intent it wrote, so a failed promotion leaves the lock Shared with no stray
                    // exclusive intents. The caller retries the transaction on the non-Locked response.
                    KeyValueResponse intents = PlaceWriteIntents(currentTime, message);
                    if (intents.Type != KeyValueResponseType.Locked)
                        return intents;
                    existing.Mode = RangeLockMode.Exclusive;
                }
                // X → S downgrade or same-mode re-entry: refresh the expiry from *now* so the
                // caller can extend the lock beyond its original TTL (heartbeat / lease-renewal).
                existing.Expires = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs);
                return KeyValueStaticResponses.LockedResponse;
            }

            // Conflict check: S∩S coexist; a WriteFence coexists with S in both directions (the fence
            // blocks writers through the write-path check, not through reader exclusion, and a reader's
            // Shared lock must not starve a split's quiesce); every pairing involving X conflicts, and
            // two WriteFences conflict (two concurrent splits over one range must serialize).
            foreach (KeyValueRangeLock existing in existingLocks)
            {
                if (existing.TransactionId == message.TransactionId)
                    continue;

                if (!RangeLockChecks.IsLive(existing, currentTime, context.SessionOwnedIntentCeilingMs))
                    continue; // expired, or orphaned past the session-owned ceiling

                if (message.RangeLockMode == RangeLockMode.Shared && existing.Mode == RangeLockMode.Shared)
                    continue; // S∩S always compatible

                if (message.RangeLockMode == RangeLockMode.WriteFence && existing.Mode == RangeLockMode.Shared)
                    continue; // a fence tolerates readers

                if (message.RangeLockMode == RangeLockMode.Shared && existing.Mode == RangeLockMode.WriteFence)
                    continue; // readers tolerate a fence

                if (RangeLockChecks.RangesOverlap(message.StartKey, message.StartInclusive, message.EndKey, message.EndInclusive,
                        existing.StartKey, existing.StartInclusive, existing.EndKey, existing.EndInclusive))
                    return KeyValueResponse.Denied(KeyValueResponseType.AlreadyLocked, existing.TransactionId);
            }
        }

        return LockExistingKeysByRange(currentTime, message);
    }

    private KeyValueResponse LockExistingKeysByRange(HLCTimestamp currentTime, KeyValueRequest message)
    {
        // Exclusive acquires place per-key write intents so existing keys are immediately locked.
        // Shared acquires skip intents — write-path conflict is enforced by TrySetHandler.
        // WriteFence acquires also skip intents: the fence must block writers (the write path refuses
        // a mutation under any foreign range lock) without wedging readers, and a per-key write intent
        // makes every snapshot scan of the range wait for the holder.
        if (message.RangeLockMode == RangeLockMode.Exclusive)
        {
            KeyValueResponse intents = PlaceWriteIntents(currentTime, message);
            if (intents.Type != KeyValueResponseType.Locked)
                return intents;
        }

        KeyValueRangeLock rangeLock = new()
        {
            TransactionId  = message.TransactionId,
            Expires        = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs),
            StartKey       = message.StartKey,
            StartInclusive = message.StartInclusive,
            EndKey         = message.EndKey,
            EndInclusive   = message.EndInclusive,
            Mode           = message.RangeLockMode,
        };

        if (!context.LocksByRange.TryGetValue(message.Key, out List<KeyValueRangeLock>? locks))
        {
            locks = [];
            context.LocksByRange[message.Key] = locks;
        }

        locks.Add(rangeLock);

        return KeyValueStaticResponses.LockedResponse;
    }

    private KeyValueResponse PlaceWriteIntents(HLCTimestamp currentTime, KeyValueRequest message)
    {
        string start = message.StartKey ?? message.Key;
        bool startIncl = message.StartKey is null || message.StartInclusive;
        HLCTimestamp requestedExpiry = KeyValueWriteIntentLease.FromRequest(currentTime, message.ExpiresMs);

        // Stamp per-key write intents atomically: a mid-loop replication conflict rolls back every
        // intent written this call, so a failed acquire/promotion never strands intents on the range's
        // keys. The LocksByRange record installed by the caller is what blocks the write path.
        List<(KeyValueEntry Entry, KeyValueWriteIntent? Prior)>? stamped = null;

        foreach ((string key, KeyValueEntry entry) in context.Store.GetByRange(start, startIncl, message.EndKey, message.EndInclusive, int.MaxValue))
        {
            if (entry.ReplicationIntent is not null)
            {
                if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                {
                    if (stamped is not null)
                        foreach ((KeyValueEntry rollback, KeyValueWriteIntent? prior) in stamped)
                            rollback.WriteIntent = prior;

                    return KeyValueStaticResponses.WaitingForReplicationResponse;
                }

                entry.ReplicationIntent = null;
            }

            if (entry.WriteIntent is not null)
            {
                if (entry.WriteIntent.TransactionId == message.TransactionId)
                {
                    entry.WriteIntent.Expires = requestedExpiry;
                    continue;
                }

                // Another tx holds a live write intent — leave it; LocksByRange will block their commit.
                if (KeyValueWriteIntentLease.IsLive(context, key, entry.WriteIntent, currentTime))
                    continue;
            }

            stamped ??= [];
            stamped.Add((entry, entry.WriteIntent));

            entry.WriteIntent = new()
            {
                TransactionId = message.TransactionId,
                Expires       = requestedExpiry,
                AcquiredAt    = currentTime,
            };

            context.Logger.LogAssignedWriteIntentRangeLock(key, message.TransactionId);
        }

        return KeyValueStaticResponses.LockedResponse;
    }

}

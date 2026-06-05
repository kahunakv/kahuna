
using Kahuna.Server.Configuration;
using Nixie;
using Kommander;
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

        if (message.TransactionId == HLCTimestamp.Zero)
            return KeyValueStaticResponses.ErroredResponse;

        // Idempotency check: same tx, same range → already locked
        if (context.LocksByRange.TryGetValue(message.Key, out List<KeyValueRangeLock>? existingLocks))
        {
            foreach (KeyValueRangeLock existing in existingLocks)
            {
                if (existing.TransactionId == message.TransactionId
                    && existing.StartKey == message.StartKey
                    && existing.EndKey == message.EndKey
                    && existing.StartInclusive == message.StartInclusive
                    && existing.EndInclusive == message.EndInclusive)
                    return KeyValueStaticResponses.LockedResponse;
            }

            // Conflict check: another active transaction has an overlapping range lock
            foreach (KeyValueRangeLock existing in existingLocks)
            {
                if (existing.TransactionId == message.TransactionId)
                    continue;

                if (existing.Expires != HLCTimestamp.Zero && existing.Expires - currentTime <= TimeSpan.Zero)
                    continue; // expired

                if (RangesOverlap(message.StartKey, message.StartInclusive, message.EndKey, message.EndInclusive,
                        existing.StartKey, existing.StartInclusive, existing.EndKey, existing.EndInclusive))
                    return KeyValueStaticResponses.AlreadyLockedResponse;
            }
        }

        return LockExistingKeysByRange(currentTime, message);
    }

    private KeyValueResponse LockExistingKeysByRange(HLCTimestamp currentTime, KeyValueRequest message)
    {
        string start = message.StartKey ?? message.Key;
        bool startIncl = message.StartKey is null || message.StartInclusive;

        foreach ((string key, KeyValueEntry entry) in context.Store.GetByRange(start, startIncl, message.EndKey, message.EndInclusive, int.MaxValue))
        {
            KeyValueResponse response = TryLock(currentTime, message.TransactionId, key, message.ExpiresMs, entry);
            if (response.Type != KeyValueResponseType.Locked)
                return response;
        }

        KeyValueRangeLock rangeLock = new()
        {
            TransactionId  = message.TransactionId,
            Expires        = message.TransactionId + message.ExpiresMs,
            StartKey       = message.StartKey,
            StartInclusive = message.StartInclusive,
            EndKey         = message.EndKey,
            EndInclusive   = message.EndInclusive,
        };

        if (!context.LocksByRange.TryGetValue(message.Key, out List<KeyValueRangeLock>? locks))
        {
            locks = [];
            context.LocksByRange[message.Key] = locks;
        }

        locks.Add(rangeLock);

        return KeyValueStaticResponses.LockedResponse;
    }

    private KeyValueResponse TryLock(HLCTimestamp currentTime, HLCTimestamp transactionId, string key, int expiresMs, KeyValueEntry entry)
    {
        if (entry.ReplicationIntent is not null)
        {
            if (entry.ReplicationIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.WaitingForReplicationResponse;

            entry.ReplicationIntent = null;
        }

        if (entry.WriteIntent is not null)
        {
            if (entry.WriteIntent.TransactionId == transactionId)
                return KeyValueStaticResponses.LockedResponse;

            // Another tx holds a live write intent — skip it; LocksByRange will block their commit.
            if (entry.WriteIntent.Expires != HLCTimestamp.Zero && entry.WriteIntent.Expires - currentTime > TimeSpan.Zero)
                return KeyValueStaticResponses.LockedResponse;
        }

        entry.WriteIntent = new()
        {
            TransactionId = transactionId,
            Expires       = transactionId + expiresMs,
        };

        context.Logger.LogDebug("Assigned {Key} write intent to TxId={TransactionId} (range lock)", key, transactionId);

        return KeyValueStaticResponses.LockedResponse;
    }

    private static bool RangesOverlap(
        string? aStart, bool aStartInclusive, string? aEnd, bool aEndInclusive,
        string? bStart, bool bStartInclusive, string? bEnd, bool bEndInclusive)
    {
        // A.start < B.end  AND  B.start < A.end
        if (!StartBeforeEnd(aStart, aStartInclusive, bEnd, bEndInclusive))
            return false;
        if (!StartBeforeEnd(bStart, bStartInclusive, aEnd, aEndInclusive))
            return false;
        return true;
    }

    private static bool StartBeforeEnd(string? start, bool startInclusive, string? end, bool endInclusive)
    {
        if (start is null || end is null)
            return true; // unbounded → always overlaps

        int cmp = string.Compare(start, end, StringComparison.Ordinal);
        if (cmp < 0) return true;
        if (cmp > 0) return false;
        return startInclusive && endInclusive;
    }
}

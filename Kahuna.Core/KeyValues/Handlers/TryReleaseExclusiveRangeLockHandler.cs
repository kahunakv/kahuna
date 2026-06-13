
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

// Release prefers an exact-bounds match (txId + StartKey + EndKey + inclusivity) and falls back
// to overlap-only if no exact match exists. The fallback lets original bounds match a clamped
// entry inserted by a split/merge transfer. Exact-first preserves precision for a tx that
// holds multiple non-overlapping locks on one partition (the common case). A residual ambiguity
// remains for the exotic "same tx + multiple clamped overlapping locks on one partition" scenario,
// which requires a split of an already-split partition while the lock is still live; document-only.

internal sealed class TryReleaseExclusiveRangeLockHandler : BaseHandler
{
    public TryReleaseExclusiveRangeLockHandler(KeyValueContext context) : base(context)
    {
    }

    public KeyValueResponse Execute(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
            return KeyValueStaticResponses.ErroredResponse;

        if (!context.LocksByRange.TryGetValue(message.Key, out List<KeyValueRangeLock>? locks))
            return KeyValueStaticResponses.UnlockedResponse;

        // Pass 1: exact-bounds match — preserves precision for same-tx multi-lock.
        KeyValueRangeLock? found = null;
        foreach (KeyValueRangeLock rl in locks)
        {
            if (rl.TransactionId == message.TransactionId
                && rl.StartKey       == message.StartKey
                && rl.EndKey         == message.EndKey
                && rl.StartInclusive == message.StartInclusive
                && rl.EndInclusive   == message.EndInclusive)
            {
                found = rl;
                break;
            }
        }

        // Pass 2: overlap fallback — matches a clamped entry from a split/merge transfer.
        if (found is null)
        {
            foreach (KeyValueRangeLock rl in locks)
            {
                if (rl.TransactionId != message.TransactionId)
                    continue;

                if (RangeLockChecks.RangesOverlap(
                        rl.StartKey, rl.StartInclusive, rl.EndKey, rl.EndInclusive,
                        message.StartKey, message.StartInclusive, message.EndKey, message.EndInclusive))
                {
                    found = rl;
                    break;
                }
            }
        }

        if (found is null)
            return KeyValueStaticResponses.UnlockedResponse;

        locks.Remove(found);

        if (locks.Count == 0)
            context.LocksByRange.Remove(message.Key);

        ReleaseExistingLocksInRange(message);

        return KeyValueStaticResponses.UnlockedResponse;
    }

    private void ReleaseExistingLocksInRange(KeyValueRequest message)
    {
        string start = message.StartKey ?? message.Key;
        bool startIncl = message.StartKey is null || message.StartInclusive;

        foreach ((string _, KeyValueEntry entry) in context.Store.GetByRange(
            start, startIncl, message.EndKey, message.EndInclusive, int.MaxValue))
        {
            if (entry.WriteIntent is not null && entry.WriteIntent.TransactionId == message.TransactionId)
                entry.WriteIntent = null;
        }
    }
}

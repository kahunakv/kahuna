
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

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

        KeyValueRangeLock? found = null;
        foreach (KeyValueRangeLock rl in locks)
        {
            if (rl.TransactionId == message.TransactionId
                && rl.StartKey == message.StartKey
                && rl.EndKey == message.EndKey
                && rl.StartInclusive == message.StartInclusive
                && rl.EndInclusive == message.EndInclusive)
            {
                found = rl;
                break;
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

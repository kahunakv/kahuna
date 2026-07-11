
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Returns a snapshot of all live range-lock entries for the key space named by
/// <see cref="KeyValueRequest.Key"/>. Used by <c>KvStateMachineTransfer</c> to read lock
/// state before serializing it into a range-snapshot stream.
/// </summary>
internal sealed class GetRangeLocksHandler : BaseHandler
{
    public GetRangeLocksHandler(KeyValueContext context) : base(context)
    {
    }

    public KeyValueResponse Execute(KeyValueRequest message)
    {
        if (!context.LocksByRange.TryGetValue(message.Key, out List<KeyValueRangeLock>? locks) || locks.Count == 0)
            return KeyValueResponse.ForRangeLocks([]);

        // Prune expired locks before the snapshot so an abandoned lock is never carried into the
        // split/merge transfer stream as if it were live; drop the bucket if nothing survives.
        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());
        if (RangeLockChecks.PruneExpired(locks, currentTime, int.MaxValue))
        {
            context.LocksByRange.Remove(message.Key);
            return KeyValueResponse.ForRangeLocks([]);
        }

        // Return a shallow copy — the caller must not mutate the live list.
        return KeyValueResponse.ForRangeLocks(new List<KeyValueRangeLock>(locks));
    }
}

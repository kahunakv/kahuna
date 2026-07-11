
using Kommander.Time;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Injects a list of <see cref="KeyValueRangeLock"/> entries directly into
/// <c>LocksByRange[keySpace]</c> without running conflict checks or acquire logic.
/// Used by <c>KvStateMachineTransfer</c> to restore clamped locks into a destination
/// partition after a split or merge.
///
/// <para>
/// Deduplication: an entry is skipped when an existing entry for the same transaction
/// and overlapping bounds already exists, preventing double-insertion on re-import.
/// </para>
/// </summary>
internal sealed class ImportRangeLocksHandler : BaseHandler
{
    public ImportRangeLocksHandler(KeyValueContext context) : base(context)
    {
    }

    public KeyValueResponse Execute(KeyValueRequest message)
    {
        if (message.RangeLockImportList is null || message.RangeLockImportList.Count == 0)
            return KeyValueStaticResponses.LockedResponse;

        HLCTimestamp currentTime = context.Raft.HybridLogicalClock.TrySendOrLocalEvent(context.Raft.GetLocalNodeId());

        if (!context.LocksByRange.TryGetValue(message.Key, out List<KeyValueRangeLock>? locks))
        {
            locks = [];
            context.LocksByRange[message.Key] = locks;
        }
        else
        {
            // Prune expired destination locks first so re-import dedups against live locks only.
            RangeLockChecks.PruneExpired(locks, currentTime, int.MaxValue);
        }

        foreach (KeyValueRangeLock entry in message.RangeLockImportList)
        {
            // Never import an already-expired lock as if it were live.
            if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime <= TimeSpan.Zero)
                continue;

            // Deduplicate: skip if this tx already has an overlapping entry (handles re-import).
            bool duplicate = false;
            foreach (KeyValueRangeLock existing in locks)
            {
                if (existing.TransactionId != entry.TransactionId)
                    continue;
                if (RangeLockChecks.RangesOverlap(
                        existing.StartKey, existing.StartInclusive, existing.EndKey, existing.EndInclusive,
                        entry.StartKey, entry.StartInclusive, entry.EndKey, entry.EndInclusive))
                {
                    duplicate = true;
                    break;
                }
            }

            if (!duplicate)
                locks.Add(entry);
        }

        return KeyValueStaticResponses.LockedResponse;
    }
}


using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Drops the belief-only state this shard holds for a partition the node stopped leading: the staged
/// transactional entries (MVCC staging plus the write intent that guards them) and the exclusive prefix
/// and range locks. None of that state ever reached Raft — it was admitted on the strength of a
/// leadership that has ended — so a new leader cannot see it, and any proposal derived from it under
/// the lost term is refused by the term fence. Dropping it here makes the transactions that staged it
/// fail deterministically at their next step instead of lingering until a lease expires, and keeps a
/// re-elected node from carrying stale staging into its new term. Committed entries stay resident: they
/// are the applied log, which the node keeps following.
/// </summary>
internal sealed class DropLeaderStateHandler : BaseHandler
{
    public DropLeaderStateHandler(KeyValueContext context) : base(context) { }

    public KeyValueResponse Execute(KeyValueRequest message)
    {
        RangeMap map = context.RangeMapStore.Current;
        int hashPoolSize = context.Raft.Configuration.InitialPartitions;
        int partitionId = message.PartitionId;

        int dropped = 0;

        foreach (KeyValuePair<string, KeyValueEntry> kv in context.Store.GetItems())
        {
            KeyValueEntry entry = kv.Value;
            if (entry.WriteIntent is null && entry.MvccEntries is null)
                continue;

            if (PartitionDataEnumerator.OwnerOfKey(map, kv.Key, hashPoolSize) != partitionId)
                continue;

            entry.WriteIntent = null;
            entry.MvccEntries = null;
            dropped++;
        }

        List<string>? buckets = null;

        foreach (KeyValuePair<string, KeyValueWriteIntent> lockByPrefix in context.LocksByPrefix)
            if (OwnerOfBucket(map, lockByPrefix.Key, hashPoolSize) == partitionId)
                (buckets ??= []).Add(lockByPrefix.Key);

        if (buckets is not null)
        {
            foreach (string bucket in buckets)
                context.LocksByPrefix.Remove(bucket);

            dropped += buckets.Count;
            buckets = null;
        }

        foreach (KeyValuePair<string, List<KeyValueRangeLock>> locksByRange in context.LocksByRange)
            if (OwnerOfBucket(map, locksByRange.Key, hashPoolSize) == partitionId)
                (buckets ??= []).Add(locksByRange.Key);

        if (buckets is not null)
        {
            foreach (string bucket in buckets)
                context.LocksByRange.Remove(bucket);

            dropped += buckets.Count;
        }

        return new(KeyValueResponseType.Set, dropped);
    }

    /// <summary>A prefix or range lock is keyed by its bucket; route it the way the locator routes a prefix operation.</summary>
    private static int OwnerOfBucket(RangeMap map, string bucket, int hashPoolSize) =>
        PartitionDataEnumerator.OwnerOfKey(map, KeyValueKeySpace.OfPrefix(bucket) + "/", hashPoolSize);
}

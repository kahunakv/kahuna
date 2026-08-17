
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Handlers;

/// <summary>
/// Removes every resident entry owned by one Raft partition from this shard's in-memory store:
/// after the committed placement map stopped listing this node as one of the partition's replicas,
/// or after a whole-partition snapshot install replaced the partition's backend rows.
/// The backend rows are purged/installed separately; this drops the actor-resident copies —
/// including ephemeral entries and leader-tenure leftovers that never reach the backend — so a
/// locally-initiated scan cannot surface them, and the node can never serve a stale resident
/// entry (with, e.g., a lower revision or fencing state) over the freshly seeded backend row.
///
/// <para>
/// Classification uses the same routing data as the request path, against one range-map snapshot
/// captured at entry. Eviction is unconditional — dirty flags, write intents and replication
/// intents are not consulted — because delivery for the partition is quiescent at both call sites
/// (stopped by the committed map, or held by the install occupying the partition's single-writer
/// executor); a late proposal completion for an evicted key is a harmless no-op (the completion
/// handlers tolerate a missing entry). The walk is O(resident entries) in one mailbox turn,
/// acceptable for a once-per-replica-move maintenance message.
/// </para>
/// </summary>
internal sealed class EvictPartitionHandler : BaseHandler
{
    public EvictPartitionHandler(KeyValueContext context) : base(context) { }

    public KeyValueResponse Execute(KeyValueRequest message)
    {
        RangeMap map = context.RangeMapStore.Current;
        int hashPoolSize = context.Raft.Configuration.InitialPartitions;
        int partitionId = message.PartitionId;

        List<string>? toEvict = null;

        foreach (KeyValuePair<string, KeyValueEntry> kv in context.Store.GetItems())
        {
            if (PartitionDataEnumerator.OwnerOfKey(map, kv.Key, hashPoolSize) == partitionId)
                (toEvict ??= []).Add(kv.Key);
        }

        if (toEvict is null)
            return new(KeyValueResponseType.Set, 0);

        foreach (string key in toEvict)
            context.RemoveStoreEntry(key);

        return new(KeyValueResponseType.Set, toEvict.Count);
    }
}

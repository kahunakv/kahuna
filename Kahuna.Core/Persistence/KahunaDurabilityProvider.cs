
using System.Collections.Concurrent;
using Kahuna.Server.Persistence.Backend;
using Kommander;
using Kommander.System;

namespace Kahuna.Server.Persistence;

/// <summary>
/// Kahuna's <see cref="IApplicationDurabilityProvider"/>: answers Kommander's per-partition
/// application-durability query from the live <see cref="PartitionDurabilityTracker"/> watermark,
/// falling back to the floor persisted in the backend — the value that matters at restart, when
/// Kommander decides the replay window before any entry has been delivered.
///
/// <para>
/// The result may be stale-low (redelivery on restore; the appliers are idempotent) but is never
/// high: the persisted floor is written only after the flush batches it certifies landed, and the
/// live watermark only advances over resolved entries. -1 means no opinion, which keeps
/// Kommander's checkpoint-anchored behavior for that partition.
/// </para>
/// </summary>
internal sealed class KahunaDurabilityProvider : IApplicationDurabilityProvider
{
    private readonly PartitionDurabilityTracker tracker;

    private readonly IPersistenceBackend persistenceBackend;

    /// <summary>
    /// Persisted floors read once per partition: the on-disk value only advances through this
    /// node's own flush cycle, which also advances the in-memory watermark past it, so a single
    /// startup read stays current for the process lifetime.
    /// </summary>
    private readonly ConcurrentDictionary<int, long> persistedFloors = new();

    public KahunaDurabilityProvider(PartitionDurabilityTracker tracker, IPersistenceBackend persistenceBackend)
    {
        this.tracker = tracker;
        this.persistenceBackend = persistenceBackend;
    }

    public long GetDurablyAppliedIndex(int partitionId)
    {
        long persisted = persistedFloors.GetOrAdd(partitionId, ReadPersistedFloor);

        long live = tracker.GetWatermark(partitionId);

        return Math.Max(live, persisted);
    }

    /// <summary>
    /// The backend's floor for the partition, with an absent floor resolved by what absence means for the
    /// backend. A store that persists floors (<see cref="IPersistenceBackend.KeepsDurabilityFloors"/>) and
    /// has none for a data partition has flushed nothing for it: the durable state is empty, and the
    /// provider contract asks for 0 there, so Kommander treats the store as empty rather than as "no
    /// opinion" — an empty store under a compacted log is then refused replay and rebuilt by snapshot
    /// instead of silently serving a suffix without its prefix (a store that was wiped or replaced). The
    /// system partition keeps "no opinion": its consumer entries (the range map, the hold registry) are
    /// not floor-tracked, so their absence says nothing about the store. A backend that keeps no floors
    /// never has an opinion.
    /// </summary>
    private long ReadPersistedFloor(int partitionId)
    {
        long floor = persistenceBackend.GetDurabilityFloor(partitionId);

        if (floor >= 0)
            return floor;

        return persistenceBackend.KeepsDurabilityFloors && partitionId != RaftSystemConfig.SystemPartition ? 0 : -1;
    }

    /// <summary>
    /// Evicts the cached persisted floor for a partition this node stopped hosting. The "read once
    /// per partition" contract above assumes the floor only advances through this node's own flush
    /// cycle — but a partition that leaves and later returns re-seeds from a snapshot, and the old
    /// cached floor would then vouch for applies the purged local copy no longer has. Evicting here
    /// makes a re-hosted partition re-read the backend, which reflects whatever the replica-loss
    /// purge left there.
    /// </summary>
    internal void Forget(int partitionId) => persistedFloors.TryRemove(partitionId, out _);
}

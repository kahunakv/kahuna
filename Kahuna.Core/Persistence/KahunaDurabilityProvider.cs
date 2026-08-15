
using System.Collections.Concurrent;
using Kahuna.Server.Persistence.Backend;
using Kommander;

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
        long persisted = persistedFloors.GetOrAdd(partitionId, persistenceBackend.GetDurabilityFloor);

        long live = tracker.GetWatermark(partitionId);

        return Math.Max(live, persisted);
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

using Nixie;

using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Periodic and operator-driven node upkeep for the key-value subsystem: running a collection pass across
/// every actor instance, computing the node's safe MVCC timestamp, purging the data of partitions this node
/// no longer hosts, evicting a partition's resident entries, and draining the write aggregator.
///
/// The safe timestamp is a <b>minimum</b> across instances — it feeds MVCC pruning, so taking a maximum would
/// reclaim revisions a reader still needs. The un-host purge re-checks its <c>stillUnhosted</c> predicate
/// while it runs, because placement can change mid-sweep.
/// </summary>
internal sealed class NodeMaintenanceService
{
    private readonly KeyValuesRuntime runtime;

    internal NodeMaintenanceService(KeyValuesRuntime runtime) => this.runtime = runtime;

    // Aliases matching the field names the moved bodies use, so those bodies stay byte-for-byte as they were.
    private IRaft raft => runtime.Raft;

    private ILogger<IKahuna> logger => runtime.Logger;

    private IPersistenceBackend persistenceBackend => runtime.PersistenceBackend;

    private RangeMapStore rangeMapStore => runtime.RangeMapStore;

    private PartitionDataEnumerator partitionDataEnumerator => runtime.PartitionDataEnumerator;

    private Writes.PartitionWriteAggregator writeAggregator => runtime.WriteAggregator;

    private PartitionStateTransfer partitionStateTransfer => runtime.PartitionStateTransfer;

    private IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> ephemeralInstances => runtime.Routers.EphemeralInstances;

    private IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> persistentInstances => runtime.Routers.PersistentInstances;

    internal async Task RunCollectOnAllInstancesAsync()
    {
        KeyValueRequest collect = new(KeyValueRequestType.Collect);
        List<Task<KeyValueResponse?>> tasks = new(ephemeralInstances.Count + persistentInstances.Count);

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in ephemeralInstances)
            tasks.Add(actor.Ask(collect)!);

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in persistentInstances)
            tasks.Add(actor.Ask(collect)!);

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Fans out a <c>GetSafeTimestamp</c> query to every key-value actor shard and returns
    /// the minimum prepared <c>CommitTimestamp</c> across all live write intents in the cluster.
    /// Returns <see cref="HLCTimestamp.Zero"/> when no shard has an in-flight prepared transaction.
    /// </summary>
    internal async Task<HLCTimestamp> GetSafeTimestampAsync()
    {
        KeyValueRequest request = new(KeyValueRequestType.GetSafeTimestamp);
        List<Task<KeyValueResponse?>> tasks = new(ephemeralInstances.Count + persistentInstances.Count);

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in ephemeralInstances)
            tasks.Add(actor.Ask(request)!);

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in persistentInstances)
            tasks.Add(actor.Ask(request)!);

        KeyValueResponse?[] results = await Task.WhenAll(tasks);

        HLCTimestamp min = HLCTimestamp.Zero;
        foreach (KeyValueResponse? r in results)
        {
            if (r is null || r.Ticket == HLCTimestamp.Zero)
                continue;
            if (min == HLCTimestamp.Zero || r.Ticket.CompareTo(min) < 0)
                min = r.Ticket;
        }

        return min;
    }

    /// <summary>
    /// Removes everything this node retains for a partition the committed map no longer hosts
    /// here: the durable state (backend rows, store slices, floor, half-install marker — via the
    /// state transfer, serialized against a concurrent seeding install) and then the actor-resident
    /// entries. Memory eviction runs strictly after the durable purge: the purge drains the
    /// background writer first, so by eviction time no evicted entry has an unflushed write that
    /// could land afterwards and resurrect rows. Returns false when aborted (the partition was
    /// re-gained) or a durable step could not complete; the startup re-derivation converges it.
    /// </summary>
    internal async Task<bool> PurgeUnhostedPartitionDataAsync(int partitionId, Func<bool> stillUnhosted, CancellationToken cancellationToken)
    {
        if (!await partitionStateTransfer.PurgeUnhostedPartitionAsync(partitionId, stillUnhosted, cancellationToken).ConfigureAwait(false))
            return false;

        await EvictPartitionEntriesAsync(partitionId).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Broadcasts a partition eviction to every key-value actor shard (ephemeral and persistent)
    /// so no shard retains a resident entry — ephemeral or leader-tenure leftovers included — for
    /// a partition this node stopped hosting. Completes when every shard has processed it.
    /// </summary>
    internal async Task EvictPartitionEntriesAsync(int partitionId)
    {
        KeyValueRequest request = new(
            KeyValueRequestType.EvictPartition,
            HLCTimestamp.Zero,
            HLCTimestamp.Zero,
            string.Empty,
            null,
            null,
            -1,
            KeyValueFlags.None,
            0,
            HLCTimestamp.Zero,
            KeyValueDurability.Persistent,
            0,
            partitionId,
            null);

        List<Task<KeyValueResponse?>> tasks = new(ephemeralInstances.Count + persistentInstances.Count);

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in ephemeralInstances)
            tasks.Add(actor.Ask(request)!);

        foreach (IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse> actor in persistentInstances)
            tasks.Add(actor.Ask(request)!);

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    /// <summary>Observable async drain of the direct-write aggregator: rejects new writes, releases queued
    /// ones retryably, and awaits in-flight batch settlement. Must run while the actor system and Raft are
    /// still alive (before their disposal), so in-flight batches can report their outcome.</summary>
    public Task DrainWritesAsync(TimeSpan timeout) => writeAggregator.StopAsync(timeout);
}

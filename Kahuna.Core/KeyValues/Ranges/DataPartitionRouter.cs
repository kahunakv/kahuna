using Kahuna.Shared.Routing;
using Kommander;
using Kommander.System;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Kahuna-owned hash assignment for hash-routed key spaces — the replacement for
/// <c>IRaft.GetPartitionKey</c> under the decision that <b>Kahuna owns key→partition assignment</b>
/// (Kommander only hosts groups; the partitions Kahuna creates for splits are <c>Unrouted</c>).
///
/// <para>
/// The hash pool is <b>the user partitions Kommander created at cluster bootstrap</b>, <c>[1, N]</c>.
/// Partition 0 is the Kommander system partition; it hosts the replicated meta map
/// (<see cref="RangeMapStore.MetaPartitionId"/>) alongside the partition-map coordinator (by log
/// type, Kommander 0.11.0+) and is <b>not</b> in the data pool. Every user partition from 1 upward
/// carries hash data; key-range data also starts at partition <c>≥
/// <see cref="RangeMapStore.FirstDataPartitionId"/></c> (1), with splits allocating fresh higher ids,
/// enforced by <see cref="RangeMapStore.MutateAsync"/>.
/// </para>
///
/// <para>
/// <b>Where the pool size comes from.</b> Kommander writes <c>RaftConfiguration.InitialPartitions</c>
/// into the committed partition map at the first boot and ignores the configured value at every
/// later boot, so the configured value describes the cluster only until the map exists. The pool
/// is therefore read from the committed map once the node applied one (<see cref="IRaft.IsInitialized"/>):
/// the bootstrap partitions are the map's <see cref="RaftRoutingMode.HashRange"/> entries, and the
/// partitions a key-range split creates later are <see cref="RaftRoutingMode.Unrouted"/> and belong to
/// ranged spaces, not the pool — hash spaces don't split — so the count stays stable and is cached.
/// Before a committed map is applied, the configured value is used without caching: at a first
/// boot it is exactly what Kommander is about to commit, and no partition leads yet on a restart.
/// Reading the configuration after bootstrap routed hash keys to partitions that do not exist
/// whenever an operator raised <c>InitialPartitions</c> on an existing deployment, which made the
/// rows of most placement groups unreadable and their writes fail with retryable errors forever.
/// </para>
/// </summary>
internal sealed class DataPartitionRouter
{
    /// <summary>The lowest partition in the hash pool. Partition 0 is the system/meta partition.</summary>
    internal const int FirstUserPartitionId = 1;

    /// <summary>Null only for a router over a fixed pool (<see cref="DataPartitionRouter(int)"/>).</summary>
    private readonly IRaft? raft;

    /// <summary>The pool size read from the committed map, or 0 until a committed map was applied.</summary>
    private int poolSize;

    public DataPartitionRouter(IRaft raft) => this.raft = raft;

    /// <summary>
    /// A router over a fixed pool of <paramref name="poolSize"/> partitions, for callers that classify
    /// data without a Raft node (offline snapshot builders and tests).
    /// </summary>
    internal DataPartitionRouter(int poolSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(poolSize, 1);

        this.poolSize = poolSize;
    }

    /// <summary>
    /// Size of the hash pool: every user partition <c>[1, PoolSize]</c>. The committed map's bootstrap
    /// partition count once the node applied a committed map; the configured
    /// <c>InitialPartitions</c> before that.
    /// </summary>
    public int PoolSize
    {
        get
        {
            int size = Volatile.Read(ref poolSize);
            return size > 0 ? size : ResolvePoolSize();
        }
    }

    private int ResolvePoolSize()
    {
        // The fixed-pool constructor always sets the size, so only a Raft-backed router gets here.
        IRaft raft = this.raft!;

        // No committed map applied yet: the configured value is the only description of the pool. It is
        // not cached, because the committed map replaces it as soon as the node applies one.
        if (!raft.IsInitialized)
            return raft.Configuration.InitialPartitions;

        int size = HashPoolSizeOf(raft.GetPartitionMap());

        // A committed map without hash-range partitions is a harness that registered partitions directly;
        // keep the configured value for it, uncached.
        if (size < 1)
            return raft.Configuration.InitialPartitions;

        // Every thread derives the same value from the same committed set, so a racing write is benign.
        Volatile.Write(ref poolSize, size);
        return size;
    }

    /// <summary>
    /// The hash pool a committed partition map describes: the number of its live
    /// <see cref="RaftRoutingMode.HashRange"/> partitions, which are the user partitions created at
    /// bootstrap. Partitions a split created are <see cref="RaftRoutingMode.Unrouted"/> and not counted.
    /// </summary>
    internal static int HashPoolSizeOf(IReadOnlyList<RaftPartitionRange> map)
    {
        int size = 0;

        for (int i = 0; i < map.Count; i++)
        {
            RaftPartitionRange range = map[i];

            if (range.RoutingMode == RaftRoutingMode.HashRange && range.State != RaftPartitionState.Removed)
                size++;
        }

        return size;
    }

    /// <summary>
    /// Resolves <paramref name="key"/> to a user partition in <c>[1, PoolSize]</c> by
    /// hashing the placement group of its key space (<see cref="HashPlacement"/>: the key space is
    /// the prefix before the last <c>'/'</c>, matching <see cref="KeySpaceRegistry.ExtractKeySpace"/>;
    /// the group is the key space's prefix before its first <c>'|'</c>, or the whole key space). Key
    /// spaces that name the same group land on the same partition, which is how a consumer keeps a
    /// table's rows and index entries together. Deterministic and stable for a given pool size.
    /// </summary>
    public int Locate(string key) => FirstUserPartitionId + HashPlacement.BucketOfKey(key, PoolSize);

    /// <summary>
    /// The user partition in <c>[1, PoolSize]</c> that hash routing gives every key of
    /// <paramref name="keySpace"/>: the same rule as <see cref="Locate"/>, for callers that hold the key
    /// space rather than a key.
    /// </summary>
    public int LocateKeySpace(string keySpace) => HashPartitionOfKeySpace(keySpace, PoolSize);

    /// <summary>
    /// The user partition in <c>[1, poolSize]</c> that hash routing gives every key of
    /// <paramref name="keySpace"/> over a pool of <paramref name="poolSize"/> partitions. This is the
    /// one definition of "the hash partition of a key space": <see cref="Locate"/>, the partition data
    /// enumerator, the watch service and the key-range seed all answer through it, so a key space
    /// starts on the same partition whichever routing mode it is in. Pure, so it is answerable without
    /// a live node.
    /// </summary>
    internal static int HashPartitionOfKeySpace(string keySpace, int poolSize) =>
        FirstUserPartitionId + HashPlacement.BucketOfKeySpace(keySpace, poolSize);
}

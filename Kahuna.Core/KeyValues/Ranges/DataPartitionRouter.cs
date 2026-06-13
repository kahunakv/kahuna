using Kommander;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Kahuna-owned hash assignment for hash-routed key spaces — the replacement for
/// <c>IRaft.GetPartitionKey</c> under the decision that <b>Kahuna owns key→partition assignment</b>
/// (Kommander only hosts groups; its partitions are <c>Unrouted</c>).
///
/// <para>
/// The hash pool is <b>all user partitions <c>[1, InitialPartitions]</c></b>. Partition 0 is the
/// Kommander system partition; it hosts the replicated meta map
/// (<see cref="RangeMapStore.MetaPartitionId"/>) alongside the partition-map coordinator (by log
/// type, Kommander 0.11.0+) and is <b>not</b> in the data pool. Every user partition from 1 upward
/// carries hash data; key-range data also starts at partition <c>≥
/// <see cref="RangeMapStore.FirstDataPartitionId"/></c> (1), with splits allocating fresh higher ids,
/// enforced by <see cref="RangeMapStore.MutateAsync"/>.
/// </para>
///
/// <para>
/// The pool is the fixed set of <i>initial</i> partitions. Partitions created later by key-range
/// splits get higher ids and belong to ranged spaces, not the hash pool — hash spaces don't split —
/// so the pool stays stable.
/// </para>
/// </summary>
internal sealed class DataPartitionRouter
{
    /// <summary>The lowest partition in the hash pool. Partition 0 is the system/meta partition.</summary>
    private const int FirstUserPartitionId = 1;

    private readonly IRaft raft;

    public DataPartitionRouter(IRaft raft) => this.raft = raft;

    /// <summary>Size of the hash pool: every user partition <c>[1, InitialPartitions]</c>.</summary>
    public int PoolSize => raft.Configuration.InitialPartitions;

    /// <summary>
    /// Resolves <paramref name="key"/> to a user partition in <c>[1, InitialPartitions]</c> by
    /// ordinally hashing its key-space prefix (the part before the last <c>'/'</c>, matching
    /// <see cref="KeySpaceRegistry.ExtractKeySpace"/>). Deterministic and stable for a given pool size.
    /// </summary>
    public int Locate(string key)
    {
        long bucket = HashUtils.InversePrefixedHash(key, '/', PoolSize);
        return FirstUserPartitionId + (int)bucket;
    }
}

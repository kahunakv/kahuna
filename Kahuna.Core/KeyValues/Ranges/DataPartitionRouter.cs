using Kommander;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Kahuna-owned hash assignment for hash-routed key spaces — the replacement for
/// <c>IRaft.GetPartitionKey</c> under the decision that <b>Kahuna owns key→partition assignment</b>
/// (Kommander only hosts groups; its partitions are <c>Unrouted</c>, design §7.4).
///
/// <para>
/// The hash pool is <b>all user partitions <c>[1, InitialPartitions]</c></b>. Partition 0 is
/// Kommander's reserved system partition and never receives data. Partition 1 (which also hosts the
/// replicated meta map, <see cref="RangeMapStore.MetaPartitionId"/>) <b>is</b> in the pool: hash data
/// may share it with the meta map — they coexist via distinct replication log types, and reserving a
/// whole Raft group for the (low-volume) meta map would idle a partition, costly at small
/// <c>InitialPartitions</c>. Only <i>key-range</i> data avoids partition 1 — splits always allocate
/// fresh partitions <c>≥ <see cref="RangeMapStore.FirstDataPartitionId"/></c> (2), enforced by
/// <see cref="RangeMapStore.MutateAsync"/>.
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
    /// <summary>The lowest partition in the hash pool. Partition 0 is Kommander's system partition.</summary>
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

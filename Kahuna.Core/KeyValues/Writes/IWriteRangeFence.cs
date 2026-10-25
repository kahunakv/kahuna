using Kahuna.Server.KeyValues.Ranges;
using Kommander;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Re-checks, at aggregator flush time, whether a queued key-range write may still be proposed to its
/// partition: whether its descriptor still covers the key at the same generation and partition it was admitted
/// against, and whether the range is refusing writes because its data is being moved. Behind an interface so
/// the aggregator can be tested without a real range map.
/// </summary>
internal interface IWriteRangeFence
{
    /// <summary>True if the key routes to a key-range space whose descriptor moved/split since the write was
    /// admitted — the live generation or partition no longer matches the values captured at admission — so the
    /// item must be released (retryable), not proposed. Hash spaces are never stale.</summary>
    bool IsStale(string key, long admittedGeneration, int admittedPartitionId);

    /// <summary>True if the key lies in a range, or the part of one, that is refusing writes now because its
    /// data is being moved to another partition (<see cref="RangeDescriptor.QuiescedUntil"/>). A submission that
    /// would add new state to the range is released retryably instead of being appended after the move's
    /// catch-up copy. Hash spaces are never quiesced.</summary>
    bool IsQuiesced(string key);
}

/// <summary>Production fence over the live range map + key-space registry.</summary>
internal sealed class RangeMapWriteFence : IWriteRangeFence
{
    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly RangeMapStore rangeMapStore;

    private readonly IRaft raft;

    public RangeMapWriteFence(KeySpaceRegistry keySpaceRegistry, RangeMapStore rangeMapStore, IRaft raft)
    {
        this.keySpaceRegistry = keySpaceRegistry;
        this.rangeMapStore = rangeMapStore;
        this.raft = raft;
    }

    public bool IsStale(string key, long admittedGeneration, int admittedPartitionId) =>
        RangeRouting.IsKeyRange(keySpaceRegistry, key) &&
        RangeRouting.HasKeyRangeMovedSinceAdmission(rangeMapStore.Current, key, admittedGeneration, admittedPartitionId);

    /// <summary>
    /// Judged against this node's own applied map, on the partition leader, just before the entry is appended:
    /// the authoritative quiesce check for a durable prepare. It holds whichever node the coordinator ran on,
    /// and on a leader promoted mid-move that never held the in-memory range lock covering the same window.
    /// </summary>
    public bool IsQuiesced(string key) =>
        RangeRouting.IsQuiescedForWrite(RangeRouting.FindDescriptor(keySpaceRegistry, rangeMapStore.Current, key), key, raft);
}

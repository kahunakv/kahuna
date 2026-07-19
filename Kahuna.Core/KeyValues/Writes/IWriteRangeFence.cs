using Kahuna.Server.KeyValues.Ranges;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Re-checks, at aggregator flush time, whether a queued key-range write's descriptor still covers the key at
/// the same generation and partition it was admitted against. Behind an interface so the aggregator can be
/// tested without a real range map.
/// </summary>
internal interface IWriteRangeFence
{
    /// <summary>True if the key routes to a key-range space whose descriptor moved/split since the write was
    /// admitted — the live generation or partition no longer matches the values captured at admission — so the
    /// item must be released (retryable), not proposed. Hash spaces are never stale.</summary>
    bool IsStale(string key, long admittedGeneration, int admittedPartitionId);
}

/// <summary>Production fence over the live range map + key-space registry.</summary>
internal sealed class RangeMapWriteFence : IWriteRangeFence
{
    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly RangeMapStore rangeMapStore;

    public RangeMapWriteFence(KeySpaceRegistry keySpaceRegistry, RangeMapStore rangeMapStore)
    {
        this.keySpaceRegistry = keySpaceRegistry;
        this.rangeMapStore = rangeMapStore;
    }

    public bool IsStale(string key, long admittedGeneration, int admittedPartitionId) =>
        RangeRouting.IsKeyRange(keySpaceRegistry, key) &&
        RangeRouting.HasKeyRangeMovedSinceAdmission(rangeMapStore.Current, key, admittedGeneration, admittedPartitionId);
}

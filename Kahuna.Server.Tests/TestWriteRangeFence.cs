using Kahuna.Server.KeyValues.Ranges;
using Xunit;

namespace Kahuna.Server.Tests;

/// <summary>
/// Production-semantics tests for the direct-write range fence. A direct (auto-commit) key-range write is
/// resolved and fenced at admission, then may sit in the partition aggregator through a linger before it is
/// proposed. The generation captured at admission — even for a write that carried no routed generation, such
/// as a delete or extend — must let the flush-time fence detect a range that split, moved, or was dropped
/// while the write waited. These drive the real <see cref="RangeRouting"/> logic over a real
/// <see cref="RangeMap"/>; no cluster.
/// </summary>
public sealed class TestWriteRangeFence
{
    private const string Space = "t:r";

    private static string K(string suffix) => Space + "/" + suffix;

    private static RangeMap MapAt(int partitionId, long generation) =>
        new([new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = null, PartitionId = partitionId, Generation = generation }]);

    [Fact]
    public void Admission_CapturesLiveGeneration_EvenWhenWriteCarriedNone()
    {
        // A delete/extend routes with no generation (0). Admission must still capture the live descriptor
        // generation so the deferred flush fence has a real value to compare — not the 0 that disables it.
        RangeMap map = MapAt(partitionId: 2, generation: 5);

        Assert.True(RangeRouting.TryFenceKeyRange(map, K("a"), routedGeneration: 0, out int partitionId, out long generation));
        Assert.Equal(2, partitionId);
        Assert.Equal(5, generation);
    }

    [Fact]
    public void Admission_MatchingRoutedGeneration_PassesAndCaptures()
    {
        RangeMap map = MapAt(partitionId: 2, generation: 5);

        Assert.True(RangeRouting.TryFenceKeyRange(map, K("a"), routedGeneration: 5, out int partitionId, out long generation));
        Assert.Equal(2, partitionId);
        Assert.Equal(5, generation);
    }

    [Fact]
    public void Admission_StaleRoutedGeneration_Rejected()
    {
        RangeMap map = MapAt(partitionId: 2, generation: 5);

        Assert.False(RangeRouting.TryFenceKeyRange(map, K("a"), routedGeneration: 4, out _, out _));
    }

    [Fact]
    public void Flush_SameGenerationAndPartition_NotStale()
    {
        RangeMap map = MapAt(partitionId: 2, generation: 5);

        Assert.False(RangeRouting.HasKeyRangeMovedSinceAdmission(map, K("a"), admittedGeneration: 5, admittedPartitionId: 2));
    }

    [Fact]
    public void Flush_GenerationBumpedSinceAdmission_Stale()
    {
        // The delete/extend fix end to end: admitted at gen 5 (captured from a gen-0 write), the range then
        // splits and the generation bumps. The flush fence must now catch it; before the fix a gen-0 write
        // disabled the fence and would have been proposed to the pre-split partition.
        RangeMap admitted = MapAt(partitionId: 2, generation: 5);
        Assert.True(RangeRouting.TryFenceKeyRange(admitted, K("a"), routedGeneration: 0, out int partitionId, out long generation));

        RangeMap afterSplit = MapAt(partitionId: 2, generation: 6);
        Assert.True(RangeRouting.HasKeyRangeMovedSinceAdmission(afterSplit, K("a"), generation, partitionId));
    }

    [Fact]
    public void Flush_PartitionChangedSameGeneration_Stale()
    {
        // Defense in depth: even a move that somehow did not bump the generation is caught by the partition
        // comparison, so the write is never proposed to the partition that owned the key before the move.
        RangeMap afterMove = MapAt(partitionId: 3, generation: 5);

        Assert.True(RangeRouting.HasKeyRangeMovedSinceAdmission(afterMove, K("a"), admittedGeneration: 5, admittedPartitionId: 2));
    }

    [Fact]
    public void Flush_DescriptorDropped_Stale()
    {
        Assert.True(RangeRouting.HasKeyRangeMovedSinceAdmission(RangeMap.Empty, K("a"), admittedGeneration: 5, admittedPartitionId: 2));
    }
}

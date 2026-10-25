using Kahuna.Server.KeyValues.Ranges;
using Kommander.Time;
using Xunit;

namespace Kahuna.Server.Tests;

/// <summary>
/// Production-semantics tests for the write-side range fence over a real <see cref="RangeMap"/>; no cluster.
/// A key-range write is resolved at admission and may wait in the partition write scheduler through a linger
/// before it is proposed, so the dispatch fence must detect, from the generation and partition captured at
/// admission, a range that split, moved or was dropped while the write waited — and, for a submission that
/// would add new state, a range that started refusing writes because its data is being moved.
/// </summary>
public sealed class TestWriteRangeFence
{
    private const string Space = "t:r";

    private static string K(string suffix) => Space + "/" + suffix;

    private static RangeMap MapAt(int partitionId, long generation) =>
        new([new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = null, PartitionId = partitionId, Generation = generation }]);

    private static KeySpaceRegistry RangedRegistry()
    {
        KeySpaceRegistry registry = new();
        registry.RegisterKeyRange(Space);
        return registry;
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
        // Admitted at generation 5, then the range splits and the generation bumps: the flush fence catches it,
        // even for a delete or extend that carried no routed generation and captured the live one at admission.
        RangeMap afterSplit = MapAt(partitionId: 2, generation: 6);

        Assert.True(RangeRouting.HasKeyRangeMovedSinceAdmission(afterSplit, K("a"), admittedGeneration: 5, admittedPartitionId: 2));
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

    // ── the descriptor behind a quiesce check ───────────────────────────────────

    [Fact]
    public void FindDescriptor_KeyRangeKey_ResolvesItsCoveringDescriptor()
    {
        RangeMap map = new([
            new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = K("m"), PartitionId = 2, Generation = 3 },
            new RangeDescriptor { KeySpace = Space, StartKey = K("m"), EndKey = null, PartitionId = 7, Generation = 3 }
        ]);

        RangeDescriptor? low = RangeRouting.FindDescriptor(RangedRegistry(), map, K("a"));
        RangeDescriptor? high = RangeRouting.FindDescriptor(RangedRegistry(), map, K("z"));

        Assert.Equal(2, low?.PartitionId);
        Assert.Equal(7, high?.PartitionId);
    }

    [Fact]
    public void FindDescriptor_HashKey_IsNull()
    {
        // A hash-routed space has no descriptor to be quiesced: the fence asks nothing about it, even when a
        // descriptor of the same name happens to be in the map.
        RangeMap map = MapAt(partitionId: 2, generation: 5);

        Assert.Null(RangeRouting.FindDescriptor(new KeySpaceRegistry(), map, K("a")));
    }

    [Fact]
    public void FindDescriptor_KeyRangeKeyWithNoCoveringDescriptor_IsNull()
    {
        Assert.Null(RangeRouting.FindDescriptor(RangedRegistry(), RangeMap.Empty, K("a")));
    }

    // ── the quiesced sub-interval the fence judges a key against ────────────────

    private static RangeDescriptor QuiescedFrom(string? quiesceStart, string? quiesceEnd, HLCTimestamp until) => new()
    {
        KeySpace = Space,
        PartitionId = 2,
        Generation = 5,
        QuiescedUntil = until,
        QuiesceOwner = new HLCTimestamp(0, 1, 0),
        QuiesceStartKey = quiesceStart,
        QuiesceEndKey = quiesceEnd
    };

    [Fact]
    public void Quiesce_JudgesTheKeyAgainstTheMovingHalfOnly()
    {
        // A split moves [m, +inf) and quiesces exactly that half: a write below the split point is admitted
        // while one at or above it is refused, for the same descriptor and the same deadline.
        HLCTimestamp now = new(0, 1_000, 0);
        RangeDescriptor descriptor = QuiescedFrom(K("m"), null, until: new HLCTimestamp(0, 31_000, 0));

        Assert.False(descriptor.IsQuiescedAt(K("a"), now));
        Assert.True(descriptor.IsQuiescedAt(K("m"), now));
        Assert.True(descriptor.IsQuiescedAt(K("z"), now));
    }

    [Fact]
    public void Quiesce_LapsedDeadline_ReadsAsOpen()
    {
        // The deadline is what ends a window whose owner died: a stale stamp left on the descriptor admits writes.
        RangeDescriptor descriptor = QuiescedFrom(null, null, until: new HLCTimestamp(0, 1_000, 0));

        Assert.True(descriptor.IsQuiescedAt(K("a"), new HLCTimestamp(0, 999, 0)));
        Assert.False(descriptor.IsQuiescedAt(K("a"), new HLCTimestamp(0, 1_000, 0)));
        Assert.False(descriptor.IsQuiescedAt(K("a"), new HLCTimestamp(0, 5_000, 0)));
    }

    [Fact]
    public void Quiesce_NoDeadline_ReadsAsOpen()
    {
        RangeDescriptor descriptor = QuiescedFrom(null, null, until: HLCTimestamp.Zero);

        Assert.False(descriptor.IsQuiescedAt(K("a"), new HLCTimestamp(0, 1, 0)));
    }
}

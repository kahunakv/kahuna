using Kahuna.Server.KeyValues.Ranges;

namespace Kahuna.Tests.Server;

/// <summary>
/// Unit tests for the in-memory range-descriptor map. Pure data structure — no Raft,
/// no cluster. Covers the no-gap/no-overlap invariant (G1), half-open boundary semantics, null
/// (±inf) bounds, and ordinal ordering.
/// </summary>
public class TestRangeMap
{
    private static RangeDescriptor R(string keySpace, string? start, string? end, int partition, long gen = 1) =>
        new() { KeySpace = keySpace, StartKey = start, EndKey = end, PartitionId = partition, Generation = gen };

    // ── Find_NoGapNoOverlap_OverHandBuiltSet ──────────────────────────────────

    [Fact]
    public void Find_NoGapNoOverlap_OverHandBuiltSet()
    {
        // Contiguous cover of "t:r": (-inf,k30) (k30,k60) (k60,+inf)
        RangeMap map = new([
            R("t:r", null, "k30", 1),
            R("t:r", "k30", "k60", 2),
            R("t:r", "k60", null, 3),
        ]);

        Assert.True(map.Validate(out string? err), err);

        // Every probe resolves to exactly one descriptor, partitions as expected.
        Assert.Equal(1, map.Find("t:r", "k00")!.PartitionId);
        Assert.Equal(1, map.Find("t:r", "k29")!.PartitionId);
        Assert.Equal(2, map.Find("t:r", "k30")!.PartitionId); // start inclusive
        Assert.Equal(2, map.Find("t:r", "k59")!.PartitionId);
        Assert.Equal(3, map.Find("t:r", "k60")!.PartitionId); // start inclusive
        Assert.Equal(3, map.Find("t:r", "k99")!.PartitionId);

        // An intentionally-gapped set is rejected by Validate (gap between k30 and k60).
        RangeMap gapped = new([
            R("t:r", null, "k30", 1),
            R("t:r", "k60", null, 3),
        ]);
        Assert.False(gapped.Validate(out string? gapErr));
        Assert.Contains("gap", gapErr);

        // Probing the gap returns null (no range owns it).
        Assert.Null(gapped.Find("t:r", "k45"));

        // An overlapping set is rejected too.
        RangeMap overlap = new([
            R("t:r", null, "k60", 1),
            R("t:r", "k30", null, 2),
        ]);
        Assert.False(overlap.Validate(out string? overlapErr));
        Assert.Contains("overlap", overlapErr);
    }

    // ── Find_BoundaryConditions ───────────────────────────────────────────────

    [Fact]
    public void Find_BoundaryConditions()
    {
        RangeMap split = new([
            R("s", null, "m", 1),
            R("s", "m", null, 2),
        ]);
        Assert.True(split.Validate(out string? err), err);

        // Half-open: StartKey hit, EndKey miss.
        Assert.Equal(2, split.Find("s", "m")!.PartitionId);  // "m" belongs to [m, +inf), not [-inf, m)
        Assert.Equal(1, split.Find("s", "lz")!.PartitionId); // just below "m"

        // Null bounds behave as ±inf within the key space.
        Assert.Equal(1, split.Find("s", "")!.PartitionId);     // empty string < "m" → -inf side
        Assert.Equal(2, split.Find("s", "zzzzz")!.PartitionId); // far right → +inf side

        // A single range covering the whole space (both bounds null).
        RangeMap whole = new([R("s2", null, null, 7)]);
        Assert.True(whole.Validate(out _));
        Assert.Equal(7, whole.Find("s2", "anything")!.PartitionId);

        // Unregistered key space → null.
        Assert.Null(split.Find("not-registered", "m"));
    }

    // ── Find_OrdinalOrdering ──────────────────────────────────────────────────

    [Fact]
    public void Find_OrdinalOrdering()
    {
        // Ordinally "r/10" < "r/9" (because '1' (0x31) < '9' (0x39)). A numeric/culture sort would
        // place "r/9" before "r/10" and make this set overlap. Under ordinal it is a valid cover.
        Assert.True(string.CompareOrdinal("r/10", "r/9") < 0);

        RangeMap map = new([
            R("x", null, "r/10", 1),
            R("x", "r/10", "r/9", 2),
            R("x", "r/9", null, 3),
        ]);

        Assert.True(map.Validate(out string? err), err);

        Assert.Equal(2, map.Find("x", "r/10")!.PartitionId);  // start inclusive
        Assert.Equal(2, map.Find("x", "r/100")!.PartitionId); // "r/100" in [r/10, r/9)
        Assert.Equal(3, map.Find("x", "r/9")!.PartitionId);   // start inclusive
        Assert.Equal(3, map.Find("x", "r/90")!.PartitionId);  // "r/90" >= "r/9"
        Assert.Equal(1, map.Find("x", "r/0")!.PartitionId);   // "r/0" < "r/10"
    }
}

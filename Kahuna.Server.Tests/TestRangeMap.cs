using Kahuna.Server.KeyValues.Ranges;

namespace Kahuna.Server.Tests;

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

    // ── Descriptors_ReturnsAllEntriesInKeySpaceOrder ──────────────────────────

    [Fact]
    public void Descriptors_ReturnsAllEntriesInKeySpaceOrder()
    {
        RangeMap map = new([
            R("b", "b0", null,  4),
            R("a", "a1", "a2",  2),
            R("a", null, "a1",  1),
            R("b", null, "b0",  3),
        ]);

        IReadOnlyList<RangeDescriptor> d = map.Descriptors;
        Assert.Equal(4, d.Count);

        // Within each key space entries must be in ascending StartKey order.
        // Dictionary enumeration order is insertion-order in .NET, but the important
        // invariant is that each key space's slice is sorted (the flat list is the
        // concatenation of per-space sorted arrays).
        int[] partitions = d.Select(x => x.PartitionId).ToArray();

        // Space "a": partitions 1 (null start) then 2 ("a1" start); space "b": 3 then 4.
        // Exact order of spaces may vary, but within each space the sort must hold.
        int idxA1 = Array.IndexOf(partitions, 1);
        int idxA2 = Array.IndexOf(partitions, 2);
        int idxB3 = Array.IndexOf(partitions, 3);
        int idxB4 = Array.IndexOf(partitions, 4);

        Assert.True(idxA1 < idxA2, "partition 1 must precede partition 2 within key space 'a'");
        Assert.True(idxB3 < idxB4, "partition 3 must precede partition 4 within key space 'b'");

        // Repeated reads return the same (cached) array reference, not a new allocation.
        Assert.Same(map.Descriptors, map.Descriptors);
    }

    // ── FindAll_ReturnsSortedSliceForKeySpace ─────────────────────────────────

    [Fact]
    public void FindAll_ReturnsSortedSliceForKeySpace()
    {
        RangeMap map = new([
            R("ks", "m", null, 2),
            R("ks", null, "m", 1),
        ]);

        IReadOnlyList<RangeDescriptor> all = map.FindAll("ks");
        Assert.Equal(2, all.Count);
        Assert.Null(all[0].StartKey);    // null (−inf) sorts first
        Assert.Equal("m", all[1].StartKey);

        Assert.Empty(map.FindAll("missing-ks"));
    }

    // ── FindIntersecting_HalfOpenSemantics ────────────────────────────────────

    [Fact]
    public void FindIntersecting_HalfOpenSemantics()
    {
        // Three contiguous ranges: (-inf,"d") ("d","m") ("m",+inf)
        RangeMap map = new([
            R("ks", null, "d",  1),
            R("ks", "d",  "m",  2),
            R("ks", "m",  null, 3),
        ]);

        // Query spanning first two ranges.
        ArraySegment<RangeDescriptor> r1 = map.FindIntersecting("ks", "a", "e");
        Assert.Equal(2, r1.Count);
        Assert.Equal(1, r1[0].PartitionId);
        Assert.Equal(2, r1[1].PartitionId);

        // Query touching only the exact boundary "d" should include [d,m) but not (-inf,d). The
        // binary search lands on (-inf,d) as the leading candidate; it must be dropped because its
        // EndKey "d" is not > the query start "d", leaving just [d,m).
        ArraySegment<RangeDescriptor> r2 = map.FindIntersecting("ks", "d", "e");
        Assert.Single(r2);
        Assert.Equal(2, r2[0].PartitionId);

        // Null bounds = ±inf: full scan returns all three.
        ArraySegment<RangeDescriptor> r3 = map.FindIntersecting("ks", null, null);
        Assert.Equal(3, r3.Count);

        // Query beyond the last range returns only the last.
        ArraySegment<RangeDescriptor> r4 = map.FindIntersecting("ks", "z", null);
        Assert.Single(r4);
        Assert.Equal(3, r4[0].PartitionId);

        // A query landing strictly inside the first range keeps it (its EndKey "d" > start "a") and
        // extends across the contiguous window until a descriptor starts at/after the query end.
        ArraySegment<RangeDescriptor> r5 = map.FindIntersecting("ks", "a", "q");
        Assert.Equal(3, r5.Count);
        Assert.Equal(1, r5[0].PartitionId);
        Assert.Equal(3, r5[2].PartitionId);

        Assert.Equal(0, map.FindIntersecting("missing", null, null).Count);
    }
}

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// In-memory <c>key → range</c> lookup over a set of <see cref="RangeDescriptor"/>s, grouped by
/// key space and kept sorted by <see cref="RangeDescriptor.StartKey"/> (ordinal). This is the
/// read side of the range-descriptor map; Task 2 makes it the replicated source of
/// truth on the meta partition. Task 1 is pure in-memory with no Raft.
///
/// <para>
/// All comparisons are ordinal. A null <c>StartKey</c> sorts as −infinity (lowest); a null
/// <c>EndKey</c> means +infinity. The map is immutable once constructed — mutation arrives in
/// Task 2 via replicated meta entries.
/// </para>
/// </summary>
internal sealed class RangeMap
{
    /// <summary>Per key space, descriptors sorted ascending by <c>StartKey</c> (null first).</summary>
    private readonly Dictionary<string, RangeDescriptor[]> bySpace;

    public RangeMap(IEnumerable<RangeDescriptor> descriptors)
    {
        bySpace = descriptors
            .GroupBy(static d => d.KeySpace, StringComparer.Ordinal)
            .ToDictionary(
                static g => g.Key,
                static g => g.OrderBy(static d => d.StartKey, StartKeyComparer.Instance).ToArray(),
                StringComparer.Ordinal);
    }

    /// <summary>An empty map (no ranges registered for any key space).</summary>
    public static RangeMap Empty { get; } = new(Array.Empty<RangeDescriptor>());

    /// <summary>
    /// All descriptors across every key space, sorted within each space by <c>StartKey</c>. Used by
    /// <see cref="RangeMapStore"/> to snapshot the map for replication.
    /// </summary>
    public IReadOnlyList<RangeDescriptor> Descriptors =>
        bySpace.Values.SelectMany(static r => r).ToArray();

    /// <summary>
    /// Resolves the descriptor whose half-open ordinal interval contains <paramref name="key"/>
    /// within <paramref name="keySpace"/>, or null if no range covers it (gap, or key space not
    /// registered). O(log n) over the key space's descriptors.
    /// </summary>
    public RangeDescriptor? Find(string keySpace, string key)
    {
        if (!bySpace.TryGetValue(keySpace, out RangeDescriptor[]? ranges) || ranges.Length == 0)
            return null;

        // Rightmost descriptor whose StartKey <= key (null StartKey = -inf, always <= key).
        int lo = 0, hi = ranges.Length - 1, idx = -1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (StartLessOrEqual(ranges[mid].StartKey, key))
            {
                idx = mid;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        if (idx < 0)
            return null;

        RangeDescriptor candidate = ranges[idx];
        return candidate.Contains(key) ? candidate : null;
    }

    /// <summary>Returns all descriptors for <paramref name="keySpace"/> in StartKey order.</summary>
    public IReadOnlyList<RangeDescriptor> FindAll(string keySpace) =>
        bySpace.TryGetValue(keySpace, out RangeDescriptor[]? ranges)
            ? ranges
            : Array.Empty<RangeDescriptor>();

    /// <summary>
    /// Returns descriptors for <paramref name="keySpace"/> whose half-open interval
    /// <c>[StartKey, EndKey)</c> intersects <c>[<paramref name="startKey"/>,
    /// <paramref name="endKey"/>)</c>, in StartKey order.
    /// Null <paramref name="startKey"/> = −∞; null <paramref name="endKey"/> = +∞.
    /// O(log n + k) where k is the number of matching descriptors: a binary search locates
    /// the first candidate (rightmost descriptor with StartKey ≤ startKey), then a forward
    /// scan stops as soon as D.StartKey ≥ endKey.
    /// </summary>
    public IReadOnlyList<RangeDescriptor> FindIntersecting(string keySpace, string? startKey, string? endKey)
    {
        if (!bySpace.TryGetValue(keySpace, out RangeDescriptor[]? ranges) || ranges.Length == 0)
            return Array.Empty<RangeDescriptor>();

        // Binary-search for the rightmost descriptor with StartKey ≤ startKey.
        // Descriptors before it have EndKey ≤ startKey (no-gap invariant) and cannot intersect.
        int first = 0;
        if (startKey is not null)
        {
            int lo = 0, hi = ranges.Length - 1;
            while (lo <= hi)
            {
                int mid = lo + ((hi - lo) >> 1);
                if (StartLessOrEqual(ranges[mid].StartKey, startKey))
                {
                    first = mid;
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }
        }

        var result = new List<RangeDescriptor>();
        for (int i = first; i < ranges.Length; i++)
        {
            RangeDescriptor d = ranges[i];

            // [D.Start, D.End) intersects [startKey, endKey) iff D.End > startKey && D.Start < endKey.
            // D.End > startKey: guaranteed for i > first by the no-gap invariant (D.StartKey >
            // startKey implies D.EndKey > D.StartKey > startKey); still checked for i == first.
            bool endAfterQueryStart = d.EndKey is null
                                   || startKey is null
                                   || string.CompareOrdinal(d.EndKey, startKey) > 0;

            // D.Start < endKey: once false, all subsequent descriptors also fail (sorted order).
            bool startBeforeQueryEnd = d.StartKey is null
                                    || endKey is null
                                    || string.CompareOrdinal(d.StartKey, endKey) < 0;

            if (!startBeforeQueryEnd)
                break;

            if (endAfterQueryStart)
                result.Add(d);
        }
        return result;
    }

    /// <summary>
    /// Verifies the no-gap/no-overlap invariant for every key space: within a key
    /// space the descriptors must be contiguous and non-overlapping, each range non-empty, with
    /// a null bound only at the corresponding extreme. Returns false with a human-readable
    /// <paramref name="error"/> on the first violation. This is invariant <b>G1</b> in the task
    /// plan and is asserted after every descriptor mutation.
    /// </summary>
    public bool Validate(out string? error)
    {
        foreach ((string keySpace, RangeDescriptor[] ranges) in bySpace)
        {
            for (int i = 0; i < ranges.Length; i++)
            {
                RangeDescriptor r = ranges[i];

                // Non-empty: when both bounds are concrete, Start must be strictly below End.
                if (r.StartKey is not null && r.EndKey is not null &&
                    string.CompareOrdinal(r.StartKey, r.EndKey) >= 0)
                {
                    error = $"{keySpace}: empty/inverted range {r}";
                    return false;
                }

                // A null StartKey (-inf) is only valid at the very start of the key space.
                if (r.StartKey is null && i != 0)
                {
                    error = $"{keySpace}: -inf StartKey not at the first range ({r})";
                    return false;
                }

                if (i == 0)
                    continue;

                RangeDescriptor prev = ranges[i - 1];

                // prev EndKey = +inf cannot be followed by another range.
                if (prev.EndKey is null)
                {
                    error = $"{keySpace}: +inf EndKey on {prev} is followed by {r}";
                    return false;
                }

                // Contiguity: prev.EndKey must exactly meet r.StartKey (ordinal).
                int cmp = string.CompareOrdinal(prev.EndKey, r.StartKey);
                if (cmp < 0)
                {
                    error = $"{keySpace}: gap between {prev} and {r}";
                    return false;
                }
                if (cmp > 0)
                {
                    error = $"{keySpace}: overlap between {prev} and {r}";
                    return false;
                }
            }
        }

        error = null;
        return true;
    }

    /// <summary>Convenience: <see cref="Validate(out string?)"/> discarding the message.</summary>
    public bool IsValid => Validate(out _);

    private static bool StartLessOrEqual(string? start, string key) =>
        start is null || string.CompareOrdinal(start, key) <= 0;

    /// <summary>Orders <c>StartKey</c> ascending with null (−infinity) sorting first.</summary>
    private sealed class StartKeyComparer : IComparer<string?>
    {
        public static readonly StartKeyComparer Instance = new();

        public int Compare(string? x, string? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;
            return string.CompareOrdinal(x, y);
        }
    }
}

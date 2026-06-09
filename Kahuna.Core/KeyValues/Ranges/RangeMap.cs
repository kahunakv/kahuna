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

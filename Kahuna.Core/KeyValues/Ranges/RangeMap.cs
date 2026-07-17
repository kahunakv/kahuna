namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// In-memory <c>key → range</c> lookup over a set of <see cref="RangeDescriptor"/>s, grouped by
/// key space and kept sorted by <see cref="RangeDescriptor.StartKey"/> (ordinal). This is the
/// read side of the range-descriptor map; the replicated source of
/// truth on the meta partition. The map itself is pure in-memory with no Raft.
///
/// <para>
/// All comparisons are ordinal. A null <c>StartKey</c> sorts as −infinity (lowest); a null
/// <c>EndKey</c> means +infinity. The map is immutable once constructed — mutation arrives in
/// via replicated meta entries.
/// </para>
/// </summary>
internal sealed class RangeMap
{
    /// <summary>Per key space, descriptors sorted ascending by <c>StartKey</c> (null first).</summary>
    private readonly Dictionary<string, RangeDescriptor[]> bySpace;

    /// <summary>Pre-flattened view across all key spaces, built once in the constructor.</summary>
    private readonly RangeDescriptor[] _descriptors;

    public RangeMap(IEnumerable<RangeDescriptor> descriptors)
    {
        bySpace = descriptors
            .GroupBy(static d => d.KeySpace, StringComparer.Ordinal)
            .ToDictionary(
                static g => g.Key,
                static g => g.OrderBy(static d => d.StartKey, StartKeyComparer.Instance).ToArray(),
                StringComparer.Ordinal);

        _descriptors = [.. bySpace.Values.SelectMany(static r => r)];
    }

    /// <summary>An empty map (no ranges registered for any key space).</summary>
    public static RangeMap Empty { get; } = new(Array.Empty<RangeDescriptor>());

    /// <summary>
    /// All descriptors across every key space, sorted within each space by <c>StartKey</c>. Used by
    /// <see cref="RangeMapStore"/> to snapshot the map for replication.
    /// </summary>
    public IReadOnlyList<RangeDescriptor> Descriptors => _descriptors;

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
    /// <remarks>
    /// The result is a slice over this key space's immutable, StartKey-sorted descriptor array —
    /// no copy. The map is replaced wholesale on mutation, so the backing array never changes under
    /// the returned segment. Consume it synchronously or copy it before crossing an <c>await</c>.
    /// </remarks>
    public ArraySegment<RangeDescriptor> FindIntersecting(string keySpace, string? startKey, string? endKey)
    {
        if (!bySpace.TryGetValue(keySpace, out RangeDescriptor[]? ranges) || ranges.Length == 0)
            return ArraySegment<RangeDescriptor>.Empty;

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

        // The intersecting descriptors form a contiguous window [start, end): they run from `first`
        // forward until one starts at or beyond endKey. Only the leading candidate can fail the
        // lower-bound test (D.End > startKey) — every later descriptor satisfies it by the no-gap
        // invariant — so the window drops at most that single descriptor off the front.
        int start = first;
        {
            RangeDescriptor d = ranges[start];

            // D.Start < endKey: if the first candidate already starts beyond the query, nothing
            // in this key space intersects (later descriptors start even higher).
            bool startBeforeQueryEnd = d.StartKey is null
                                    || endKey is null
                                    || string.CompareOrdinal(d.StartKey, endKey) < 0;
            if (!startBeforeQueryEnd)
                return ArraySegment<RangeDescriptor>.Empty;

            // D.End > startKey: the only descriptor that can fail this is the leading one.
            bool endAfterQueryStart = d.EndKey is null
                                   || startKey is null
                                   || string.CompareOrdinal(d.EndKey, startKey) > 0;
            if (!endAfterQueryStart)
                start++;
        }

        // Extend while D.Start < endKey; once false, all subsequent descriptors also fail (sorted).
        int end = start;
        while (end < ranges.Length)
        {
            RangeDescriptor d = ranges[end];
            bool startBeforeQueryEnd = d.StartKey is null
                                    || endKey is null
                                    || string.CompareOrdinal(d.StartKey, endKey) < 0;
            if (!startBeforeQueryEnd)
                break;
            end++;
        }

        return new ArraySegment<RangeDescriptor>(ranges, start, end - start);
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


using Kommander.Time;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Pure policy: given an ordered sample of <c>(key, LastModified)</c> entries for a range,
/// computes the split key.
///
/// <para>
/// <b>Normal distribution:</b> returns the median key (ordinal position <c>count/2</c>).
/// </para>
///
/// <para>
/// <b>Monotonic-append detection (§8 — "gen_id() / hot tail" pattern):</b> if the most-recently
/// written quarter of the sample are clustered in the last quarter of the ordinal key space
/// (i.e. 75 %+ of the recently-written keys sit above the 75th-percentile key), the range is
/// monotonically-appended at its tail. In that case, splitting at the median would put all future
/// writes on the right half and leave the left half cold. Instead, the policy splits at the
/// 75th-percentile key, which (a) moves the hot tail to the new partition and (b) keeps a
/// reasonably-sized cold left half.
/// </para>
/// </summary>
internal static class RangeSplitPolicy
{
    /// <summary>
    /// Computes the split key for the given ordered key sample.
    /// </summary>
    /// <param name="sample">
    /// Keys in ordinal order, each paired with their <c>LastModified</c> timestamp.
    /// Must be non-empty and already sorted ascending by key.
    /// </param>
    /// <param name="threshold">
    /// The minimum number of keys a range must contain before a split is triggered.
    /// If <c>sample.Count &lt; threshold</c> the method returns <c>null</c>.
    /// </param>
    /// <param name="minRangeSize">
    /// Minimum keys each half must have after the split.
    /// Ensures neither child range is trivially small.
    /// </param>
    /// <returns>The split key, or <c>null</c> if the range should not be split.</returns>
    public static string? ComputeSplitKey(
        IReadOnlyList<(string Key, HLCTimestamp LastModified)> sample,
        int threshold,
        int minRangeSize)
    {
        if (sample.Count < threshold)
            return null;

        int splitIndex = IsMonotonicAppend(sample)
            ? ComputePercentileIndex(sample.Count, 75)
            : ComputePercentileIndex(sample.Count, 50);

        // Clamp so both halves have at least minRangeSize keys.
        splitIndex = Math.Clamp(splitIndex, minRangeSize, sample.Count - minRangeSize);

        // After clamping, re-check feasibility.
        if (splitIndex <= 0 || splitIndex >= sample.Count)
            return null;

        return sample[splitIndex].Key;
    }

    /// <summary>
    /// Returns <c>true</c> when inserts are top-skewed (monotonic-append pattern).
    ///
    /// <para>
    /// Heuristic: consider the 25 % most-recently-modified keys. If 75 %+ of them
    /// sit in the top-25 % ordinal tail of the sample, the range has a hot tail.
    /// </para>
    /// </summary>
    internal static bool IsMonotonicAppend(IReadOnlyList<(string Key, HLCTimestamp LastModified)> sample)
    {
        if (sample.Count < 4)
            return false;

        // Find the timestamp cutoff: top 25 % most-recently-modified.
        // Sort a copy by LastModified descending to get the cutoff without mutating the input.
        int recentCount = Math.Max(1, sample.Count / 4);
        HLCTimestamp cutoff = GetNthLargestTimestamp(sample, recentCount);

        // Count how many recently-modified keys fall in the top-25 % ordinal tail.
        int tailStart = ComputePercentileIndex(sample.Count, 75);
        int recentInTail = 0;
        int totalRecent  = 0;

        for (int i = 0; i < sample.Count; i++)
        {
            if (sample[i].LastModified.CompareTo(cutoff) >= 0)
            {
                totalRecent++;
                if (i >= tailStart)
                    recentInTail++;
            }
        }

        if (totalRecent == 0)
            return false;

        // Hot-tail threshold: 75 % of recent writes are in the top quartile.
        return recentInTail * 4 >= totalRecent * 3;
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static int ComputePercentileIndex(int count, int percentile) =>
        (int)Math.Round(count * percentile / 100.0);

    /// <summary>
    /// Returns the timestamp of the <paramref name="n"/>-th largest entry (1-based)
    /// using partial selection to avoid a full sort.
    /// </summary>
    private static HLCTimestamp GetNthLargestTimestamp(
        IReadOnlyList<(string Key, HLCTimestamp LastModified)> sample,
        int n)
    {
        // Copy just the timestamps into a local list for partial sort.
        List<HLCTimestamp> ts = new(sample.Count);
        foreach ((_, HLCTimestamp lm) in sample)
            ts.Add(lm);

        // Partial descending sort: we only need the nth element.
        ts.Sort((a, b) => b.CompareTo(a));
        return ts[n - 1];
    }
}

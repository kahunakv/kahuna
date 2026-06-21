
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
/// <b>Monotonic-append detection ("gen_id() / hot tail" pattern):</b> if the most-recently
/// written quarter of the sample are clustered in the last quarter of the ordinal key space
/// (i.e. 75 %+ of the recently-written keys sit above the 75th-percentile key), the range is
/// monotonically-appended at its tail. In that case, splitting at the median would put all future
/// writes on the right half and leave the left half cold. Instead, the policy splits at the
/// 75th-percentile key, which (a) moves the hot tail to the new partition and (b) keeps a
/// reasonably-sized cold left half.
/// </para>
///
/// <para>
/// <b>Write-centroid split (K2.4 / K1b):</b> when a <c>writeFrequency</c> map is supplied and
/// non-empty, the policy finds the split index that most evenly bisects the cumulative write
/// count across the sample (the write centroid). If the centroid cannot put each child below
/// <see cref="IndivisibleImbalance"/> the function still returns the best-effort split key but
/// <paramref name="achievableImbalance"/> is set to the worst-case fraction so callers can apply
/// the K2.3 indivisibility guard.
/// </para>
/// </summary>
internal static class RangeSplitPolicy
{
    /// <summary>
    /// Imbalance fraction at or above which a range is considered indivisible by load
    /// (K2.3). A value of 1.0 means all writes are on one key.
    /// </summary>
    internal const double IndivisibleImbalance = 1.0;

    /// <summary>
    /// Computes the split key for the given ordered key sample using only key count
    /// (count-based, unchanged from earlier behaviour).
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
        return ComputeSplitKey(sample, threshold, minRangeSize, null, out _);
    }

    /// <summary>
    /// Computes the split key for the given ordered key sample, optionally using a
    /// write-frequency histogram to locate the write centroid (K2.4 / K1b).
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
    /// <param name="writeFrequency">
    /// Optional per-key write-count map (K1b snapshot). When non-null and non-empty the policy
    /// picks the key that best bisects cumulative writes. When null or empty the policy falls
    /// back to the count-based median/percentile path.
    /// </param>
    /// <param name="achievableImbalance">
    /// Output: the best achievable write-imbalance fraction in <c>[0.5, 1.0]</c>.
    /// <c>max(leftWrites, rightWrites) / totalWrites</c> at the chosen split index.
    /// <c>0</c> when <paramref name="writeFrequency"/> is null or empty (count path).
    /// Callers use this to enforce the K2.3 indivisibility guard.
    /// </param>
    /// <returns>The split key, or <c>null</c> if the range should not be split.</returns>
    public static string? ComputeSplitKey(
        IReadOnlyList<(string Key, HLCTimestamp LastModified)> sample,
        int threshold,
        int minRangeSize,
        IReadOnlyDictionary<string, long>? writeFrequency,
        out double achievableImbalance)
    {
        achievableImbalance = 0;

        if (sample.Count < threshold)
            return null;

        // Guard: if the sample can't produce two halves each with minRangeSize keys,
        // refuse to split. This can happen when threshold < 2*minRangeSize (misconfiguration)
        // or when the sample is genuinely too small. Math.Clamp(x, lo, hi) throws when lo > hi,
        // so we must check before reaching it.
        if (sample.Count < 2 * minRangeSize)
            return null;

        // ── Write-centroid path (K2.4) ─────────────────────────────────────────
        // Use when the write-frequency histogram is warm (non-empty) and at least some of the
        // sample keys are present in it, so the centroid is meaningful. Falls back to the
        // count-based path when the histogram is cold (post-failover blind window).
        if (writeFrequency is { Count: > 0 })
        {
            string? centroidKey = TryComputeWriteCentroid(
                sample, minRangeSize, writeFrequency, out achievableImbalance);

            if (centroidKey is not null)
                return centroidKey;

            // Histogram present but no sample keys matched (e.g. histogram is warming up on a
            // different key set). Fall through to the count-based path.
            achievableImbalance = 0;
        }

        // ── Count-based path (unchanged) ──────────────────────────────────────
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

    // ── write-centroid helpers ────────────────────────────────────────────────

    /// <summary>
    /// Finds the split index that best bisects cumulative writes across the ordered sample.
    /// Returns null when fewer than <c>minRangeSize * 2</c> sample keys have a non-zero
    /// write count (histogram too sparse to be meaningful).
    /// </summary>
    private static string? TryComputeWriteCentroid(
        IReadOnlyList<(string Key, HLCTimestamp LastModified)> sample,
        int minRangeSize,
        IReadOnlyDictionary<string, long> writeFrequency,
        out double achievableImbalance)
    {
        achievableImbalance = 0;

        // Build per-sample-index write counts.
        long[] weights = new long[sample.Count];
        long totalWrites = 0;

        for (int i = 0; i < sample.Count; i++)
        {
            long w = writeFrequency.TryGetValue(sample[i].Key, out long freq) ? freq : 0;
            weights[i] = w;
            totalWrites += w;
        }

        // No write signal at all — histogram is cold; caller falls back to count-based path.
        if (totalWrites == 0)
            return null;

        // Prefix-sum sweep: find the split index i (left = [0..i), right = [i..n)) that
        // minimises |leftSum − rightSum|, i.e. puts the split at the write centroid.
        // Pre-seed leftSum with the first minRangeSize elements so the invariant holds:
        // at the start of iteration i, leftSum = sum(weights[0..i-1]).
        long leftSum = 0;
        for (int j = 0; j < minRangeSize; j++)
            leftSum += weights[j];

        long bestDiff = long.MaxValue;
        int bestIdx = -1;

        for (int i = minRangeSize; i <= sample.Count - minRangeSize; i++)
        {
            // At this point leftSum = sum(weights[0..i-1]) — the left half for split at i.
            long rightSum = totalWrites - leftSum;
            long diff = Math.Abs(leftSum - rightSum);

            if (diff < bestDiff)
            {
                bestDiff = diff;
                bestIdx = i;
            }

            // Advance leftSum for the next iteration: include weights[i] in the left half.
            leftSum += weights[i];
        }

        if (bestIdx <= 0 || bestIdx >= sample.Count)
            return null;

        // Compute and report the achievable imbalance fraction for the chosen split.
        long bestLeft = 0;
        for (int i = 0; i < bestIdx; i++)
            bestLeft += weights[i];
        long bestRight = totalWrites - bestLeft;
        achievableImbalance = (double)Math.Max(bestLeft, bestRight) / totalWrites;

        return sample[bestIdx].Key;
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
    /// Returns the timestamp of the <paramref name="n"/>-th largest entry (1-based).
    /// Uses a full O(n log n) sort on a copied timestamp list; the list is bounded by
    /// <see cref="RangeSplitTrigger.MaxSampleKeys"/> so this is acceptable.
    /// </summary>
    private static HLCTimestamp GetNthLargestTimestamp(
        IReadOnlyList<(string Key, HLCTimestamp LastModified)> sample,
        int n)
    {
        List<HLCTimestamp> ts = new(sample.Count);
        foreach ((_, HLCTimestamp lm) in sample)
            ts.Add(lm);

        ts.Sort((a, b) => b.CompareTo(a));
        return ts[n - 1];
    }
}

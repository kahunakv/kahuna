
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Kahuna.Server.KeyValues.Ranges;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Pure unit tests for <see cref="RangeSplitPolicy.ComputeSplitKey"/> —
/// both the count-based (original) path and the write-centroid (K1b/K2.4) path.
/// No cluster, no actors, no I/O.
/// </summary>
public sealed class TestRangeSplitPolicy
{
    // ── helpers ──────────────────────────────────────────────────────────────

    private static IReadOnlyList<(string Key, HLCTimestamp LastModified)> MakeSample(
        int count, string prefix = "k")
    {
        var list = new List<(string, HLCTimestamp)>(count);
        for (int i = 0; i < count; i++)
            list.Add(($"{prefix}{i:D4}", HLCTimestamp.Zero));
        return list;
    }

    // ── count-based path (backward compat) ────────────────────────────────────

    [Fact]
    public void CountPath_BelowThreshold_ReturnsNull()
    {
        var sample = MakeSample(50);
        Assert.Null(RangeSplitPolicy.ComputeSplitKey(sample, threshold: 100, minRangeSize: 10));
    }

    [Fact]
    public void CountPath_AboveThreshold_ReturnsSplitKey()
    {
        var sample = MakeSample(200);
        string? key = RangeSplitPolicy.ComputeSplitKey(sample, threshold: 100, minRangeSize: 10);
        Assert.NotNull(key);
        // Median of 200 is at index 100.
        Assert.Equal("k0100", key);
    }

    [Fact]
    public void CountPath_TooSmallForTwoHalves_ReturnsNull()
    {
        var sample = MakeSample(15);
        // threshold = 10, minRangeSize = 10 → needs at least 20 keys
        Assert.Null(RangeSplitPolicy.ComputeSplitKey(sample, threshold: 10, minRangeSize: 10));
    }

    // ── write-centroid path (K1b / K2.4) ────────────────────────────────────

    [Fact]
    public void WriteCentroid_NullFrequency_FallsBackToMedian()
    {
        var sample = MakeSample(100);
        string? key = RangeSplitPolicy.ComputeSplitKey(
            sample, threshold: 50, minRangeSize: 5, writeFrequency: null, out double imbalance);
        Assert.NotNull(key);
        Assert.Equal(0, imbalance); // count path sets achievableImbalance = 0
    }

    [Fact]
    public void WriteCentroid_EmptyFrequency_FallsBackToMedian()
    {
        var sample = MakeSample(100);
        var freq = new Dictionary<string, long>(); // empty
        string? key = RangeSplitPolicy.ComputeSplitKey(
            sample, threshold: 50, minRangeSize: 5, writeFrequency: freq, out double imbalance);
        Assert.NotNull(key);
        Assert.Equal(0, imbalance);
    }

    [Fact]
    public void WriteCentroid_UniformLoad_SplitsNearMedian()
    {
        // All 100 keys have the same write count → centroid ≈ median.
        var sample = MakeSample(100);
        var freq = sample.ToDictionary(e => e.Key, _ => 10L);

        string? key = RangeSplitPolicy.ComputeSplitKey(
            sample, threshold: 50, minRangeSize: 5, writeFrequency: freq, out double imbalance);

        Assert.NotNull(key);
        // With uniform weights the best split bisects writes as evenly as possible.
        // minRangeSize=5; the sweep starts at i=5 where leftSum = sum(weights[0..4]) = 5*10 = 50.
        // The sweep finds the split where |leftSum - rightSum| is minimised.
        // totalWrites = 1000; perfect balance = 500. leftSum reaches 500 at i=50.
        Assert.Equal("k0050", key);
        // Imbalance at a perfect split is exactly 0.5.
        Assert.InRange(imbalance, 0.49, 0.51);
    }

    [Fact]
    public void WriteCentroid_SkewedLoad_SplitsAtWriteCentroid()
    {
        // Keys k0000–k0079 have 1 write each; keys k0080–k0099 have 100 writes each.
        // Total = 80 * 1 + 20 * 100 = 2_080. Centroid ≈ index where leftSum ≈ 1_040.
        // Left 80 keys contribute 80. First hot key at 80 adds 100 → leftSum = 180 still < 1040.
        // We need to sweep deeper into the hot tail. Let's verify the result is past index 80.
        var sample = MakeSample(100);
        var freq = new Dictionary<string, long>();
        for (int i = 0; i < 80; i++)
            freq[$"k{i:D4}"] = 1;
        for (int i = 80; i < 100; i++)
            freq[$"k{i:D4}"] = 100;

        string? key = RangeSplitPolicy.ComputeSplitKey(
            sample, threshold: 50, minRangeSize: 5, writeFrequency: freq, out double imbalance);

        Assert.NotNull(key);
        // The centroid must be past k0079 because that's where the load lives.
        int idx = sample.ToList().IndexOf(sample.First(e => e.Key == key));
        Assert.True(idx > 80, $"Expected split index > 80, got {idx} ({key})");
        // The best achievable imbalance for this skewed distribution should be > 0.5.
        Assert.InRange(imbalance, 0.5, 1.0);
    }

    [Fact]
    public void WriteCentroid_AllWritesOnOneKey_ReportsHighImbalance()
    {
        // All writes on a single key — the range is effectively indivisible.
        var sample = MakeSample(100);
        var freq = new Dictionary<string, long> { ["k0050"] = 999 };

        string? key = RangeSplitPolicy.ComputeSplitKey(
            sample, threshold: 50, minRangeSize: 5, writeFrequency: freq, out double imbalance);

        // A split key is still returned (the policy doesn't refuse here — the caller applies K2.3).
        Assert.NotNull(key);
        // The imbalance must be very high because one key holds all the writes.
        Assert.True(imbalance > 0.9, $"Expected imbalance > 0.9, got {imbalance:F3}");
    }

    [Fact]
    public void WriteCentroid_BelowThreshold_ReturnsNull()
    {
        var sample = MakeSample(20);
        var freq = sample.ToDictionary(e => e.Key, _ => 5L);
        string? key = RangeSplitPolicy.ComputeSplitKey(
            sample, threshold: 50, minRangeSize: 5, writeFrequency: freq, out _);
        Assert.Null(key);
    }

    [Fact]
    public void WriteCentroid_BelowMinRangeSize_ReturnsNull()
    {
        // 15 keys, threshold=10, minRangeSize=10 → need 20 minimum
        var sample = MakeSample(15);
        var freq = sample.ToDictionary(e => e.Key, _ => 1L);
        string? key = RangeSplitPolicy.ComputeSplitKey(
            sample, threshold: 10, minRangeSize: 10, writeFrequency: freq, out _);
        Assert.Null(key);
    }

    // ── KeyWriteFrequencyTracker unit tests ──────────────────────────────────

    [Fact]
    public void Tracker_RecordAndSnapshot()
    {
        var tracker = new KeyWriteFrequencyTracker();
        tracker.RecordWrite("a");
        tracker.RecordWrite("a");
        tracker.RecordWrite("b");

        IReadOnlyDictionary<string, long> snap = tracker.GetSnapshot();
        Assert.Equal(2L, snap["a"]);
        Assert.Equal(1L, snap["b"]);
    }

    [Fact]
    public void Tracker_Decay_HalvesCounts()
    {
        var tracker = new KeyWriteFrequencyTracker();
        tracker.RecordWrite("a");
        tracker.RecordWrite("a");
        tracker.RecordWrite("a");
        tracker.RecordWrite("a"); // count = 4

        tracker.Decay(); // count = 2

        IReadOnlyDictionary<string, long> snap = tracker.GetSnapshot();
        Assert.Equal(2L, snap["a"]);
    }

    [Fact]
    public void Tracker_Decay_RemovesZeroEntries()
    {
        var tracker = new KeyWriteFrequencyTracker();
        tracker.RecordWrite("a"); // count = 1

        tracker.Decay(); // 1 >> 1 = 0 → removed

        Assert.True(tracker.IsEmpty);
    }

    [Fact]
    public void Tracker_Clear_WipesAllEntries()
    {
        var tracker = new KeyWriteFrequencyTracker();
        tracker.RecordWrite("a");
        tracker.RecordWrite("b");
        tracker.Clear();
        Assert.True(tracker.IsEmpty);
    }

    [Fact]
    public void Tracker_FilterForChild_KeepsInRangeOnly()
    {
        var tracker = new KeyWriteFrequencyTracker();
        tracker.RecordWrite("k0010");
        tracker.RecordWrite("k0050");
        tracker.RecordWrite("k0090");

        // Left child: [null, k0050) — should include k0010, exclude k0050 and k0090
        KeyWriteFrequencyTracker left = tracker.FilterForChild(null, "k0050");
        IReadOnlyDictionary<string, long> snap = left.GetSnapshot();
        Assert.True(snap.ContainsKey("k0010"));
        Assert.False(snap.ContainsKey("k0050"));
        Assert.False(snap.ContainsKey("k0090"));

        // Right child: [k0050, null) — should include k0050 and k0090, exclude k0010
        KeyWriteFrequencyTracker right = tracker.FilterForChild("k0050", null);
        snap = right.GetSnapshot();
        Assert.False(snap.ContainsKey("k0010"));
        Assert.True(snap.ContainsKey("k0050"));
        Assert.True(snap.ContainsKey("k0090"));
    }
}

using Kahuna.Server.KeyValues.Transactions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for <see cref="FinalizeLatencyEstimator"/>, the rolling p99 that sizes the durable-transaction
/// decision deadline. Verifies warmup returns zero (deadline falls back to the floor), that the p99 tracks the
/// recent distribution, and that the ring buffer keeps only the most recent window.
/// </summary>
public sealed class TestFinalizeLatencyEstimator
{
    [Fact]
    public void WarmupBeforeFirstRecompute_ReportsZero()
    {
        FinalizeLatencyEstimator estimator = new(window: 512, recomputeEvery: 32);

        // Fewer samples than the recompute cadence: the cached p99 has never been computed, so it reads zero and
        // the derived deadline falls back to the configured floor.
        for (int i = 0; i < 10; i++)
            estimator.Record(100);

        Assert.Equal(0, estimator.P99Ms);
    }

    [Fact]
    public void P99_TracksRecentDistribution()
    {
        FinalizeLatencyEstimator estimator = new(window: 100, recomputeEvery: 1);

        // 98 samples at 10ms and two tall outliers at 500ms. p99 index = ceil(0.99*100)-1 = 98 (0-based), which
        // with a sorted [10×98, 500, 500] lands on the tall tail — a single 1% outlier would not move p99, but a
        // 2% tail does.
        for (int i = 0; i < 98; i++)
            estimator.Record(10);
        estimator.Record(500);
        estimator.Record(500);

        Assert.Equal(500, estimator.P99Ms);
    }

    [Fact]
    public void P99_RoundsUp_ForModerateSpread()
    {
        FinalizeLatencyEstimator estimator = new(window: 100, recomputeEvery: 1);

        for (int i = 1; i <= 100; i++)
            estimator.Record(i); // 1..100ms uniformly

        // p99 index = ceil(0.99 * 100) - 1 = 98 (0-based) → the 99th smallest = 99ms.
        Assert.Equal(99, estimator.P99Ms);
    }

    [Fact]
    public void RingBuffer_ForgetsOldSamplesBeyondWindow()
    {
        FinalizeLatencyEstimator estimator = new(window: 8, recomputeEvery: 1);

        // Fill the window with a tall value, then overwrite every slot with a small one: the p99 must reflect only
        // the recent (small) samples, proving the old tall ones aged out of the ring.
        for (int i = 0; i < 8; i++)
            estimator.Record(1000);
        for (int i = 0; i < 8; i++)
            estimator.Record(5);

        Assert.Equal(5, estimator.P99Ms);
    }
}

using Kahuna.Server.KeyValues.Transactions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for <see cref="FinalizeLatencyEstimator"/>, the rolling p99s that size the durable-transaction
/// decision deadline. Verifies that the estimate is live from the first sample and recomputes on its cadence
/// afterward, that the p99 tracks the recent distribution, that the ring buffer keeps only the most recent window,
/// that an intent is priced from large finalizes once one is seen, and that the margin grows with the manifest a
/// finalize carries and clamps to the floor and the ceiling.
/// </summary>
public sealed class TestFinalizeLatencyEstimator
{
    [Fact]
    public void NoSamples_ReportsZero_AndTheMarginIsTheFloor()
    {
        FinalizeLatencyEstimator estimator = new(window: 512, recomputeEvery: 32);

        Assert.Equal(0, estimator.P99Ms);
        Assert.Equal(0, estimator.P99PerIntentMs);
        Assert.Equal(5_000, estimator.DeadlineMarginMs(intentCount: 30_000, multiplier: 4, floorMs: 5_000, ceilingMs: 60_000));
    }

    [Fact]
    public void Warmup_RecomputesOnEverySample()
    {
        FinalizeLatencyEstimator estimator = new(window: 512, recomputeEvery: 32);

        // Fewer samples than the recompute cadence: the estimate must still reflect them, so the first finalizes of
        // a node are sized from the latency it has already measured rather than from the floor alone.
        for (int i = 0; i < 10; i++)
            estimator.Record(100, intentCount: 1);

        Assert.Equal(100, estimator.P99Ms);
        Assert.Equal(100, estimator.P99PerIntentMs);
    }

    [Fact]
    public void AfterWarmup_RecomputesOnTheCadence()
    {
        FinalizeLatencyEstimator estimator = new(window: 512, recomputeEvery: 32);

        for (int i = 0; i < 32; i++)
            estimator.Record(100, intentCount: 1);

        // The 33rd sample is a tall one. Past warmup the cached p99 moves only when the cadence recomputes, which
        // is at the 64th sample.
        estimator.Record(5_000, intentCount: 1);
        Assert.Equal(100, estimator.P99Ms);

        for (int i = 0; i < 30; i++)
            estimator.Record(100, intentCount: 1);
        Assert.Equal(100, estimator.P99Ms);

        estimator.Record(100, intentCount: 1);
        Assert.Equal(5_000, estimator.P99Ms);
    }

    [Fact]
    public void P99_TracksRecentDistribution()
    {
        FinalizeLatencyEstimator estimator = new(window: 100, recomputeEvery: 1);

        // 98 samples at 10ms and two tall outliers at 500ms. p99 index = ceil(0.99*100)-1 = 98 (0-based), which
        // with a sorted [10×98, 500, 500] lands on the tall tail — a single 1% outlier would not move p99, but a
        // 2% tail does.
        for (int i = 0; i < 98; i++)
            estimator.Record(10, intentCount: 1);
        estimator.Record(500, intentCount: 1);
        estimator.Record(500, intentCount: 1);

        Assert.Equal(500, estimator.P99Ms);
    }

    [Fact]
    public void P99_RoundsUp_ForModerateSpread()
    {
        FinalizeLatencyEstimator estimator = new(window: 100, recomputeEvery: 1);

        for (int i = 1; i <= 100; i++)
            estimator.Record(i, intentCount: 1); // 1..100ms uniformly

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
            estimator.Record(1000, intentCount: 1);
        for (int i = 0; i < 8; i++)
            estimator.Record(5, intentCount: 1);

        Assert.Equal(5, estimator.P99Ms);
    }

    [Fact]
    public void PerIntent_IsPricedFromLargeFinalizesOnly_OnceOneIsSeen()
    {
        FinalizeLatencyEstimator estimator = new(window: 100, recomputeEvery: 1);

        // A one-intent finalize at 10 ms would price an intent at the whole fixed cost. A finalize of 6,000 intents
        // at 300 ms amortizes it to 0.05 ms per intent, and once it is in the window the small samples no longer
        // feed the per-intent estimate.
        for (int i = 0; i < 20; i++)
            estimator.Record(10, intentCount: 1);
        estimator.Record(300, intentCount: 6_000);

        Assert.Equal(0.05, estimator.P99PerIntentMs, precision: 9);
        Assert.Equal(300, estimator.P99Ms);
    }

    [Fact]
    public void PerIntent_IsPricedFromEverySample_UntilALargeFinalizeIsSeen()
    {
        FinalizeLatencyEstimator estimator = new(window: 100, recomputeEvery: 1);

        estimator.Record(10, intentCount: 1);
        estimator.Record(12, intentCount: 3);

        // 10 ms / 1 and 12 ms / 3 give {10, 4}: the p99 is 10, the fixed cost priced into one intent. That errs
        // toward a wider margin for a large manifest, which the ceiling bounds, until a large finalize is observed.
        Assert.Equal(10, estimator.P99PerIntentMs, precision: 9);
    }

    [Fact]
    public void Margin_GrowsWithTheManifest_AndClampsToFloorAndCeiling()
    {
        FinalizeLatencyEstimator estimator = new(window: 100, recomputeEvery: 1);

        // Five finalizes of 6,000 intents at 750 ms: total p99 750 ms, 0.125 ms per intent.
        for (int i = 0; i < 5; i++)
            estimator.Record(750, intentCount: 6_000);

        // A manifest like the ones seen: 4 × max(750, 0.125 × 6,000) = 3,000, under the floor.
        Assert.Equal(5_000, estimator.DeadlineMarginMs(6_000, multiplier: 4, floorMs: 5_000, ceilingMs: 60_000));

        // Five times larger: 4 × 0.125 × 30,000 = 15,000. The margin this manifest gets is sized by its own size,
        // not by the smaller finalizes before it.
        Assert.Equal(15_000, estimator.DeadlineMarginMs(30_000, multiplier: 4, floorMs: 5_000, ceilingMs: 60_000));

        // Fifty times larger: 4 × 0.125 × 300,000 = 150,000, capped by the ceiling.
        Assert.Equal(60_000, estimator.DeadlineMarginMs(300_000, multiplier: 4, floorMs: 5_000, ceilingMs: 60_000));
    }

    [Fact]
    public void Margin_UsesTheTotalP99_ForAManifestSmallerThanTheOnesSeen()
    {
        FinalizeLatencyEstimator estimator = new(window: 100, recomputeEvery: 1);

        for (int i = 0; i < 5; i++)
            estimator.Record(750, intentCount: 6_000);

        // Ten intents cost 1.25 ms by the per-intent estimate; the total p99 of 750 ms is the larger term.
        Assert.Equal(3_000, estimator.DeadlineMarginMs(10, multiplier: 4, floorMs: 1_000, ceilingMs: 60_000));
    }

    [Fact]
    public void Margin_ACeilingBelowTheFloor_ReadsAsTheFloor()
    {
        FinalizeLatencyEstimator estimator = new(window: 100, recomputeEvery: 1);

        estimator.Record(750, intentCount: 6_000);

        Assert.Equal(5_000, estimator.DeadlineMarginMs(300_000, multiplier: 4, floorMs: 5_000, ceilingMs: 300));
    }
}

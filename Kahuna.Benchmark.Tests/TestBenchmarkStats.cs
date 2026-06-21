/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using HdrHistogram;
using Kahuna.Benchmark;

namespace Kahuna.Benchmark.Tests;

/// <summary>
/// Unit coverage for the benchmark stats/aggregation layer (Phase C1/C2): percentile
/// computation, mean tracking, outcome classification, miss handling, cross-worker
/// merge, and the timeout-derived histogram ceiling. These tests feed known latencies
/// and assert the computed numbers — no network or running cluster is involved.
/// </summary>
public sealed class TestBenchmarkStats
{
    private const long Ceiling = 15_000_000L; // timeout 10s × 1.5, in µs

    // ── percentiles + mean from known latencies (C1/C2) ───────────────────────

    [Fact]
    public void Percentiles_And_Mean_FromKnownLatencies()
    {
        WorkerStats w = new(Ceiling);

        // 990 requests at 1 ms, 10 at 100 ms. Both values are exact at 3 sig figs.
        for (int i = 0; i < 990; i++)
            w.Record(OperationType.Get, OpOutcome.Success, 1_000);
        for (int i = 0; i < 10; i++)
            w.Record(OperationType.Get, OpOutcome.Success, 100_000);

        Assert.Equal(1000, w.GetSuccess(OperationType.Get));

        LongHistogram h = w.GetHistogram(OperationType.Get);

        // 99% of samples are 1 ms, so p50 and p99 land in the 1 ms band; only the
        // top 1% (p99.9) reaches 100 ms.
        Assert.InRange(h.GetValueAtPercentile(50), 990, 1_010);
        Assert.InRange(h.GetValueAtPercentile(99), 990, 1_010);
        Assert.InRange(h.GetValueAtPercentile(99.9), 99_000, 101_000);
        Assert.InRange(h.GetValueAtPercentile(100), 99_000, 101_000);

        // Mean uses the exact accumulated sum: (990×1000 + 10×100000) / 1000 = 1990 µs.
        Assert.Equal(1990.0, w.GetMeanMicros(OperationType.Get), 1.0);
    }

    [Fact]
    public void Median_IsExact_ForUniformLatency()
    {
        WorkerStats w = new(Ceiling);
        for (int i = 0; i < 5_000; i++)
            w.Record(OperationType.Set, OpOutcome.Success, 2_000); // every request 2 ms

        LongHistogram h = w.GetHistogram(OperationType.Set);
        Assert.InRange(h.GetValueAtPercentile(50), 1_990, 2_010);
        Assert.InRange(h.GetValueAtPercentile(99.9), 1_990, 2_010);
        Assert.Equal(2000.0, w.GetMeanMicros(OperationType.Set), 1.0);
    }

    // ── outcome classification (C3 plumbing) ──────────────────────────────────

    [Fact]
    public void Outcomes_AreCountedSeparately_AndOnlySuccessHitsHistogram()
    {
        WorkerStats w = new(Ceiling);

        for (int i = 0; i < 7; i++) w.Record(OperationType.Get, OpOutcome.Success, 1_000);
        for (int i = 0; i < 3; i++) w.Record(OperationType.Get, OpOutcome.Error, 1_000);
        for (int i = 0; i < 2; i++) w.Record(OperationType.Get, OpOutcome.Timeout, 1_000);
        for (int i = 0; i < 5; i++) w.Record(OperationType.Get, OpOutcome.Miss, 1_000);

        Assert.Equal(7, w.GetSuccess(OperationType.Get));
        Assert.Equal(3, w.GetErrors(OperationType.Get));
        Assert.Equal(2, w.GetTimeouts(OperationType.Get));
        Assert.Equal(5, w.GetMisses(OperationType.Get));

        // Only the 7 successes are recorded in the latency histogram; errors,
        // timeouts and misses are excluded so percentiles reflect hits only.
        Assert.Equal(7, w.GetHistogram(OperationType.Get).TotalCount);
    }

    [Fact]
    public void Mean_IsZero_WhenNoSuccess()
    {
        WorkerStats w = new(Ceiling);
        w.Record(OperationType.Get, OpOutcome.Miss, 5_000);
        Assert.Equal(0.0, w.GetMeanMicros(OperationType.Get));
    }

    // ── miss-only op stays visible (regression for the ActiveOps fix) ─────────

    [Fact]
    public void ActiveOps_IncludesAnOpWithOnlyMisses()
    {
        WorkerStats w = new(Ceiling);
        w.Record(OperationType.Get, OpOutcome.Miss, 1_000);

        Assert.Contains(OperationType.Get, w.ActiveOps);
    }

    [Fact]
    public void ActiveOps_ExcludesUntouchedOps()
    {
        WorkerStats w = new(Ceiling);
        w.Record(OperationType.Set, OpOutcome.Success, 1_000);

        Assert.Contains(OperationType.Set, w.ActiveOps);
        Assert.DoesNotContain(OperationType.Get, w.ActiveOps);
        Assert.DoesNotContain(OperationType.Lock, w.ActiveOps);
    }

    // ── cross-worker merge ────────────────────────────────────────────────────

    [Fact]
    public void MergeInto_SumsCountsHistogramsAndMean()
    {
        WorkerStats a = new(Ceiling);
        WorkerStats b = new(Ceiling);

        for (int i = 0; i < 100; i++) a.Record(OperationType.Get, OpOutcome.Success, 1_000);
        for (int i = 0; i < 100; i++) b.Record(OperationType.Get, OpOutcome.Success, 3_000);
        a.Record(OperationType.Get, OpOutcome.Error, 0);
        b.Record(OperationType.Get, OpOutcome.Miss, 0);

        WorkerStats agg = new(Ceiling);
        a.MergeInto(agg);
        b.MergeInto(agg);

        Assert.Equal(200, agg.GetSuccess(OperationType.Get));
        Assert.Equal(1, agg.GetErrors(OperationType.Get));
        Assert.Equal(1, agg.GetMisses(OperationType.Get));
        Assert.Equal(200, agg.GetHistogram(OperationType.Get).TotalCount);

        // Combined mean = (100×1000 + 100×3000) / 200 = 2000 µs.
        Assert.Equal(2000.0, agg.GetMeanMicros(OperationType.Get), 1.0);
    }

    // ── BenchmarkStats aggregate (totals + rps) ───────────────────────────────

    [Fact]
    public void BenchmarkStats_ComputesTotalsAndRps()
    {
        WorkerStats w = new(Ceiling);
        for (int i = 0; i < 600; i++) w.Record(OperationType.Get, OpOutcome.Success, 1_000);
        for (int i = 0; i < 400; i++) w.Record(OperationType.Set, OpOutcome.Success, 2_000);
        w.Record(OperationType.Get, OpOutcome.Error, 0);
        w.Record(OperationType.Get, OpOutcome.Miss, 0);

        BenchmarkOptions opts = new() { Timeout = 10 };
        BenchmarkStats stats = BenchmarkStats.Merge([w], elapsedSeconds: 2.0, opts);

        Assert.Equal(1000, stats.TotalSuccess);
        Assert.Equal(1, stats.TotalErrors);
        Assert.Equal(1, stats.TotalMisses);

        // 1000 successes over 2 seconds.
        Assert.Equal(500.0, stats.AchievedRps, 0.001);
    }

    [Fact]
    public void BenchmarkStats_AchievedRps_IsZero_WhenNoElapsed()
    {
        WorkerStats w = new(Ceiling);
        w.Record(OperationType.Get, OpOutcome.Success, 1_000);

        BenchmarkStats stats = BenchmarkStats.Merge([w], elapsedSeconds: 0.0, new BenchmarkOptions { Timeout = 10 });
        Assert.Equal(0.0, stats.AchievedRps);
    }

    // ── histogram ceiling derivation ──────────────────────────────────────────

    [Theory]
    [InlineData(10, 15_000_000L)]   // 10s × 1.5
    [InlineData(1, 1_500_000L)]     // 1s × 1.5
    [InlineData(0, 1_000_000L)]     // floored at 1s
    public void CeilingMicrosFromTimeout_DerivesFromTimeout(int timeoutSeconds, long expected)
    {
        Assert.Equal(expected, WorkerStats.CeilingMicrosFromTimeout(timeoutSeconds));
    }

    [Fact]
    public void Record_ClampsToCeiling_SoOversizeLatencyDoesNotThrow()
    {
        long ceiling = WorkerStats.CeilingMicrosFromTimeout(1); // 1.5s = 1_500_000 µs
        WorkerStats w = new(ceiling);

        // A latency above the ceiling must be clamped, not rejected by HdrHistogram.
        w.Record(OperationType.Get, OpOutcome.Success, ceiling + 5_000_000);

        Assert.Equal(1, w.GetSuccess(OperationType.Get));
        // GetValueAtPercentile returns the upper bound of the bucket holding the clamped
        // value, so it can sit a fraction of a percent (one 3-sig-fig bucket) above the
        // ceiling — the point is it is pinned at the ceiling, not the 6.5s raw value.
        Assert.InRange(w.GetHistogram(OperationType.Get).GetValueAtPercentile(100), ceiling - 2_000, ceiling + 2_000);
    }
}

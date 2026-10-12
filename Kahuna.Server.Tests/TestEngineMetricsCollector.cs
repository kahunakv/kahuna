using System.Diagnostics.Metrics;

using Kahuna.Server.Diagnostics;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the in-process meter listener behind the operator dashboard's engine panel.
///
/// <para>Every test here publishes its own instruments on a meter it owns, under a name no other
/// test uses. That is not tidiness — the real Kahuna and Kommander meters are <c>static</c> and
/// process-wide, so a collector built in one test observes every other test running beside it. An
/// assertion on a real instrument's value would depend on what the rest of the suite happened to be
/// doing, which is how a test becomes flaky. A unique instrument name makes exact assertions
/// sound.</para>
/// </summary>
public sealed class TestEngineMetricsCollector
{
    private static EngineMetricRow? Find(IReadOnlyList<EngineMetricRow> rows, string metric, string tags = "") =>
        rows.FirstOrDefault(r => r.Metric == metric && r.Tags == tags);

    [Fact]
    public void CounterReportsARunningTotal()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        using EngineMetricsCollector collector = new();

        Counter<long> counter = meter.CreateCounter<long>("test.collector.counter_total");

        counter.Add(3);
        counter.Add(4);

        EngineMetricRow? row = Find(collector.Snapshot(), "test.collector.counter_total");

        Assert.NotNull(row);
        Assert.Equal(EngineMetricKind.Counter, row.Kind);
        Assert.Equal("kahuna", row.Source);
        Assert.Equal(7, row.Total);

        // A counter defines no distribution, so these must arrive as absent rather than as zero —
        // the dashboard renders "no such reading" and "measured zero" differently.
        Assert.Null(row.Min);
        Assert.Null(row.Max);
        Assert.Null(row.Last);
    }

    [Fact]
    public void HistogramReportsCountSumMinMaxAndLast()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        using EngineMetricsCollector collector = new();

        Histogram<int> histogram = meter.CreateHistogram<int>("test.collector.histogram_ms");

        histogram.Record(5);
        histogram.Record(1);
        histogram.Record(9);

        EngineMetricRow? row = Find(collector.Snapshot(), "test.collector.histogram_ms");

        Assert.NotNull(row);
        Assert.Equal(EngineMetricKind.Histogram, row.Kind);
        Assert.Equal(3, row.Count);
        Assert.Equal(15, row.Total);
        Assert.Equal(1, row.Min);
        Assert.Equal(9, row.Max);
        Assert.Equal(9, row.Last);
    }

    /// <summary>
    /// Kahuna and Kommander between them publish <c>Counter&lt;long&gt;</c>, <c>Histogram&lt;int&gt;</c>,
    /// <c>Histogram&lt;long&gt;</c> and <c>Histogram&lt;double&gt;</c>. A collector that registered only
    /// the <c>double</c> callback would drop the integer instruments silently — the row would simply
    /// never appear, which reads as "this never fired" rather than as a fault.
    /// </summary>
    [Fact]
    public void EveryNumericInstrumentTypeIsObserved()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        using EngineMetricsCollector collector = new();

        meter.CreateHistogram<int>("test.collector.width_int").Record(2);
        meter.CreateHistogram<long>("test.collector.width_long").Record(3);
        meter.CreateHistogram<double>("test.collector.width_double").Record(4.5);
        meter.CreateCounter<long>("test.collector.width_counter").Add(6);

        IReadOnlyList<EngineMetricRow> rows = collector.Snapshot();

        Assert.Equal(2, Find(rows, "test.collector.width_int")?.Total);
        Assert.Equal(3, Find(rows, "test.collector.width_long")?.Total);
        Assert.Equal(4.5, Find(rows, "test.collector.width_double")?.Total);
        Assert.Equal(6, Find(rows, "test.collector.width_counter")?.Total);
    }

    /// <summary>
    /// An observable gauge holds no history. It is sampled when the snapshot is taken and reports
    /// only that instant, so a second snapshot must follow the value rather than accumulate it.
    /// </summary>
    [Fact]
    public void ObservableGaugeIsSampledAtSnapshotTime()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        using EngineMetricsCollector collector = new();

        int depth = 4;
        meter.CreateObservableGauge("test.collector.depth", () => depth);

        EngineMetricRow? first = Find(collector.Snapshot(), "test.collector.depth");

        Assert.NotNull(first);
        Assert.Equal(EngineMetricKind.Gauge, first.Kind);
        Assert.Equal(4, first.Last);
        Assert.Null(first.Total);

        depth = 9;

        Assert.Equal(9, Find(collector.Snapshot(), "test.collector.depth")?.Last);
    }

    /// <summary>
    /// The recording path is striped per processor and folded at snapshot time without a lock. Many
    /// threads recording the same histogram at once must lose no sample: the count, the sum and the
    /// extremes must equal what a single-threaded fold of the same values gives. Integer values keep
    /// the double sum exact, so the assertion is on equality and not on tolerance.
    /// </summary>
    [Fact]
    public void ConcurrentRecordsLoseNoSample()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        using EngineMetricsCollector collector = new();

        Histogram<long> histogram = meter.CreateHistogram<long>("test.collector.concurrent_ms");
        Counter<long> counter = meter.CreateCounter<long>("test.collector.concurrent_total");

        const int threads = 8;
        const int perThread = 20_000;

        Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, t =>
        {
            for (int i = 1; i <= perThread; i++)
            {
                histogram.Record(t * perThread + i);
                counter.Add(1);
            }
        });

        // The very last sample on one thread after the burst is the most recent value.
        histogram.Record(3);

        IReadOnlyList<EngineMetricRow> rows = collector.Snapshot();

        EngineMetricRow? row = Find(rows, "test.collector.concurrent_ms");
        Assert.NotNull(row);

        const long samples = threads * perThread;
        Assert.Equal(samples + 1, row.Count);
        Assert.Equal((double)samples * (samples + 1) / 2 + 3, row.Total);
        Assert.Equal(1, row.Min);
        Assert.Equal(samples, row.Max);
        Assert.Equal(3, row.Last);

        Assert.Equal(samples, Find(rows, "test.collector.concurrent_total")?.Total);
    }

    /// <summary>
    /// A snapshot taken while samples arrive must never block a recorder nor report a count outside
    /// the range of what was recorded so far, and the final snapshot must be exact.
    /// </summary>
    [Fact]
    public void SnapshotWhileRecordingIsBoundedAndFinalIsExact()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        using EngineMetricsCollector collector = new();

        Histogram<int> histogram = meter.CreateHistogram<int>("test.collector.live_ms");

        const int threads = 4;
        const int perThread = 50_000;
        const long samples = threads * perThread;

        using CancellationTokenSource stop = new();
        long snapshots = 0;

        Task reader = Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                EngineMetricRow? row = Find(collector.Snapshot(), "test.collector.live_ms");

                if (row is not null)
                {
                    Assert.InRange(row.Count, 1, samples);
                    Assert.Equal(1, row.Min);
                    Assert.Equal(1, row.Max);
                }

                snapshots++;
            }
        });

        Parallel.For(0, threads, new ParallelOptions { MaxDegreeOfParallelism = threads }, _ =>
        {
            for (int i = 0; i < perThread; i++)
                histogram.Record(1);
        });

        stop.Cancel();
        reader.GetAwaiter().GetResult();

        Assert.True(snapshots > 0);

        EngineMetricRow? final = Find(collector.Snapshot(), "test.collector.live_ms");
        Assert.NotNull(final);
        Assert.Equal(samples, final.Count);
        Assert.Equal(samples, final.Total);
    }

    /// <summary>
    /// Two call sites that emit the same tags in a different order describe the same thing and must
    /// land on one row. The canonical rendering sorts keys ordinally, which is what makes that true.
    /// </summary>
    [Fact]
    public void TagOrderDoesNotSplitARow()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        using EngineMetricsCollector collector = new();

        Counter<long> counter = meter.CreateCounter<long>("test.collector.tagged_total");

        counter.Add(1,
            new KeyValuePair<string, object?>("partition_id", 7),
            new KeyValuePair<string, object?>("outcome", "success"));

        counter.Add(1,
            new KeyValuePair<string, object?>("outcome", "success"),
            new KeyValuePair<string, object?>("partition_id", 7));

        IReadOnlyList<EngineMetricRow> rows = collector.Snapshot();

        Assert.Single(rows, r => r.Metric == "test.collector.tagged_total");
        Assert.Equal(2, Find(rows, "test.collector.tagged_total", "outcome=success,partition_id=7")?.Total);
    }

    /// <summary>
    /// A repeated tag-set is resolved by the identity of its strings, so two call sites that spell the
    /// same tags through different string instances must still land on one row through the canonical
    /// rendering, and a tag-set that differs in one value must not be captured by the cache.
    /// </summary>
    [Fact]
    public void SameTagsThroughDifferentStringInstancesLandOnOneRow()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        using EngineMetricsCollector collector = new();

        Histogram<int> histogram = meter.CreateHistogram<int>("test.collector.identity_ms");

        string key = "stage";
        string hit = "hit";
        string otherKey = new([.. "stage"]);
        string otherHit = new([.. "hit"]);
        Assert.False(ReferenceEquals(key, otherKey));
        Assert.False(ReferenceEquals(hit, otherHit));

        for (int i = 0; i < 3; i++)
        {
            histogram.Record(1, new KeyValuePair<string, object?>(key, hit));
            histogram.Record(2, new KeyValuePair<string, object?>(otherKey, otherHit));
            histogram.Record(5, new KeyValuePair<string, object?>(key, "miss"));
        }

        IReadOnlyList<EngineMetricRow> rows = collector.Snapshot();

        EngineMetricRow? hits = Find(rows, "test.collector.identity_ms", "stage=hit");
        Assert.NotNull(hits);
        Assert.Equal(6, hits.Count);
        Assert.Equal(9, hits.Total);

        EngineMetricRow? misses = Find(rows, "test.collector.identity_ms", "stage=miss");
        Assert.NotNull(misses);
        Assert.Equal(3, misses.Count);
        Assert.Equal(15, misses.Total);
    }

    /// <summary>Different tag-sets are different rows, so the dashboard can fold them itself.</summary>
    [Fact]
    public void DifferentTagSetsAreSeparateRows()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        using EngineMetricsCollector collector = new();

        Counter<long> counter = meter.CreateCounter<long>("test.collector.split_total");

        counter.Add(2, new KeyValuePair<string, object?>("partition_id", 1));
        counter.Add(5, new KeyValuePair<string, object?>("partition_id", 2));

        IReadOnlyList<EngineMetricRow> rows = collector.Snapshot();

        Assert.Equal(2, Find(rows, "test.collector.split_total", "partition_id=1")?.Total);
        Assert.Equal(5, Find(rows, "test.collector.split_total", "partition_id=2")?.Total);
    }

    /// <summary>A meter that is neither Kahuna's nor Kommander's is not this collector's business.</summary>
    [Fact]
    public void UnrelatedMetersAreIgnored()
    {
        using Meter meter = new("SomeOtherLibrary", "1.0");
        using EngineMetricsCollector collector = new();

        meter.CreateCounter<long>("test.collector.foreign_total").Add(11);

        Assert.Null(Find(collector.Snapshot(), "test.collector.foreign_total"));
    }

    /// <summary>
    /// Disposal must stop the listener. The suite builds and tears down many nodes, and a leaked
    /// listener keeps observing every one of them for the lifetime of the process.
    /// </summary>
    [Fact]
    public void DisposeStopsObserving()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");
        EngineMetricsCollector collector = new();

        Counter<long> counter = meter.CreateCounter<long>("test.collector.after_dispose_total");

        counter.Add(1);
        collector.Dispose();
        counter.Add(100);

        Assert.Equal(1, Find(collector.Snapshot(), "test.collector.after_dispose_total")?.Total);
    }

    /// <summary>
    /// Instruments published before the collector starts are replayed by the listener, so
    /// construction order relative to the node does not matter. Without this the dashboard would
    /// show nothing for every instrument created during startup.
    /// </summary>
    [Fact]
    public void InstrumentsPublishedBeforeTheCollectorAreStillObserved()
    {
        using Meter meter = new(EngineMetricsCollector.KahunaMeterName, "1.0");

        Counter<long> counter = meter.CreateCounter<long>("test.collector.pre_existing_total");

        using EngineMetricsCollector collector = new();

        counter.Add(8);

        Assert.Equal(8, Find(collector.Snapshot(), "test.collector.pre_existing_total")?.Total);
    }
}

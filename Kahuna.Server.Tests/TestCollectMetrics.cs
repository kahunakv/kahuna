
using System.Diagnostics.Metrics;
using System.Text;
using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies that a collect cycle publishes its statistics on the <c>"Kahuna"</c> meter, so an operator
/// can see eviction pressure and cycle duration without reading the log — the log line only fires when
/// something was actually evicted, which hides exactly the case that matters (a long cycle that
/// reclaimed nothing).
///
/// <para>Measurements are captured with a real <see cref="MeterListener"/>, the same mechanism a
/// consumer uses, rather than by inspecting the instruments directly.</para>
/// </summary>
public sealed class TestCollectMetrics : RaftTrackingTest
{
    /// <summary>
    /// Captures measurements on the "Kahuna" meter for the duration of a scope.
    ///
    /// <para>Only measurements recorded on the constructing thread are kept. The meter is process-wide
    /// and this assembly runs test collections in parallel, so a live actor elsewhere doing its own
    /// collect cycle would otherwise be counted here and make the exact-count assertions flaky. A
    /// measurement callback runs synchronously on whichever thread recorded it, and these tests drive
    /// <c>Execute()</c> directly, so thread identity separates this cycle from every other one.</para>
    /// </summary>
    private sealed class MetricCapture : IDisposable
    {
        private readonly MeterListener listener;
        private readonly List<(string Name, double Value, string Tags)> measurements = [];
        private readonly Lock gate = new();
        private readonly int ownerThreadId = Environment.CurrentManagedThreadId;

        public MetricCapture()
        {
            listener = new MeterListener
            {
                InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Meter.Name == "Kahuna")
                        l.EnableMeasurementEvents(instrument);
                },
            };

            listener.SetMeasurementEventCallback<long>((i, m, t, _) => Add(i, m, t));
            listener.SetMeasurementEventCallback<int>((i, m, t, _) => Add(i, m, t));
            listener.SetMeasurementEventCallback<double>((i, m, t, _) => Add(i, m, t));
            listener.Start();
        }

        private void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            if (Environment.CurrentManagedThreadId != ownerThreadId)
                return;

            string rendered = string.Join(",", tags.ToArray().Select(t => $"{t.Key}={t.Value}"));

            lock (gate)
                measurements.Add((instrument.Name, value, rendered));
        }

        public double Sum(string name, string tags = "")
        {
            lock (gate)
                return measurements.Where(m => m.Name == name && m.Tags == tags).Sum(m => m.Value);
        }

        public int Count(string name)
        {
            lock (gate)
                return measurements.Count(m => m.Name == name);
        }

        public void Dispose() => listener.Dispose();
    }

    /// <summary>
    /// A cycle that evicts under budget pressure must report the LRU eviction class, the entries it
    /// walked, and a duration — the three numbers needed to tell "collection is reclaiming efficiently"
    /// from "collection is burning its budget on pinned entries".
    /// </summary>
    [Fact]
    public void CollectCycle_PublishesEvictionsAndDuration()
    {
        using MetricCapture capture = new();

        KahunaConfiguration config = CreateConfiguration(maxEntries: 100, batchMax: 50);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        for (int i = 0; i < 200; i++)
            InsertClean(context, $"e/{i:D4}", now);

        handler.Execute();

        CollectCycleStats stats = handler.LastCycleStats;

        Assert.Equal(1, capture.Sum("kahuna.collect.cycles"));
        Assert.Equal(1, capture.Count("kahuna.collect.cycle.duration"));

        // The published numbers must be the same ones the handler recorded — a metric that drifts from
        // the stats struct would be worse than no metric at all.
        Assert.Equal(stats.LruEvicted, capture.Sum("kahuna.collect.evicted", "reason=lru"));
        Assert.Equal(stats.LruVisited, capture.Sum("kahuna.collect.inspected", "scan=lru"));
        Assert.True(stats.LruEvicted > 0, "the fixture must actually evict for this test to mean anything");
    }

    /// <summary>
    /// A cycle cut short by its budget self-schedules a follow-up; that carry-over is the signal that
    /// collection is not keeping up, and it must be visible as a counter rather than only as a log line.
    /// </summary>
    [Fact]
    public void BackloggedCycle_IsCounted()
    {
        using MetricCapture capture = new();

        KahunaConfiguration config = CreateConfiguration(maxEntries: 100, batchMax: 50);
        (TryCollectHandler handler, KeyValueContext context, RaftManager raft) = CreateHandler(config);
        HLCTimestamp now = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        // 200 entries against a 100 budget with a 50-eviction cap leaves the store over budget, so the
        // walk stops with a live cursor and the cycle reports a backlog.
        for (int i = 0; i < 200; i++)
            InsertClean(context, $"e/{i:D4}", now);

        handler.Execute();

        Assert.True(handler.LastCycleStats.Backlog, "the fixture must produce a backlogged cycle");
        Assert.Equal(1, capture.Sum("kahuna.collect.backlogged"));
    }

    /// <summary>
    /// An idle store runs collect cycles constantly. Those cycles must still report that they ran and
    /// how long they took — that is precisely the case the eviction log line omits — but must not emit
    /// zero-valued eviction counters, which would be pure measurement overhead on the mailbox thread.
    /// </summary>
    [Fact]
    public void IdleCycle_ReportsDurationButNoZeroValuedCounters()
    {
        using MetricCapture capture = new();

        (TryCollectHandler handler, KeyValueContext _, RaftManager _) = CreateHandler();

        handler.Execute();

        Assert.Equal(1, capture.Sum("kahuna.collect.cycles"));
        Assert.Equal(1, capture.Count("kahuna.collect.cycle.duration"));
        Assert.Equal(0, capture.Count("kahuna.collect.evicted"));
        Assert.Equal(0, capture.Count("kahuna.collect.inspected"));
        Assert.Equal(0, capture.Count("kahuna.collect.backlogged"));
    }

    // ── helpers (mirrors TestKeyValueEvictionSweep) ──────────────────────────────────

    private (TryCollectHandler, KeyValueContext, RaftManager) CreateHandler(KahunaConfiguration? config = null)
    {
        config ??= CreateConfiguration();

        BTree<string, KeyValueEntry> store = new(32);
        ILogger<IKahuna> logger = NullLogger<IKahuna>.Instance;
        ILogger<IRaft> raftLogger = NullLogger<IRaft>.Instance;

        RaftManager raft = new(
            new RaftConfiguration
            {
                NodeName = "collect-metrics-test",
                NodeId = 1,
                Host = "localhost",
                Port = 0,
                InitialPartitions = 1,
                EnableQuiescence = false,
                PartitionExecutorPoolSize = 1
            },
            new StaticDiscovery([]),
            new InMemoryWAL(raftLogger),
            new InMemoryCommunication(),
            new HybridLogicalClock(),
            raftLogger
        );

        KeyValueContext context = new(
            null!,
            store,
            new Dictionary<string, KeyValueWriteIntent>(),
            new Dictionary<string, List<KeyValueRangeLock>>(),
            new Dictionary<int, KeyValueProposal>(),
            null!,
            null!,
            null!,
            raft,
            raft.ReadScheduler,
            new KeySpaceRegistry(),
            new RangeMapStore(Track(raft), null, null, logger),
            config,
            logger
        );

        return (new TryCollectHandler(context), context, raft);
    }

    private static KahunaConfiguration CreateConfiguration(int maxEntries = 50_000, int batchMax = 1000)
    {
        KahunaConfiguration cfg = ConfigurationValidator.Validate(new()
        {
            LocksWorkers = 1,
            KeyValueWorkers = 1,
            BackgroundWriterWorkers = 1,
            Storage = "memory",
            CacheEntryTtl = TimeSpan.FromMinutes(5),
            CacheEntriesToRemove = 1000,
            MaxEntriesPerActor = maxEntries,
            MaxBytesPerActor = 256L * 1024 * 1024,
            CollectBatchMax = batchMax,
            RevisionRetention = 16
        });
        cfg.DirtyObjectsWriterDelay = 1_000;
        return cfg;
    }

    private static void InsertClean(KeyValueContext context, string key, HLCTimestamp lastUsed)
    {
        context.InsertStoreEntry(key, new KeyValueEntry
        {
            Value = Encoding.UTF8.GetBytes("v"),
            State = KeyValueState.Set,
            LastUsed = lastUsed,
            LastModified = HLCTimestamp.Zero,
            Revision = 0,
            FlushedRevision = 0
        });
    }
}

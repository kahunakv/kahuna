using System.Diagnostics.Metrics;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The partition write aggregator's per-submission queue delay is attributable to the stage that produced each
/// submission: a one-phase bundle, a 2PC prepare (the anchor init+prepare bundle included), a decision, a
/// materialization and a settle each land on their own <c>stage</c> series, carried from the creation site —
/// never inferred from the entry's log type. The workload drives both commit paths on one embedded node:
/// single-partition transactions take the one-phase bundle; two-partition fan-outs take the full 2PC stage
/// sequence.
/// </summary>
public sealed class TestSubmissionStageAttribution
{
    private readonly ILoggerFactory loggerFactory;

    public TestSubmissionStageAttribution(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>Samples of the queue-delay instrument keyed by the <c>stage</c> tag value.</summary>
    private sealed class StageCapture : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly object gate = new();
        private readonly Dictionary<string, int> counts = [];

        public StageCapture()
        {
            listener.InstrumentPublished = (inst, l) =>
            {
                if (inst.Meter.Name == "Kahuna" && inst.Name == "kahuna.kv.write.submission_queue_delay")
                    l.EnableMeasurementEvents(inst);
            };
            listener.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string stage = "";
                foreach (KeyValuePair<string, object?> tag in tags)
                {
                    if (tag.Key == "stage")
                        stage = tag.Value?.ToString() ?? "";
                }

                lock (gate)
                    counts[stage] = counts.GetValueOrDefault(stage) + 1;
            });
            listener.Start();
        }

        public IReadOnlyDictionary<string, int> Snapshot()
        {
            lock (gate)
                return new Dictionary<string, int>(counts);
        }

        public void Dispose() => listener.Dispose();
    }

    private static readonly HashSet<string> AllowedStages =
        ["one_phase", "record_init", "prepare", "re_prepare", "decision", "materialize", "settle", "other"];

    [Fact]
    public async Task BothCommitPaths_AttributeQueueDelayPerStage()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 2,
            KeyValueWriteLingerMs = 1
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("sa0/k0", ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;

        // Two key spaces on different partitions, so a two-space transaction is a genuine 2PC fan-out
        // (prepare + decision + materialize + settle) while a one-space transaction stays one-phase.
        string spaceA = "sa0";
        int partitionA = manager.KeyValues.LocateDurablePartition($"{spaceA}/k0").PartitionId;
        string spaceB = spaceA;
        for (int i = 1; i < 4_096; i++)
        {
            string candidate = $"sb{i}";
            if (manager.KeyValues.LocateDurablePartition($"{candidate}/k0").PartitionId != partitionA)
            {
                spaceB = candidate;
                break;
            }
        }
        Assert.NotEqual(spaceA, spaceB);

        using StageCapture capture = new();

        const int waves = 6;
        int committed = 0;
        for (int w = 0; w < waves; w++)
        {
            Task<KeyValueTransactionResult>[] tasks = new Task<KeyValueTransactionResult>[16];
            for (int t = 0; t < tasks.Length; t++)
            {
                // Even slots: single-partition (one-phase bundle). Odd slots: two-partition 2PC fan-out.
                string script = t % 2 == 0
                    ? $"BEGIN SET `{spaceA}/one{t}` 'v' COMMIT END"
                    : $"BEGIN SET `{spaceA}/two{t}` 'v' SET `{spaceB}/two{t}` 'v' COMMIT END";
                tasks[t] = node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
            }

            KeyValueTransactionResult[] results = await Task.WhenAll(tasks);
            committed += results.Count(r => r.Type == KeyValueResponseType.Set);
        }

        // Let the deferred resolution of the last wave (materializations, settles) reach the scheduler.
        await Task.Delay(800, ct);

        IReadOnlyDictionary<string, int> stages = capture.Snapshot();

        Assert.True(committed > 0);

        // Every dispatched submission carries a stage from the bounded set — nothing untagged, nothing invented.
        Assert.All(stages.Keys, stage => Assert.Contains(stage, AllowedStages));

        // The one-phase path and every 2PC stage of the fan-out landed on their own series.
        Assert.True(stages.GetValueOrDefault("one_phase") > 0, "no one-phase bundle submissions were observed");
        Assert.True(stages.GetValueOrDefault("prepare") > 0, "no prepare submissions were observed");
        Assert.True(stages.GetValueOrDefault("decision") > 0, "no decision submissions were observed");
        Assert.True(stages.GetValueOrDefault("materialize") > 0, "no materialization submissions were observed");
        Assert.True(stages.GetValueOrDefault("settle") > 0, "no settle submissions were observed");
    }
}

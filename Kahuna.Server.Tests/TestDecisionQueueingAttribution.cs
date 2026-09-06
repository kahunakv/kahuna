using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Measures where each kind of durable submission waits in the shared partition write scheduler under a
/// contended two-partition fan-out: record inits and prepares (ordinary), decisions (terminal records),
/// materializations (terminal key/value) and settles (terminal intents) each get their own queue-delay series
/// from the per-submission histogram's class and type tags. The numbers answer whether decisions wait behind
/// background terminal work; the assertions only require that every kind was observed, so the measurement
/// itself is what the test output carries.
/// </summary>
public sealed class TestDecisionQueueingAttribution
{
    private readonly ITestOutputHelper output;

    private readonly ILoggerFactory loggerFactory;

    public TestDecisionQueueingAttribution(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>Samples of one instrument keyed by two tags, so a kind of work (class, type) can be read apart.</summary>
    private sealed class KindCapture : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly object gate = new();
        private readonly Dictionary<(string Class, string Type), List<double>> samples = [];

        public KindCapture(string instrument)
        {
            listener.InstrumentPublished = (inst, l) =>
            {
                if (inst.Meter.Name == "Kahuna" && inst.Name == instrument)
                    l.EnableMeasurementEvents(inst);
            };
            listener.SetMeasurementEventCallback<long>((_, value, tags, _) => Record(tags, value));
            listener.Start();
        }

        private void Record(ReadOnlySpan<KeyValuePair<string, object?>> tags, double value)
        {
            string cls = "", type = "";
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                if (tag.Key == "class") cls = tag.Value?.ToString() ?? "";
                else if (tag.Key == "type") type = tag.Value?.ToString() ?? "";
            }

            lock (gate)
            {
                if (!samples.TryGetValue((cls, type), out List<double>? list))
                    samples[(cls, type)] = list = [];
                list.Add(value);
            }
        }

        public IReadOnlyDictionary<(string Class, string Type), List<double>> Snapshot()
        {
            lock (gate)
                return samples.ToDictionary(p => p.Key, p => new List<double>(p.Value));
        }

        public void Dispose() => listener.Dispose();
    }

    private static double Percentile(List<double> sorted, double p) =>
        sorted.Count == 0 ? 0 : sorted[Math.Min(sorted.Count - 1, (int)Math.Ceiling(p * sorted.Count) - 1 < 0 ? 0 : (int)Math.Ceiling(p * sorted.Count) - 1)];

    private static string F(double v) => v.ToString("F2", CultureInfo.InvariantCulture);

    [Fact]
    public async Task ContendedFanOut_AttributesQueueDelayPerWorkKind()
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
        await node.WaitForLeaderForKeyAsync("qa0/k0", ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;

        // Two key spaces on different partitions, so every transaction is a genuine two-participant fan-out
        // with a separate decision barrier (the one-phase bundle needs a single partition).
        string spaceA = "qa0";
        int partitionA = manager.KeyValues.LocateDurablePartition($"{spaceA}/k0").PartitionId;
        string spaceB = spaceA;
        for (int i = 1; i < 4_096; i++)
        {
            string candidate = $"qb{i}";
            if (manager.KeyValues.LocateDurablePartition($"{candidate}/k0").PartitionId != partitionA)
            {
                spaceB = candidate;
                break;
            }
        }
        Assert.NotEqual(spaceA, spaceB);

        const int hotKeys = 4;
        const int workers = 32;
        const int waves = 12;
        Random random = new(7);

        async Task<int> RunWave()
        {
            Task<KeyValueTransactionResult>[] tasks = new Task<KeyValueTransactionResult>[workers];
            for (int w = 0; w < workers; w++)
            {
                int a = random.Next(hotKeys), b = random.Next(hotKeys);
                tasks[w] = node.Kahuna.TryExecuteTransactionScript(
                    Encoding.UTF8.GetBytes($"BEGIN SET `{spaceA}/k{a}` 'v' SET `{spaceB}/k{b}` 'v' COMMIT END"), null, null);
            }

            KeyValueTransactionResult[] results = await Task.WhenAll(tasks);
            return results.Count(r => r.Type == KeyValueResponseType.Set);
        }

        for (int w = 0; w < 3; w++)
            await RunWave();
        await Task.Delay(300, ct);

        using KindCapture capture = new("kahuna.kv.write.submission_queue_delay");

        int committed = 0;
        for (int w = 0; w < waves; w++)
            committed += await RunWave();

        // Let the deferred resolution of the last wave (materializations, settles) reach the scheduler.
        await Task.Delay(800, ct);

        IReadOnlyDictionary<(string Class, string Type), List<double>> kinds = capture.Snapshot();

        output.WriteLine($"contended two-partition fan-out: {workers} workers x {waves} waves, {hotKeys} hot keys per space, {committed} committed");
        output.WriteLine($"{"kind",-22}{"count",-8}{"mean ms",-10}{"p50 ms",-10}{"p99 ms",-10}{"max ms",-10}");
        foreach (((string cls, string type), List<double> list) in kinds.OrderBy(k => k.Key.Class).ThenBy(k => k.Key.Type))
        {
            List<double> sorted = [.. list.Order()];
            string kind = (cls, type) switch
            {
                ("ordinary", "record") => "init (ord/record)",
                ("ordinary", "intent") => "prepare (ord/intent)",
                ("terminal", "record") => "decision (term/rec)",
                ("terminal", "kv") => "materialize (term/kv)",
                ("terminal", "intent") => "settle (term/intent)",
                _ => $"{cls}/{type}"
            };
            output.WriteLine($"{kind,-22}{sorted.Count,-8}{F(sorted.Average()),-10}{F(Percentile(sorted, 0.5)),-10}{F(Percentile(sorted, 0.99)),-10}{F(sorted[^1]),-10}");
        }

        Assert.True(committed > 0);
        Assert.True(kinds.ContainsKey(("terminal", "record")), "no decision submissions were observed");
        Assert.True(kinds.ContainsKey(("ordinary", "intent")), "no prepare submissions were observed");
        Assert.True(kinds.ContainsKey(("terminal", "kv")), "no materialization submissions were observed");
    }
}

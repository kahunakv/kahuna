using System.Diagnostics.Metrics;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Operational-gate coverage: under deferred settlement a durable transaction's prepared intents linger until the
/// background resolution drains them, so a sustained commit burst must not let resident prepared state grow without
/// bound. The prepared-intent admission gate (<c>DurablePreparedIntentMaxCount</c>) caps resident intents — a burst
/// that outruns settlement is backpressured with the retryable <c>MustRetry</c> rather than admitted — and once the
/// inflow relents the backlog drains to zero. No acknowledged commit is lost to the backpressure.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDeferredSettlementBacklogBound
{
    private readonly ILoggerFactory loggerFactory;

    public TestDeferredSettlementBacklogBound(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    [Fact]
    public async Task SustainedOverload_AdmissionEngages_BacklogDrains_NoCommitLost()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        const int cap = 8;
        const int workers = 16;
        const int perWorker = 8;
        const int total = workers * perWorker;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            DurableDeferredSettlement = true,
            // A low resident prepared-intent cap so sustained inflow that outruns background settlement trips the
            // bound. Bounded concurrency (workers) drives a rolling backlog rather than one instantaneous burst,
            // which is the regime the resident-count admission check (a soft, non-reserving read) actually governs.
            DurablePreparedIntentMaxCount = cap
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("blog/0", ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        long admissionRejections = 0;
        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Kahuna" && instrument.Name == "kahuna.durable_tx.admission_rejections")
                l.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, m, _, _) => Interlocked.Add(ref admissionRejections, m));
        listener.Start();

        // Sample resident prepared-intent count while the burst runs, tracking the peak.
        int peakResident = 0;
        using CancellationTokenSource samplerCts = new();
        Task sampler = Task.Run(async () =>
        {
            while (!samplerCts.IsCancellationRequested)
            {
                int now = kahuna.DurablePreparedIntentStore.Count;
                int observed = Volatile.Read(ref peakResident);
                while (now > observed)
                {
                    int prior = Interlocked.CompareExchange(ref peakResident, now, observed);
                    if (prior == observed) break;
                    observed = prior;
                }
                await Task.Delay(1);
            }
        }, ct);

        // Each self-contained commit retries the retryable admission MustRetry so the transaction eventually lands
        // despite the backpressure (no commit is lost).
        async Task<bool> CommitOne(int i)
        {
            string script = $"BEGIN SET `blog/{i}` 'v{i}' COMMIT END";
            for (int attempt = 0; attempt < 400; attempt++)
            {
                KeyValueTransactionResult r = await node.Kahuna.TryExecuteTransactionScript(Encoding.UTF8.GetBytes(script), null, null);
                if (r.Type == KeyValueResponseType.Set) return true;
                if (r.Type != KeyValueResponseType.MustRetry) return false;
                await Task.Delay(Math.Min(2 * attempt, 25), ct);
            }
            return false;
        }

        // Bounded-concurrency sustained inflow: each worker commits its slice sequentially, so at any instant at most
        // `workers` transactions contend for admission — a rolling backlog rather than one instantaneous burst.
        async Task<bool> RunWorker(int w)
        {
            bool all = true;
            for (int j = 0; j < perWorker; j++)
                all &= await CommitOne(w * perWorker + j);
            return all;
        }

        bool[] workerOk = await Task.WhenAll(Enumerable.Range(0, workers).Select(RunWorker));

        samplerCts.Cancel();
        try { await sampler; } catch (OperationCanceledException) { }
        listener.Dispose();

        // Every transaction eventually committed — backpressure never dropped an acknowledged commit.
        Assert.All(workerOk, Assert.True);

        // Resident prepared state never accumulated the entire workload at once — the cap kept the rolling backlog
        // far below the total inflow (a soft cap overshoots under concurrency, so allow headroom, but it must be
        // well below `total`).
        Assert.True(Volatile.Read(ref peakResident) < total,
            $"resident prepared intents peaked at {peakResident}, expected bounded below {total}");

        // The admission gate engaged under the sustained overload (inflow outran settlement at least once).
        Assert.True(Interlocked.Read(ref admissionRejections) > 0,
            "expected the prepared-intent admission gate to refuse at least one transaction under sustained overload");

        // Once the inflow relents, the backlog drains to zero — no monotonic growth.
        await WaitUntil(() => kahuna.DurablePreparedIntentStore.Count == 0);

        // Every committed value is durably present after the backlog drains.
        for (int i = 0; i < total; i++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                Kommander.Time.HLCTimestamp.Zero, $"blog/{i}", -1, Kommander.Time.HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal(Encoding.UTF8.GetBytes($"v{i}"), entry!.Value);
        }
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 10000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(10);
        }
        Assert.True(predicate(), "condition not met in time");
    }
}

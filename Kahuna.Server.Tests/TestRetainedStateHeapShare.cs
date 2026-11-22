using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text;

using Kahuna;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The warning that ties the retained transaction state to the managed heap limit. Transaction records,
/// completion receipts and settled identities grow with the write commit rate, and nothing in their retention
/// reads the heap limit: a node whose rate is too high for its heap learned it by running out of memory. The
/// maintenance tick now compares the estimated bytes of the three with a share of the heap limit, warns when
/// the share is passed, and reports the share on a gauge.
/// </summary>
public sealed class TestRetainedStateHeapShare
{
    private const int TransactionCount = 40;

    /// <summary>Keeps every line at Information or above that the node logs.</summary>
    private sealed class CapturingLoggerFactory : ILoggerFactory
    {
        public readonly ConcurrentQueue<(LogLevel Level, string Line)> Lines = new();

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => new CapturingLogger(this);

        public void Dispose()
        {
        }

        public List<string> Matching(LogLevel level, string fragment) =>
            [.. Lines.Where(l => l.Level == level && l.Line.Contains(fragment, StringComparison.Ordinal)).Select(l => l.Line)];

        private sealed class CapturingLogger(CapturingLoggerFactory owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (IsEnabled(logLevel))
                    owner.Lines.Enqueue((logLevel, formatter(state, exception)));
            }
        }
    }

    private static async Task<EmbeddedKahunaNode> StartNodeAsync(CapturingLoggerFactory logs, Action<EmbeddedKahunaOptions> configure, CancellationToken ct)
    {
        EmbeddedKahunaOptions options = new()
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            // The tick is driven by hand, and nothing is reclaimed during the test.
            DurableMaintenanceInterval = TimeSpan.FromMinutes(1),
            TransactionOutcomeRetentionTtl = TimeSpan.FromMinutes(30),
            CompletionReceiptRetentionTtl = TimeSpan.FromMinutes(60),
            DurableRecordRetentionMax = 0,
            DurableRecordRetentionMaxBytes = 0,
            DurableRecordRetentionHeapPressure = 0
        };
        configure(options);

        EmbeddedKahunaNode node = new(options, logs);
        await node.StartAsync(ct);
        return node;
    }

    private static async Task CommitTransactionsAsync(EmbeddedKahunaNode node, int count, string prefix)
    {
        for (int i = 0; i < count; i++)
        {
            string key = $"{prefix}/row-{i:D3}";
            await node.WaitForLeaderForKeyAsync(key, CancellationToken.None);

            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"BEGIN SET `{key}` 'v{i}' COMMIT END"), null, null);
            Assert.Equal(KeyValueResponseType.Set, result.Type);
        }
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 10_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate())
                return;
            await Task.Delay(20);
        }

        Assert.Fail("the condition was not met in time");
    }

    private static long RetainedBytes(KahunaManager kahuna) =>
        kahuna.DurableTransactionRecordStore.EstimatedBytes + kahuna.CompletionReceiptStore.EstimatedBytes + kahuna.KeyValues.DurablePreparedIntentStore.CommittedHeadLedgerBytes;

    [Fact]
    public async Task TheNodeWarns_WhenTheRetainedStatePassesItsShareOfTheHeapLimit_AndOnceWhenItIsBackUnder()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingLoggerFactory logs = new();

        await using EmbeddedKahunaNode node = await StartNodeAsync(logs, o => o.DurableRetainedStateHeapShare = 0.4, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        DurableMaintenanceService maintenance = kahuna.KeyValues.DurableMaintenance;

        await CommitTransactionsAsync(node, TransactionCount, "share");
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count >= TransactionCount && kahuna.KeyValues.DurablePreparedIntentStore.SettledIdentityCount >= TransactionCount);

        long retained = RetainedBytes(kahuna);
        Assert.True(retained > 0);

        // A heap limit ten times the retained state: the share is 10%, under the 40% this node warns at. The
        // first check states the byte value of the share once.
        long roomyLimit = retained * 10;
        maintenance.HeapLimitProbe = () => roomyLimit;
        maintenance.CheckRetainedStateHeapShare();

        Assert.InRange(maintenance.RetainedStateHeapShare, 0.05, 0.2);
        Assert.Single(logs.Matching(LogLevel.Information, "The retained transaction state (transaction records, completion receipts, committed-head ledger with settled identities) warns above"));
        Assert.Empty(logs.Matching(LogLevel.Warning, "Retained transaction state is"));

        // A heap limit twice the retained state: the share is 50%. One warning, with the size of each store
        // and the settings that lower it.
        long tightLimit = RetainedBytes(kahuna) * 2;
        maintenance.HeapLimitProbe = () => tightLimit;
        maintenance.CheckRetainedStateHeapShare();

        Assert.InRange(maintenance.RetainedStateHeapShare, 0.4, 0.6);
        string warning = Assert.Single(logs.Matching(LogLevel.Warning, "Retained transaction state is"));
        Assert.Contains("transaction records", warning, StringComparison.Ordinal);
        Assert.Contains("completion receipts", warning, StringComparison.Ordinal);
        Assert.Contains("settled identities", warning, StringComparison.Ordinal);
        Assert.Contains("DurableRecordRetentionFloor", warning, StringComparison.Ordinal);

        // The condition continues: the gauge is the signal between reminders, not a line for each tick.
        for (int i = 0; i < 5; i++)
            maintenance.CheckRetainedStateHeapShare();

        Assert.Single(logs.Matching(LogLevel.Warning, "Retained transaction state is"));

        // The periodic tick runs the same check: the sweep entry point refreshes the gauge.
        maintenance.HeapLimitProbe = () => roomyLimit;
        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);

        Assert.InRange(maintenance.RetainedStateHeapShare, 0.05, 0.2);
        Assert.Single(logs.Matching(LogLevel.Information, "Retained transaction state is back under"));
        Assert.Single(logs.Matching(LogLevel.Information, "warns above"));
    }

    [Fact]
    public async Task TheShareIsReported_OnAGauge_AndTheSettledIdentitiesOnTheirOwn()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingLoggerFactory logs = new();

        await using EmbeddedKahunaNode node = await StartNodeAsync(logs, static _ => { }, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        DurableMaintenanceService maintenance = kahuna.KeyValues.DurableMaintenance;

        await CommitTransactionsAsync(node, TransactionCount, "gauge");
        await WaitUntil(() => kahuna.KeyValues.DurablePreparedIntentStore.SettledIdentityCount >= TransactionCount);

        long limit = RetainedBytes(kahuna) * 4;
        maintenance.HeapLimitProbe = () => limit;
        maintenance.CheckRetainedStateHeapShare();

        double share = 0;
        long settledIdentities = 0;

        using MeterListener listener = new();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == "Kahuna"
                && instrument.Name is "kahuna.durable_tx.retained_state_heap_share" or "kahuna.durable_tx.settled_identity_entries")
                l.EnableMeasurementEvents(instrument);
        };

        // Several nodes of other tests publish the same instruments in this process: the largest share and the
        // sum of the identities are lower bounds for this node's values.
        listener.SetMeasurementEventCallback<double>((instrument, value, _, _) =>
        {
            if (instrument.Name == "kahuna.durable_tx.retained_state_heap_share" && Math.Abs(value - maintenance.RetainedStateHeapShare) < 1e-9)
                share = value;
        });
        listener.SetMeasurementEventCallback<long>((instrument, value, _, _) =>
        {
            if (instrument.Name == "kahuna.durable_tx.settled_identity_entries")
                Interlocked.Add(ref settledIdentities, value);
        });
        listener.Start();
        listener.RecordObservableInstruments();

        Assert.InRange(share, 0.2, 0.3);
        Assert.True(Interlocked.Read(ref settledIdentities) >= TransactionCount, $"{settledIdentities} settled identities reported");
    }

    [Fact]
    public async Task ANonPositiveShare_TurnsTheWarningOff_AndKeepsTheGauge()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingLoggerFactory logs = new();

        await using EmbeddedKahunaNode node = await StartNodeAsync(logs, o => o.DurableRetainedStateHeapShare = 0, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        DurableMaintenanceService maintenance = kahuna.KeyValues.DurableMaintenance;

        await CommitTransactionsAsync(node, TransactionCount, "off");
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count >= TransactionCount);

        long limit = RetainedBytes(kahuna);
        maintenance.HeapLimitProbe = () => limit;
        maintenance.CheckRetainedStateHeapShare();

        Assert.InRange(maintenance.RetainedStateHeapShare, 0.9, 1.1);
        Assert.Empty(logs.Matching(LogLevel.Warning, "Retained transaction state is"));
        Assert.Empty(logs.Matching(LogLevel.Information, "warns above"));
    }

    [Fact]
    public async Task AByteBudgetAboveTheShare_IsReportedAtTheFirstCheck()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        CapturingLoggerFactory logs = new();

        // The byte budget of the records and receipts is 256 MiB, and 40% of this node's heap limit is
        // 40 MiB: the budget alone lets the retained state pass the share.
        await using EmbeddedKahunaNode node = await StartNodeAsync(logs, o => o.DurableRecordRetentionMaxBytes = 256L * 1024 * 1024, ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        DurableMaintenanceService maintenance = kahuna.KeyValues.DurableMaintenance;

        maintenance.HeapLimitProbe = static () => 100L * 1024 * 1024;
        maintenance.CheckRetainedStateHeapShare();
        maintenance.CheckRetainedStateHeapShare();

        Assert.Single(logs.Matching(LogLevel.Warning, "DurableRecordRetentionMaxBytes (268435456 bytes) is above"));
    }
}

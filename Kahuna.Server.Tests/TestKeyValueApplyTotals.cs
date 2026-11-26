using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The totals of the ordered apply path by entry type: a one-phase commit applies a record init, a prepare and a
/// decision, and its settle applies as an intent delta with no prepare. The totals belong to the node, so each node
/// of a process has its own.
/// </summary>
public sealed class TestKeyValueApplyTotals
{
    private readonly ILoggerFactory loggerFactory;

    public TestKeyValueApplyTotals(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaOptions Options(bool materializeOnResolve) => new()
    {
        TimerInitialDelay = TimeSpan.FromMilliseconds(50),
        ReadIOThreads = 1,
        WriteIOThreads = 1,
        PartitionExecutorPoolSize = 1,
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1,
        DurableMaterializeOnResolve = materializeOnResolve
    };

    private static byte[] Script(string key) =>
        Encoding.UTF8.GetBytes($"BEGIN SET `{key}` 'value' COMMIT END");

    private static long[] Snapshot(KeyValueApplyTotals totals)
    {
        long[] counts = new long[KeyValueApplyTotals.TypeCount];
        for (int i = 0; i < counts.Length; i++)
            counts[i] = totals.Entries((KeyValueApplyEntryType)i);
        return counts;
    }

    private static long Delta(KeyValueApplyTotals totals, long[] before, KeyValueApplyEntryType type) =>
        totals.Entries(type) - before[(int)type];

    private static async Task WaitUntilAsync(Func<bool> condition, string what, CancellationToken ct)
    {
        long deadline = Environment.TickCount64 + 15_000;
        while (!condition())
        {
            if (Environment.TickCount64 > deadline)
                Assert.Fail("Timed out waiting until " + what);

            await Task.Delay(20, ct);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task OnePhaseCommits_CountTheirEntriesByType(bool materializeOnResolve)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        const int commits = 6;

        await using EmbeddedKahunaNode node = new(Options(materializeOnResolve), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("totals/row-0", ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        KeyValueApplyTotals totals = manager.KeyValues.ReplicationDispatcher.ApplyTotals;
        totals.TimeAlwaysForTesting = true;

        long[] before = Snapshot(totals);
        double settleMsBefore = totals.Milliseconds(KeyValueApplyEntryType.Settle);
        double prepareMsBefore = totals.Milliseconds(KeyValueApplyEntryType.Prepare);
        double recordMsBefore = totals.Milliseconds(KeyValueApplyEntryType.Record);

        for (int i = 0; i < commits; i++)
        {
            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(Script($"totals/row-{i}"), null, null);
            Assert.Equal(KeyValueResponseType.Set, result.Type);
        }

        await WaitUntilAsync(() => manager.DurablePreparedIntentStore.Count == 0, "every intent settled", ct);
        await WaitUntilAsync(() => Delta(totals, before, KeyValueApplyEntryType.Settle) >= commits, "every settle applied", ct);

        // Each one-phase commit: a record init and a decision, one prepare, and one settle of its own.
        Assert.Equal(2 * commits, Delta(totals, before, KeyValueApplyEntryType.Record));
        Assert.Equal(commits, Delta(totals, before, KeyValueApplyEntryType.Prepare));
        Assert.Equal(commits, Delta(totals, before, KeyValueApplyEntryType.Settle));
        Assert.Equal(0, Delta(totals, before, KeyValueApplyEntryType.Bundle));

        // With the materialization records, each commit writes one key-value entry for its key.
        if (materializeOnResolve)
            Assert.Equal(0, Delta(totals, before, KeyValueApplyEntryType.Kv));
        else
            Assert.Equal(commits, Delta(totals, before, KeyValueApplyEntryType.Kv));

        Assert.True(totals.Milliseconds(KeyValueApplyEntryType.Settle) > settleMsBefore);
        Assert.True(totals.Milliseconds(KeyValueApplyEntryType.Prepare) > prepareMsBefore);
        Assert.True(totals.Milliseconds(KeyValueApplyEntryType.Record) > recordMsBefore);
    }

    [Fact]
    public async Task ApplyTime_IsNotRead_WhileNoListenerReadsIt()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(Options(materializeOnResolve: true), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("untimed/row-0", ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;
        KeyValueApplyTotals totals = manager.KeyValues.ReplicationDispatcher.ApplyTotals;

        // Another test of the process can listen on the instrument; this node's own counter is what it reads.
        if (totals.TimingEnabled)
            return;

        long prepares = totals.Entries(KeyValueApplyEntryType.Prepare);
        double prepareMs = totals.Milliseconds(KeyValueApplyEntryType.Prepare);

        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(Script("untimed/row-1"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        await WaitUntilAsync(() => totals.Entries(KeyValueApplyEntryType.Prepare) > prepares, "the prepare applied", ct);
        Assert.Equal(prepareMs, totals.Milliseconds(KeyValueApplyEntryType.Prepare));
    }
}

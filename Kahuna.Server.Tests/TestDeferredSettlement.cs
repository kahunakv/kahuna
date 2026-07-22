using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Deferred settlement: a durable transaction's post-decision resolution (materialize committed values, settle
/// intents) runs off the commit critical path, so finalize returns as soon as the decision record is durable.
/// Read-your-writes must still hold in the decision→settlement window — a read of a committed key resolves the
/// value through the durable-intent visibility path (the canonical record, locally or routed) even before the
/// value materializes into MVCC — and the value must ultimately materialize.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDeferredSettlement
{
    private readonly ILoggerFactory loggerFactory;

    public TestDeferredSettlement(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaOptions Options(bool deferred) => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 4,
        DurableDeferredSettlement = deferred
    };

    [Fact]
    public async Task Deferred_Commit_ReadYourWrites_HoldsAndMaterializes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(Options(deferred: true), loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("def/row-1", ct);

        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `def/row-1` 'v1' SET `def/row-2` 'v2' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        // Read-your-writes: immediately after commit — before background settlement necessarily materialized the
        // value — the committed value is visible (resolved through the durable-intent visibility path if pending).
        (KeyValueResponseType t1, ReadOnlyKeyValueEntry? e1) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "def/row-1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, t1);
        Assert.Equal("v1"u8.ToArray(), e1!.Value);

        (KeyValueResponseType t2, ReadOnlyKeyValueEntry? e2) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "def/row-2", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, t2);
        Assert.Equal("v2"u8.ToArray(), e2!.Value);

        // The decision record is durable, and settlement eventually materializes (the prepared intents drain).
        KahunaManager kahuna = (KahunaManager)node.Kahuna;
        Assert.True(kahuna.DurableTransactionRecordStore.Count > 0);
        await WaitUntil(() => kahuna.DurablePreparedIntentStore.Count == 0);
    }

    [Fact]
    public async Task Synchronous_And_Deferred_BothCommitAndReadBack()
    {
        // The flag must not change observable commit/read semantics — only when settlement runs.
        CancellationToken ct = TestContext.Current.CancellationToken;

        foreach (bool deferred in new[] { false, true })
        {
            await using EmbeddedKahunaNode node = new(Options(deferred), loggerFactory);
            await node.StartAsync(ct);
            await node.WaitForLeaderForKeyAsync("both/row-1", ct);

            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes("BEGIN SET `both/row-1` 'x' COMMIT END"), null, null);
            Assert.Equal(KeyValueResponseType.Set, result.Type);

            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, "both/row-1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.Equal("x"u8.ToArray(), entry!.Value);
        }
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 5000)
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

using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// End-to-end coverage of the opt-in durable prepared-intent 2PC path
/// (<see cref="Kahuna.Server.Configuration.KahunaConfiguration.EnableDurableIntentTransactions"/>): an
/// all-persistent multi-key transaction is finalized through durable committed intents plus a canonical
/// decision record — no manual Raft ticket — and its values materialize into visible MVCC on commit. Runs on a
/// real embedded node so the actual coordinator → finalizer → replicate → replicator apply path is exercised.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableIntentTransactionEndToEnd
{
    private readonly ILoggerFactory loggerFactory;

    public TestDurableIntentTransactionEndToEnd(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task DurablePath_MultiKeyPersistentTransaction_CommitsAndReadsBack()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            EnableDurableIntentTransactions = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("orders/row-1", ct);

        const int count = 8;
        StringBuilder script = new("BEGIN ");
        for (int i = 1; i <= count; i++)
            script.Append($"SET `orders/row-{i}` 'value-{i}' ");
        script.Append("COMMIT END");

        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes(script.ToString()), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        // Every committed value materialized into visible MVCC through the durable-intent path.
        for (int i = 1; i <= count; i++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, $"orders/row-{i}", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

            Assert.Equal(KeyValueResponseType.Get, type);
            Assert.NotNull(entry);
            Assert.Equal(Encoding.UTF8.GetBytes($"value-{i}"), entry!.Value);
        }
    }
}

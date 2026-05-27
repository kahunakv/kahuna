using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public sealed class TestEmbeddedKahunaNode
{
    private readonly ILoggerFactory loggerFactory;

    public TestEmbeddedKahunaNode(ITestOutputHelper outputHelper)
    {
        loggerFactory = LoggerFactory.Create(builder =>
        {
            builder
                .AddXUnit(outputHelper)
                .SetMinimumLevel(LogLevel.Debug);
        });
    }

    [Fact]
    public async Task TestEmbeddedNodeCanStartUseKeyValuesAndDispose()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);

        string leader = await node.WaitForLeaderForKeyAsync("tenant/table/key-a", TestContext.Current.CancellationToken);
        Assert.Equal(node.Raft.GetLocalEndpoint(), leader);

        byte[] valueA = Encoding.UTF8.GetBytes("value-a");
        (KeyValueResponseType response, long revision, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            "tenant/table/key-a",
            valueA,
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Set, response);
        Assert.Equal(0, revision);

        (response, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            "tenant/table/key-a",
            -1,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Get, response);
        Assert.NotNull(entry);
        Assert.Equal(valueA, entry.Value);

        await SetValue(node, "tenant/table/key-c", "value-c");
        await SetValue(node, "tenant/table/key-b", "value-b");

        KeyValueGetByBucketResult bucket = await node.Kahuna.LocateAndGetByBucket(
            HLCTimestamp.Zero,
            "tenant/table",
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Get, bucket.Type);
        Assert.Equal(
            ["tenant/table/key-a", "tenant/table/key-b", "tenant/table/key-c"],
            bucket.Items.Select(item => item.Item1).ToArray()
        );

        (response, HLCTimestamp transactionId) = await node.Kahuna.LocateAndStartTransaction(
            new()
            {
                UniqueId = "tenant/tx",
                Timeout = 5000,
                Locking = KeyValueTransactionLocking.Pessimistic
            },
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Set, response);

        string txKey = "tenant/table/key-tx";
        (response, _, _) = await node.Kahuna.LocateAndTryAcquireExclusiveLock(
            transactionId,
            txKey,
            5000,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Locked, response);

        (response, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            transactionId,
            txKey,
            Encoding.UTF8.GetBytes("value-tx"),
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Set, response);

        response = await node.Kahuna.LocateAndCommitTransaction(
            "tenant/tx",
            transactionId,
            [new() { Key = txKey, Durability = KeyValueDurability.Persistent }],
            [new() { Key = txKey, Durability = KeyValueDurability.Persistent }],
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Committed, response);

        (response, entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero,
            txKey,
            -1,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Get, response);
        Assert.NotNull(entry);
        Assert.Equal("value-tx", Encoding.UTF8.GetString(entry.Value ?? []));
    }

    [Fact]
    public async Task TestEmbeddedNodeCanBeCreatedTwice()
    {
        await using (EmbeddedKahunaNode first = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory))
        {
            await first.StartAsync(TestContext.Current.CancellationToken);
        }

        await using EmbeddedKahunaNode second = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await second.StartAsync(TestContext.Current.CancellationToken);
        string leader = await second.WaitForLeaderForKeyAsync("tenant/table/key-a", TestContext.Current.CancellationToken);

        Assert.Equal(second.Raft.GetLocalEndpoint(), leader);
    }

    private static async Task SetValue(EmbeddedKahunaNode node, string key, string value)
    {
        (KeyValueResponseType response, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero,
            key,
            Encoding.UTF8.GetBytes(value),
            null,
            -1,
            KeyValueFlags.Set,
            0,
            KeyValueDurability.Persistent,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(KeyValueResponseType.Set, response);
    }
}

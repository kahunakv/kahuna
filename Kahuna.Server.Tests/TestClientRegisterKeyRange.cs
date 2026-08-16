
using Kahuna.Client;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

public sealed class TestClientRegisterKeyRange
{
    private readonly ILoggerFactory loggerFactory;

    public TestClientRegisterKeyRange(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaNode CreateNode(ILoggerFactory lf) => new(new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    }, lf);

    /// <summary>
    /// After RegisterKeyRange seeds the key space, writes and a subsequent range scan work correctly.
    /// </summary>
    [Fact]
    public async Task RegisterKeyRange_EnablesRangeRouting()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string space = "client/rng/" + Guid.NewGuid().ToString("N")[..8];

        KahunaRegisterKeyRangeResponse seeded = await client.RegisterKeyRange(
            space, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(seeded.Success);
        Assert.True(seeded.Seeded);

        string[] keys = [$"{space}/a", $"{space}/b", $"{space}/c"];
        foreach (string key in keys)
            await client.SetKeyValue(key, key, 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaKeyValue> results = await client.GetByRange(
            space,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(keys.Length, results.Count);
        Assert.All(results, kv => Assert.True(kv.Success));

        foreach (string key in keys)
            Assert.Contains(results, kv => kv.Key == key);
    }

    /// <summary>
    /// Registering the same key space twice succeeds both times, but only the first call seeds the
    /// descriptor. Callers are expected to register on every node, so "someone already seeded this"
    /// is the normal answer for all but one of those calls — not a failure.
    /// </summary>
    [Fact]
    public async Task RegisterKeyRange_SecondCall_SucceedsWithoutSeedingAgain()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string space = "client/rng2/" + Guid.NewGuid().ToString("N")[..8];

        KahunaRegisterKeyRangeResponse first = await client.RegisterKeyRange(
            space, cancellationToken: TestContext.Current.CancellationToken);
        KahunaRegisterKeyRangeResponse second = await client.RegisterKeyRange(
            space, cancellationToken: TestContext.Current.CancellationToken);

        // Only the first call seeds; the second is still a success, because registering on every
        // node is the contract and only one of those calls can be the one that seeds.
        Assert.True(first.Seeded);
        Assert.False(second.Seeded);
        Assert.True(second.Success);
        Assert.Equal("AlreadySeeded", second.Status);
    }
}

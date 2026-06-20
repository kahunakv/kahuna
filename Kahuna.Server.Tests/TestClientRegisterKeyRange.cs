
using Kahuna.Client;
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

        bool seeded = await client.RegisterKeyRange(space, TestContext.Current.CancellationToken);
        Assert.True(seeded);

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
    /// Registering the same key space twice returns false on the second call (already seeded).
    /// </summary>
    [Fact]
    public async Task RegisterKeyRange_SecondCall_ReturnsFalse()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string space = "client/rng2/" + Guid.NewGuid().ToString("N")[..8];

        bool first  = await client.RegisterKeyRange(space, TestContext.Current.CancellationToken);
        bool second = await client.RegisterKeyRange(space, TestContext.Current.CancellationToken);

        Assert.True(first);
        Assert.False(second);
    }
}

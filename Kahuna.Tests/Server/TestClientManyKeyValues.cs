
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

public sealed class TestClientManyKeyValues
{
    private readonly ILoggerFactory loggerFactory;

    public TestClientManyKeyValues(ITestOutputHelper outputHelper)
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
    /// GetManyKeyValues returns all keys that were set, with Success=true and the correct values.
    /// </summary>
    [Fact]
    public async Task GetManyKeyValues_ReturnsAll()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "many/get/" + Guid.NewGuid().ToString("N")[..6];
        string[] keys = [$"{prefix}/a", $"{prefix}/b", $"{prefix}/c"];

        foreach (string key in keys)
            await client.SetKeyValue(key, key, 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaGetManyKeyValuesRequestItem> request = keys.Select(k => new KahunaGetManyKeyValuesRequestItem
        {
            Key = k,
            Revision = -1,
            Durability = KeyValueDurability.Persistent
        }).ToList();

        List<KahunaKeyValue> results = await client.GetManyKeyValues(request, TestContext.Current.CancellationToken);

        Assert.Equal(keys.Length, results.Count);
        Assert.All(results, kv => Assert.True(kv.Success));

        foreach (string key in keys)
            Assert.Contains(results, kv => kv.Key == key && kv.ValueAsString() == key);
    }

    /// <summary>
    /// GetManyKeyValues returns Success=false for keys that do not exist.
    /// </summary>
    [Fact]
    public async Task GetManyKeyValues_MissingKeys_ReturnNotFound()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "many/get/miss/" + Guid.NewGuid().ToString("N")[..6];

        await client.SetKeyValue($"{prefix}/exists", "yes", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaGetManyKeyValuesRequestItem> request =
        [
            new() { Key = $"{prefix}/exists",    Revision = -1, Durability = KeyValueDurability.Persistent },
            new() { Key = $"{prefix}/no-such",   Revision = -1, Durability = KeyValueDurability.Persistent }
        ];

        List<KahunaKeyValue> results = await client.GetManyKeyValues(request, TestContext.Current.CancellationToken);

        Assert.Equal(2, results.Count);

        KahunaKeyValue found    = results.First(kv => kv.Key == $"{prefix}/exists");
        KahunaKeyValue missing  = results.First(kv => kv.Key == $"{prefix}/no-such");

        Assert.True(found.Success);
        Assert.Equal("yes", found.ValueAsString());

        Assert.False(missing.Success);
    }

    /// <summary>
    /// GetManyKeyValues results carry a non-zero LastModified on found items.
    /// </summary>
    [Fact]
    public async Task GetManyKeyValues_ItemsCarryLastModified()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "many/get/lm/" + Guid.NewGuid().ToString("N")[..6];
        string key = $"{prefix}/x";

        await client.SetKeyValue(key, "hello", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaKeyValue> results = await client.GetManyKeyValues(
            [new() { Key = key, Revision = -1, Durability = KeyValueDurability.Persistent }],
            TestContext.Current.CancellationToken);

        Assert.Single(results);
        Assert.True(results[0].Success);
        Assert.NotEqual(0L, results[0].LastModified);
    }

    /// <summary>
    /// ExistsManyKeyValues returns Success=true for existing keys and false for missing ones.
    /// </summary>
    [Fact]
    public async Task ExistsManyKeyValues_ReturnsAll()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "many/exists/" + Guid.NewGuid().ToString("N")[..6];
        string[] existingKeys = [$"{prefix}/one", $"{prefix}/two"];
        string ghostKey = $"{prefix}/ghost";

        foreach (string key in existingKeys)
            await client.SetKeyValue(key, "v", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaGetManyKeyValuesRequestItem> request = existingKeys
            .Append(ghostKey)
            .Select(k => new KahunaGetManyKeyValuesRequestItem
            {
                Key = k,
                Revision = -1,
                Durability = KeyValueDurability.Persistent
            }).ToList();

        List<KahunaKeyValue> results = await client.ExistsManyKeyValues(request, TestContext.Current.CancellationToken);

        Assert.Equal(3, results.Count);

        Assert.True(results.First(kv => kv.Key == $"{prefix}/one").Success);
        Assert.True(results.First(kv => kv.Key == $"{prefix}/two").Success);
        Assert.False(results.First(kv => kv.Key == ghostKey).Success);
    }

    /// <summary>
    /// ExistsManyKeyValues does not return values — Success conveys existence only.
    /// </summary>
    [Fact]
    public async Task ExistsManyKeyValues_DoesNotReturnValues()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "many/exists/novalue/" + Guid.NewGuid().ToString("N")[..8];
        await client.SetKeyValue(key, "secret", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaKeyValue> results = await client.ExistsManyKeyValues(
            [new() { Key = key, Revision = -1, Durability = KeyValueDurability.Persistent }],
            TestContext.Current.CancellationToken);

        Assert.Single(results);
        Assert.True(results[0].Success);
        Assert.Null(results[0].Value);
    }
}

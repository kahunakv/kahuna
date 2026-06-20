
using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

public sealed class TestClientRangeScan
{
    private readonly ILoggerFactory loggerFactory;

    public TestClientRangeScan(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaNode CreateNode(ILoggerFactory lf) => new(new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    }, lf);

    private static async Task Seed(KahunaClient client, string prefix, int count)
    {
        for (int i = 0; i < count; i++)
        {
            string key = $"{prefix}/{i:D4}";
            await client.SetKeyValue(key, $"v{i}", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// GetByRange returns exactly the bounded subset: only keys in [startKey, endKey).
    /// </summary>
    [Fact]
    public async Task GetByRange_ReturnsBoundedResults()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "range/bounded/" + Guid.NewGuid().ToString("N")[..6];
        await Seed(client, prefix, 10);

        List<KahunaKeyValue> page = await client.GetByRange(
            prefix,
            startKey: $"{prefix}/0002", startInclusive: true,
            endKey:   $"{prefix}/0005", endInclusive: false,
            limit: 100,
            cancellationToken: TestContext.Current.CancellationToken);

        List<string> keys = page.Select(kv => kv.Key).ToList();
        Assert.Contains($"{prefix}/0002", keys);
        Assert.Contains($"{prefix}/0003", keys);
        Assert.Contains($"{prefix}/0004", keys);
        Assert.DoesNotContain($"{prefix}/0005", keys);
        Assert.DoesNotContain($"{prefix}/0001", keys);
        Assert.All(page, kv => Assert.True(kv.Success));
    }

    /// <summary>
    /// GetByRange respects the limit parameter: returns at most <c>limit</c> items.
    /// </summary>
    [Fact]
    public async Task GetByRange_RespectsLimit()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "range/limit/" + Guid.NewGuid().ToString("N")[..6];
        await Seed(client, prefix, 20);

        List<KahunaKeyValue> page = await client.GetByRange(
            prefix,
            limit: 5,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(5, page.Count);
    }

    /// <summary>
    /// ScanByRange streams all items across pages; total count equals the number written.
    /// </summary>
    [Fact]
    public async Task ScanByRange_StreamsAllItems()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "range/stream/" + Guid.NewGuid().ToString("N")[..6];
        const int total = 25;
        await Seed(client, prefix, total);

        List<KahunaKeyValue> all = [];
        await foreach (KahunaKeyValue kv in client.ScanByRange(
            prefix,
            pageSize: 7,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            all.Add(kv);
        }

        Assert.Equal(total, all.Count);
        Assert.All(all, kv => Assert.True(kv.Success));
    }

    /// <summary>
    /// ScanByRange streams only the bounded subset between startKey and endKey.
    /// </summary>
    [Fact]
    public async Task ScanByRange_StreamsBoundedResults()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "range/stream-bounded/" + Guid.NewGuid().ToString("N")[..6];
        await Seed(client, prefix, 10);

        List<string?> keys = [];
        await foreach (KahunaKeyValue kv in client.ScanByRange(
            prefix,
            startKey: $"{prefix}/0003", startInclusive: true,
            endKey:   $"{prefix}/0007", endInclusive: false,
            pageSize: 2,
            cancellationToken: TestContext.Current.CancellationToken))
        {
            keys.Add(kv.Key);
        }

        Assert.Contains($"{prefix}/0003", keys);
        Assert.Contains($"{prefix}/0006", keys);
        Assert.DoesNotContain($"{prefix}/0007", keys);
        Assert.DoesNotContain($"{prefix}/0002", keys);
    }

    /// <summary>
    /// GetByRange as-of a snapshot excludes writes made after the snapshot timestamp.
    /// </summary>
    [Fact]
    public async Task GetByRange_AsOf_ExcludesLaterWrites()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "range/asof/" + Guid.NewGuid().ToString("N")[..6];

        await client.SetKeyValue($"{prefix}/0000", "early", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        await client.SetKeyValue($"{prefix}/0001", "early", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaKeyValue> before = await client.GetByRange(
            prefix, limit: 100, cancellationToken: TestContext.Current.CancellationToken);

        long snapshotMs = before.Max(kv => kv.LastModified);

        await client.SetKeyValue($"{prefix}/0002", "late", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        await client.SetKeyValue($"{prefix}/0003", "late", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaKeyValue> snap = await client.GetByRange(
            prefix, limit: 100,
            snapshotMs: snapshotMs,
            cancellationToken: TestContext.Current.CancellationToken);

        List<string> snapKeys = snap.Select(kv => kv.Key).ToList();
        Assert.Contains($"{prefix}/0000", snapKeys);
        Assert.Contains($"{prefix}/0001", snapKeys);
        Assert.DoesNotContain($"{prefix}/0002", snapKeys);
        Assert.DoesNotContain($"{prefix}/0003", snapKeys);

        List<KahunaKeyValue> latest = await client.GetByRange(
            prefix, limit: 100, cancellationToken: TestContext.Current.CancellationToken);
        Assert.Equal(4, latest.Count);
    }

    /// <summary>
    /// Results from GetByRange carry a non-zero LastModified on each item.
    /// </summary>
    [Fact]
    public async Task GetByRange_ItemsCarryLastModified()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "range/lm/" + Guid.NewGuid().ToString("N")[..6];
        await Seed(client, prefix, 5);

        List<KahunaKeyValue> items = await client.GetByRange(
            prefix, limit: 100,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.NotEmpty(items);
        Assert.All(items, kv => Assert.NotEqual(0L, kv.LastModified));
    }
}


using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

public sealed class TestClientSnapshotExists
{
    private readonly ILoggerFactory loggerFactory;

    public TestClientSnapshotExists(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static EmbeddedKahunaNode CreateNode(ILoggerFactory lf) => new(new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1
    }, lf);

    // ── ExistsKeyValue ─────────────────────────────────────────────────────────

    /// <summary>
    /// ExistsKeyValue AS OF a snapshot before the key was written returns false.
    /// </summary>
    [Fact]
    public async Task ExistsKeyValue_AsOf_BeforeWrite_ReturnsFalse()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        long snapshotBefore = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await Task.Delay(2, TestContext.Current.CancellationToken);

        string key = "snap/exists/before/" + Guid.NewGuid().ToString("N")[..8];
        await client.SetKeyValue(key, "val", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        KahunaKeyValue result = await client.ExistsKeyValue(
            key, KeyValueDurability.Persistent, snapshotBefore, TestContext.Current.CancellationToken);

        Assert.False(result.Success);
    }

    /// <summary>
    /// ExistsKeyValue AS OF a snapshot after the write returns true.
    /// </summary>
    [Fact]
    public async Task ExistsKeyValue_AsOf_AfterWrite_ReturnsTrue()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string key = "snap/exists/after/" + Guid.NewGuid().ToString("N")[..8];
        await client.SetKeyValue(key, "val", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        KahunaKeyValue written = await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        long snapshotAfter = written.LastModified;

        KahunaKeyValue result = await client.ExistsKeyValue(
            key, KeyValueDurability.Persistent, snapshotAfter, TestContext.Current.CancellationToken);

        Assert.True(result.Success);
    }

    // ── GetByBucket ────────────────────────────────────────────────────────────

    /// <summary>
    /// GetByBucket AS OF a snapshot excludes keys written after that point.
    /// </summary>
    [Fact]
    public async Task GetByBucket_AsOf_ExcludesLaterWrites()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "snap/bucket/" + Guid.NewGuid().ToString("N")[..8];

        await client.SetKeyValue($"{prefix}/early", "v1", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        KahunaKeyValue early = await client.GetKeyValue($"{prefix}/early", KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        long snapshot = early.LastModified;

        await Task.Delay(2, TestContext.Current.CancellationToken);
        await client.SetKeyValue($"{prefix}/late", "v2", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaKeyValue> results = await client.GetByBucket(
            prefix, KeyValueDurability.Persistent, snapshot, TestContext.Current.CancellationToken);

        Assert.Contains(results, kv => kv.Key == $"{prefix}/early");
        Assert.DoesNotContain(results, kv => kv.Key == $"{prefix}/late");
    }

    /// <summary>
    /// GetByBucket without snapshotMs returns the current view (all keys).
    /// </summary>
    [Fact]
    public async Task GetByBucket_WithoutSnapshot_ReturnsAllKeys()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "snap/bucket/all/" + Guid.NewGuid().ToString("N")[..8];

        await client.SetKeyValue($"{prefix}/a", "1", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        await client.SetKeyValue($"{prefix}/b", "2", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaKeyValue> results = await client.GetByBucket(prefix, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, results.Count);
        Assert.All(results, kv => Assert.True(kv.Success));
    }

    // ── ScanAllByPrefix ────────────────────────────────────────────────────────

    /// <summary>
    /// ScanAllByPrefix AS OF a snapshot excludes keys written after that point.
    /// </summary>
    [Fact]
    public async Task ScanAllByPrefix_AsOf_ExcludesLaterWrites()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "snap/scan/" + Guid.NewGuid().ToString("N")[..8];

        await client.SetKeyValue($"{prefix}/early", "v1", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        KahunaKeyValue early = await client.GetKeyValue($"{prefix}/early", KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        long snapshot = early.LastModified;

        await Task.Delay(2, TestContext.Current.CancellationToken);
        await client.SetKeyValue($"{prefix}/late", "v2", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaKeyValue> results = await client.ScanAllByPrefix(
            prefix, KeyValueDurability.Persistent, snapshot, TestContext.Current.CancellationToken);

        Assert.Contains(results, kv => kv.Key == $"{prefix}/early");
        Assert.DoesNotContain(results, kv => kv.Key == $"{prefix}/late");
    }

    /// <summary>
    /// ScanAllByPrefix without snapshotMs returns the current view (all keys).
    /// </summary>
    [Fact]
    public async Task ScanAllByPrefix_WithoutSnapshot_ReturnsAllKeys()
    {
        await using EmbeddedKahunaNode node = CreateNode(loggerFactory);
        await node.StartAsync(TestContext.Current.CancellationToken);

        KahunaClient client = new("http://localhost", communication: new InProcessKahunaCommunication(node.Kahuna));

        string prefix = "snap/scan/all/" + Guid.NewGuid().ToString("N")[..8];

        await client.SetKeyValue($"{prefix}/a", "1", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        await client.SetKeyValue($"{prefix}/b", "2", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        List<KahunaKeyValue> results = await client.ScanAllByPrefix(prefix, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(2, results.Count);
        Assert.All(results, kv => Assert.True(kv.Success));
    }
}


using Kahuna.Client;
using Kahuna.Shared.KeyValue;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

[Collection("ClusterTests")]
public sealed class TestClientPointRead
{
    private readonly ILoggerFactory loggerFactory;

    public TestClientPointRead(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>
    /// Write v1, capture its LastModified, overwrite with v2.
    /// GetKeyValue(key, snapshotMs: t1) must return v1.
    /// GetKeyValue(key) must return v2.
    /// </summary>
    [Fact]
    public async Task GetKeyValue_AsOf_ServesHistoricalRevision()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);

        InProcessKahunaCommunication comm = new(node.Kahuna);
        KahunaClient client = new("http://localhost", communication: comm);

        string key = "client/snap/" + Guid.NewGuid().ToString("N")[..8];

        KahunaKeyValue setV1 = await client.SetKeyValue(key, "version-one", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.True(setV1.Success);

        KahunaKeyValue readV1 = await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(readV1.Success);
        Assert.NotEqual(0L, readV1.LastModified);

        long snapshotMs = readV1.LastModified;

        KahunaKeyValue setV2 = await client.SetKeyValue(key, "version-two", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
        Assert.True(setV2.Success);

        KahunaKeyValue snap = await client.GetKeyValue(key, KeyValueDurability.Persistent, snapshotMs, TestContext.Current.CancellationToken);
        Assert.True(snap.Success);
        Assert.Equal("version-one", snap.ValueAsString());

        KahunaKeyValue latest = await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(latest.Success);
        Assert.Equal("version-two", latest.ValueAsString());
    }

    /// <summary>
    /// A normal GetKeyValue surfaces a non-zero LastModified that round-trips back into a
    /// successful snapshot read returning the same value.
    /// </summary>
    [Fact]
    public async Task GetKeyValue_ExposesLastModified_RoundTripsToSnapshot()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);

        InProcessKahunaCommunication comm = new(node.Kahuna);
        KahunaClient client = new("http://localhost", communication: comm);

        string key = "client/lm/" + Guid.NewGuid().ToString("N")[..8];

        await client.SetKeyValue(key, "value-a", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        KahunaKeyValue result = await client.GetKeyValue(key, KeyValueDurability.Persistent, cancellationToken: TestContext.Current.CancellationToken);
        Assert.True(result.Success);
        Assert.NotEqual(0L, result.LastModified);

        KahunaKeyValue replay = await client.GetKeyValue(key, KeyValueDurability.Persistent, result.LastModified, TestContext.Current.CancellationToken);
        Assert.True(replay.Success);
        Assert.Equal("value-a", replay.ValueAsString());
    }

    /// <summary>
    /// Snapshot read at a timestamp before the key was written returns a not-found result.
    /// </summary>
    [Fact]
    public async Task GetKeyValue_AsOf_BeforeWrite_ReturnsNotFound()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);

        InProcessKahunaCommunication comm = new(node.Kahuna);
        KahunaClient client = new("http://localhost", communication: comm);

        long beforeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await Task.Delay(10, TestContext.Current.CancellationToken);

        string key = "client/before/" + Guid.NewGuid().ToString("N")[..8];
        await client.SetKeyValue(key, "appeared-later", 0, KeyValueFlags.Set, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        KahunaKeyValue snap = await client.GetKeyValue(key, KeyValueDurability.Persistent, beforeMs, TestContext.Current.CancellationToken);
        Assert.False(snap.Success);
    }
}

using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A standalone embedded node elects itself through two phantom witness peers. The witnesses are
/// members of the Raft roster, but they hold no data and have no Kahuna transport. A prefix scan
/// fans out to the peers that may hold a copy of the data, so it must leave the witnesses out: a
/// scan that asks a witness fails for every caller of a standalone node.
/// </summary>
public sealed class TestStandaloneScanAllByPrefix
{
    private readonly ILoggerFactory loggerFactory;

    public TestStandaloneScanAllByPrefix(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Theory]
    [InlineData(KeyValueDurability.Persistent)]
    [InlineData(KeyValueDurability.Ephemeral)]
    public async Task ScanAllByPrefix_OnStandaloneNode_ReturnsLocalKeys(KeyValueDurability durability)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 3
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("standalone-scan/a", ct);

        foreach (string key in (string[])["standalone-scan/a", "standalone-scan/b", "other/c"])
        {
            (KeyValueResponseType set, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(key), null, -1,
                KeyValueFlags.Set, 0, durability, ct);
            Assert.Equal(KeyValueResponseType.Set, set);
        }

        KeyValueGetByBucketResult result = await node.Kahuna.ScanAllByPrefix(
            "standalone-scan/", HLCTimestamp.Zero, durability, ct);

        Assert.Equal(KeyValueResponseType.Get, result.Type);
        Assert.Equal(
            ["standalone-scan/a", "standalone-scan/b"],
            result.Items.Select(static item => item.Item1).Order(StringComparer.Ordinal).ToArray());
    }
}

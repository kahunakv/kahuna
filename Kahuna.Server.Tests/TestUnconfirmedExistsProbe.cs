using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Pins the contract of the unconfirmed exists probe used by the commit-time staged-base
/// compare-and-set (<c>LocateAndTryExistsManyValuesUnconfirmed</c>): on the leader it must answer
/// exactly what the ordinary confirmed probe answers — same existence, same revision, same
/// per-key response types — because the staged-base CAS compares revisions against these results
/// and a divergent answer would turn into a wrong commit-time abort or a missed moved-base
/// conflict. The variant differs only in what it skips (the read-index round and per-key leader
/// waits), never in what it reports.
///
/// The moved-base conflict semantics themselves are covered end-to-end by the durable-transaction
/// suites (<c>TestDurableTransactionFinalizer</c>, <c>TestOnePhaseBundledCommitGate</c>,
/// <c>TestTransactionConcurrencyPolicy</c>), which on a single node now flow through this variant.
/// </summary>
public sealed class TestUnconfirmedExistsProbe
{
    private readonly ILoggerFactory loggerFactory;

    public TestUnconfirmedExistsProbe(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task UnconfirmedProbe_MatchesConfirmedProbe_OnLeader()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("probe/existing", ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;

        // An existing key (with a real revision), an overwritten key (revision > 0), a deleted key,
        // and a never-written key — the four shapes the staged-base CAS distinguishes.
        Assert.Equal(KeyValueResponseType.Set, (await manager.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "probe/existing", Encoding.UTF8.GetBytes("v"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

        Assert.Equal(KeyValueResponseType.Set, (await manager.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "probe/rewritten", Encoding.UTF8.GetBytes("v0"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);
        Assert.Equal(KeyValueResponseType.Set, (await manager.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "probe/rewritten", Encoding.UTF8.GetBytes("v1"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);

        Assert.Equal(KeyValueResponseType.Set, (await manager.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, "probe/deleted", Encoding.UTF8.GetBytes("v"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct)).Item1);
        Assert.Equal(KeyValueResponseType.Deleted, (await manager.LocateAndTryDeleteKeyValue(
            HLCTimestamp.Zero, "probe/deleted", KeyValueDurability.Persistent, ct)).Item1);

        List<(string key, long revision, KeyValueDurability durability)> probes =
        [
            ("probe/existing", -1, KeyValueDurability.Persistent),
            ("probe/rewritten", -1, KeyValueDurability.Persistent),
            ("probe/deleted", -1, KeyValueDurability.Persistent),
            ("probe/missing", -1, KeyValueDurability.Persistent)
        ];

        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> confirmed =
            await manager.LocateAndTryExistsManyValues(HLCTimestamp.Zero, HLCTimestamp.Zero, probes, ct);

        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> unconfirmed =
            await manager.LocateAndTryExistsManyValuesUnconfirmed(HLCTimestamp.Zero, HLCTimestamp.Zero, probes, ct);

        Assert.Equal(confirmed.Count, unconfirmed.Count);

        Dictionary<string, (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry)> confirmedByKey = new(confirmed.Count);
        foreach ((KeyValueResponseType type, string key, KeyValueDurability _, ReadOnlyKeyValueEntry? entry) in confirmed)
            confirmedByKey[key] = (type, entry);

        foreach ((KeyValueResponseType type, string key, KeyValueDurability _, ReadOnlyKeyValueEntry? entry) in unconfirmed)
        {
            (KeyValueResponseType expectedType, ReadOnlyKeyValueEntry? expectedEntry) = confirmedByKey[key];

            Assert.Equal(expectedType, type);
            Assert.Equal(expectedEntry is not null, entry is not null);
            if (expectedEntry is not null)
                Assert.Equal(expectedEntry.Revision, entry!.Revision);
        }

        // The staged-base CAS keys on these exact shapes: a positive answer must carry the revision
        // the last committed write produced.
        Assert.Equal(KeyValueResponseType.Exists, confirmedByKey["probe/existing"].type);
        Assert.Equal(KeyValueResponseType.Exists, confirmedByKey["probe/rewritten"].type);
        Assert.Equal(1, confirmedByKey["probe/rewritten"].entry!.Revision);
        Assert.NotEqual(KeyValueResponseType.Exists, confirmedByKey["probe/deleted"].type);
        Assert.NotEqual(KeyValueResponseType.Exists, confirmedByKey["probe/missing"].type);
    }

    [Fact]
    public async Task UnconfirmedProbe_RejectsInvalidInput_LikeConfirmedProbe()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("probe/x", ct);

        KahunaManager manager = (KahunaManager)node.Kahuna;

        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> emptyList =
            await manager.LocateAndTryExistsManyValuesUnconfirmed(HLCTimestamp.Zero, HLCTimestamp.Zero, [], ct);
        Assert.Single(emptyList);
        Assert.Equal(KeyValueResponseType.InvalidInput, emptyList[0].type);

        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> emptyKey =
            await manager.LocateAndTryExistsManyValuesUnconfirmed(HLCTimestamp.Zero, HLCTimestamp.Zero,
                [("", -1, KeyValueDurability.Persistent)], ct);
        Assert.Single(emptyKey);
        Assert.Equal(KeyValueResponseType.InvalidInput, emptyKey[0].type);
    }
}

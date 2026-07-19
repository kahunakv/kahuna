using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// End-to-end coverage of the range-scan intent overlay: a persistent range scan reconciles its page with
/// lingering durable prepared intents — a committed intent-only key is injected, a committed intent overrides an
/// existing key's value, and an aborted intent-only key stays invisible. Exercises the real persistent
/// (RangeScanContinuation) scan path through the locator entry point.
/// </summary>
public sealed class TestDurableIntentScanVisibility
{
    private const string Prefix = "scanx/data";

    private readonly ILoggerFactory loggerFactory;

    public TestDurableIntentScanVisibility(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static PreparedIntent Intent(string key, byte[] value) => new(
        TransactionId: new HLCTimestamp(0, 100, 0), Epoch: 1, Key: key, ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(0, 200, 0),
        State: KeyValueState.Set, Value: value, Bucket: null, Revision: 7, Expires: HLCTimestamp.Zero,
        NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set,
        RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0), Resolution: PreparedIntentResolution.Pending);

    private static void InjectCommitted(PreparedIntentStore store, string key, byte[] value)
    {
        store.Apply(new PrepareIntentCommand(Intent(key, value)));
        store.Apply(new ResolveIntentCommand(new HLCTimestamp(0, 100, 0), 1, key, Commit: true));
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            EnableDurableIntentTransactions = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(Prefix + "/a", ct);
        return node;
    }

    private static async Task SetKey(EmbeddedKahunaNode node, string key, string value, CancellationToken ct)
    {
        await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct);

        for (int i = 0; i < 200; i++)
        {
            (KeyValueResponseType t, _) = await node.Kahuna.LocateAndTryGetValue(
                HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
            if (t == KeyValueResponseType.Get)
                return;
            await Task.Delay(25, ct);
        }

        Assert.Fail($"key {key} never became visible");
    }

    [Fact]
    public async Task RawScan_ReturnsMaterializedKeys_NoIntents()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await SetKey(node, Prefix + "/a", "A1", ct);
        await SetKey(node, Prefix + "/c", "C1", ct);

        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, Prefix, null, true, null, false, 100, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        List<string> keys = result.Items.Select(i => i.Item1).OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.Equal([Prefix + "/a", Prefix + "/c"], keys);
    }

    [Fact]
    public async Task RangeScan_OverlaysCommittedIntents_InjectOverrideExclude()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        await SetKey(node, Prefix + "/a", "A1", ct);
        await SetKey(node, Prefix + "/c", "C1", ct);

        PreparedIntentStore store = ((KahunaManager)node.Kahuna).DurablePreparedIntentStore;
        InjectCommitted(store, Prefix + "/a", Encoding.UTF8.GetBytes("A2")); // committed override of a materialized key
        InjectCommitted(store, Prefix + "/b", Encoding.UTF8.GetBytes("B2")); // committed intent-only key (injected)
        // Aborted intent-only key d: prepare then abort — must stay invisible.
        store.Apply(new PrepareIntentCommand(Intent(Prefix + "/d", Encoding.UTF8.GetBytes("Dx"))));
        store.Apply(new ResolveIntentCommand(new HLCTimestamp(0, 100, 0), 1, Prefix + "/d", Commit: false));

        KeyValueGetByRangeResult result = await node.Kahuna.LocateAndGetByRange(
            HLCTimestamp.Zero, Prefix, null, true, null, false, 100, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.Get, result.Type);

        Dictionary<string, byte[]?> byKey = result.Items.ToDictionary(i => i.Item1, i => i.Item2.Value);

        Assert.Equal([Prefix + "/a", Prefix + "/b", Prefix + "/c"], byKey.Keys.OrderBy(k => k, StringComparer.Ordinal).ToList());
        Assert.Equal(Encoding.UTF8.GetBytes("A2"), byKey[Prefix + "/a"]); // overridden by the committed intent
        Assert.Equal(Encoding.UTF8.GetBytes("B2"), byKey[Prefix + "/b"]); // injected intent-only key
        Assert.Equal(Encoding.UTF8.GetBytes("C1"), byKey[Prefix + "/c"]); // untouched materialized key
        Assert.DoesNotContain(Prefix + "/d", byKey.Keys); // aborted intent invisible
    }
}

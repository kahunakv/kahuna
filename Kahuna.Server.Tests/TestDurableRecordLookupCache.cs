using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The cache of terminal transaction records behind the routed record lookup: which records it keeps, its cap, and
/// the counter that stands for its size.
/// </summary>
public sealed class TestDurableRecordLookupCache
{
    private const string AnchorKey = "lookup/anchor";

    private readonly ILoggerFactory loggerFactory;

    public TestDurableRecordLookupCache(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
    }

    [Fact]
    public async Task TheCache_KeepsEachTerminalRecordOnce_AndNoOther()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        DurableReplicationGateway gateway = manager.KeyValues.DurableReplicationForTesting;

        HLCTimestamp absent = Id(node);
        Assert.Null(await manager.KeyValues.LookupDurableRecordRouted(absent, 1, AnchorKey, ct));

        HLCTimestamp undecided = Id(node);
        manager.DurableTransactionRecordStore.Apply(Init(undecided));
        Assert.Equal(TransactionDecision.Undecided, (await manager.KeyValues.LookupDurableRecordRouted(undecided, 1, AnchorKey, ct))!.Decision);
        Assert.Equal((0, 0), gateway.TestDurableRecordLookupCacheEntries);

        HLCTimestamp committed = Committed(node, manager);
        for (int i = 0; i < 3; i++)
            Assert.Equal(TransactionDecision.Commit, (await manager.KeyValues.LookupDurableRecordRouted(committed, 1, AnchorKey, ct))!.Decision);

        Assert.Equal((1, 1), gateway.TestDurableRecordLookupCacheEntries);

        // The record that was undecided when it was looked up is answered with its decision afterwards.
        manager.DurableTransactionRecordStore.Apply(new CommitTransactionCommand(undecided, 1, 42, undecided, undecided));
        Assert.Equal(TransactionDecision.Commit, (await manager.KeyValues.LookupDurableRecordRouted(undecided, 1, AnchorKey, ct))!.Decision);
        Assert.Equal((2, 2), gateway.TestDurableRecordLookupCacheEntries);
    }

    [Fact]
    public async Task TheCache_StopsAtItsCap_AndTheRecordsAboveItAreStillAnswered()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        DurableReplicationGateway gateway = manager.KeyValues.DurableReplicationForTesting;
        gateway.TestDurableRecordLookupCacheMax = 4;

        HLCTimestamp[] ids = [.. Enumerable.Range(0, 12).Select(_ => Committed(node, manager))];

        for (int pass = 0; pass < 2; pass++)
        {
            foreach (HLCTimestamp id in ids)
                Assert.Equal(id, (await manager.KeyValues.LookupDurableRecordRouted(id, 1, AnchorKey, ct))!.TransactionId);

            Assert.Equal((4, 4), gateway.TestDurableRecordLookupCacheEntries);
        }
    }

    [Fact]
    public async Task ConcurrentLookupsOfOneRecord_CountItOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNode(ct);
        KahunaManager manager = (KahunaManager)node.Kahuna;
        DurableReplicationGateway gateway = manager.KeyValues.DurableReplicationForTesting;

        const int records = 2_000;
        const int readers = 8;

        HLCTimestamp[] ids = [.. Enumerable.Range(0, records).Select(_ => Committed(node, manager))];

        using Barrier together = new(readers);

        await Task.WhenAll(Enumerable.Range(0, readers).Select(_ => Task.Factory.StartNew(async () =>
        {
            foreach (HLCTimestamp id in ids)
            {
                together.SignalAndWait(ct);
                Assert.Equal(id, (await manager.KeyValues.LookupDurableRecordRouted(id, 1, AnchorKey, ct))!.TransactionId);
            }
        }, ct, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap()));

        Assert.Equal((records, records), gateway.TestDurableRecordLookupCacheEntries);
    }

    private async Task<EmbeddedKahunaNode> StartNode(CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            TimerInitialDelay = TimeSpan.FromMilliseconds(50),
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync(AnchorKey, ct);

        return node;
    }

    private static HLCTimestamp Id(EmbeddedKahunaNode node) =>
        node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

    private static InitializeTransactionCommand Init(HLCTimestamp id) => new(
        id, 1, "lookup-coordinator", AnchorKey, id, new HLCTimestamp(id.N, id.L + 60_000, id.C), 42,
        [new TransactionParticipantRef(AnchorKey, KeyValueDurability.Persistent)], HLCTimestamp.Zero, id);

    private static HLCTimestamp Committed(EmbeddedKahunaNode node, KahunaManager manager)
    {
        HLCTimestamp id = Id(node);
        manager.DurableTransactionRecordStore.Apply(Init(id));
        manager.DurableTransactionRecordStore.Apply(new CommitTransactionCommand(id, 1, 42, id, id));
        return id;
    }
}

using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The range transaction-state gather (the split/merge settle barrier and pre-cutover handoff read
/// path) pages its answer: the number of receipts, records and intents in a busy range has no
/// ceiling, and an unpaged response can exceed the transport's message limit and fail the whole
/// gather. These tests seed more items than one page holds and require the leader-routed gather to
/// return every item exactly once, and the page cut to keep items that share one key on one page.
/// </summary>
public sealed class TestRangeTransactionStatePaging : BaseCluster
{
    private const int Partitions = 6;

    private const string Space = "pg:s";

    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestRangeTransactionStatePaging(ITestOutputHelper outputHelper)
    {
        Microsoft.Extensions.Logging.ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IRaft>(loggerFactory);
        kahunaLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IKahuna>(loggerFactory);
    }

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(
        int partition, IRaft[] rafts, KahunaManager[] kahunas, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partition, ct))
                    return (rafts[i], kahunas[i]);

            await Task.Delay(50, ct);
        }
    }

    /// <summary>
    /// Assembles the cluster and picks a data partition whose leader is another node than the
    /// driver, so the gather exercises the internode page loop rather than the local fast path.
    /// </summary>
    private async Task<(IRaft[] Rafts, KahunaManager[] Kahunas, KahunaManager Driver, KahunaManager SourceLeader, int SourcePartition)> Setup(
        CancellationToken ct)
    {
        (IRaft[] rafts, IKahuna[] kahunas) = await AssembleCluster(
            3, "memory", Partitions, raftLogger, kahunaLogger, replicationFactor: 0);

        KahunaManager[] managers = [.. kahunas.Cast<KahunaManager>()];

        foreach (KahunaManager kahuna in managers)
            kahuna.RegisterKeyRange(Space);

        (IRaft driverRaft, KahunaManager driver) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, managers, ct);

        int sourcePartition = 0;
        for (int partitionId = 1; partitionId <= Partitions; partitionId++)
        {
            if (!await driverRaft.AmILeaderIfHosted(partitionId, ct))
            {
                sourcePartition = partitionId;
                break;
            }
        }

        Assert.NotEqual(0, sourcePartition);

        (_, KahunaManager sourceLeader) = await LeaderOf(sourcePartition, rafts, managers, ct);

        return (rafts, managers, driver, sourceLeader, sourcePartition);
    }

    private static string K(int i) => Space + "/k" + i.ToString("D6");

    private static PreparedIntent MakeIntent(HLCTimestamp txId, string key, string anchorKey) => new(
        TransactionId: txId, Epoch: 1, Key: key, ManifestHash: 42, RecordAnchorKey: anchorKey,
        CommitTimestamp: txId, State: KeyValueState.Set, Value: Encoding.UTF8.GetBytes("v"), Bucket: Space,
        Revision: 1, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: 0, BaseState: KeyValueState.Set,
        RecoveryDeadline: new HLCTimestamp(0, long.MaxValue, 0),
        Resolution: PreparedIntentResolution.Pending);

    /// <summary>
    /// Seeds more intents, records and receipts than one 512-item page holds, then gathers from a
    /// node that does not lead the source partition. Every item must come back exactly once, and a
    /// kinds-restricted gather must carry only the requested kind.
    /// </summary>
    [Fact]
    public async Task Gather_MoreItemsThanOnePage_ReturnsEveryItemOnce()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, _, KahunaManager driver, KahunaManager sourceLeader, int sourcePartition) =
            await Setup(ct);

        const int intentCount = 1200;   // > 2 pages of 512
        const int recordCount = 700;    // > 1 page of 512
        const int receiptCount = 600;   // > 1 page of 512

        List<PreparedIntent> intents = new(intentCount);
        for (int i = 0; i < intentCount; i++)
        {
            HLCTimestamp txId = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
            intents.Add(MakeIntent(txId, K(i), K(i)));
        }

        TransactionRecordStore scratch = new();
        for (int i = 0; i < recordCount; i++)
        {
            HLCTimestamp txId = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
            scratch.Apply(new InitializeTransactionCommand(
                txId, 1, "pg-coord", K(i), txId,
                new HLCTimestamp(0, long.MaxValue, 0), 42,
                [new TransactionParticipantRef(K(i), KeyValueDurability.Persistent)],
                HLCTimestamp.Zero, txId));
        }

        Assert.True(await driver.KeyValues.ImportDurableTransactionStateToPartitionLeaderAsync(
            sourcePartition, new List<TransactionRecord>(scratch.Snapshot()), intents, ct));

        List<CompletionReceiptRecord> receipts = new(receiptCount);
        for (int i = 0; i < receiptCount; i++)
        {
            HLCTimestamp txId = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
            receipts.Add(new CompletionReceiptRecord(txId, K(i), K(i), KeyValueDurability.Persistent));
        }

        Assert.True(await driver.KeyValues.ImportCompletionReceiptsToPartitionLeaderAsync(
            sourcePartition, receipts, ct));

        // The imports replicate through the source partition; wait until the leader's own stores
        // hold everything before the gather reads them.
        await WaitUntilAsync(() =>
            sourceLeader.KeyValues.GetLocalPreparedIntentsForRange(Space + "/", Space + "0").Count == intentCount
            && sourceLeader.KeyValues.GetLocalTransactionRecordsForRange(Space + "/", Space + "0").Count == recordCount
            && sourceLeader.KeyValues.GetLocalCompletionReceiptsForRange(Space + "/", Space + "0").Count == receiptCount,
            timeoutMs: 30_000);

        // The full gather, from a node that does not lead the source partition.
        (bool ok, IReadOnlyCollection<CompletionReceiptRecord> gatheredReceipts,
                IReadOnlyList<TransactionRecord> gatheredRecords, IReadOnlyList<PreparedIntent> gatheredIntents) =
            await driver.KeyValues.GetRangeTransactionStateFromPartitionLeaderAsync(
                sourcePartition, Space + "/", Space + "0", ct);

        Assert.True(ok);

        Assert.Equal(intentCount, gatheredIntents.Count);
        Assert.Equal(intentCount, gatheredIntents.Select(i => i.Key).Distinct().Count());

        Assert.Equal(recordCount, gatheredRecords.Count);
        Assert.Equal(recordCount, gatheredRecords.Select(r => r.RecordAnchorKey).Distinct().Count());

        Assert.Equal(receiptCount, gatheredReceipts.Count);
        Assert.Equal(receiptCount, gatheredReceipts.Select(r => r.Key).Distinct().Count());

        // A kinds-restricted gather (the settle barrier's shape) carries only the intents.
        (bool intentsOk, IReadOnlyCollection<CompletionReceiptRecord> noReceipts,
                IReadOnlyList<TransactionRecord> noRecords, IReadOnlyList<PreparedIntent> onlyIntents) =
            await driver.KeyValues.GetRangeTransactionStateFromPartitionLeaderAsync(
                sourcePartition, Space + "/", Space + "0", KeyValueRangeStateKinds.Intents, ct);

        Assert.True(intentsOk);
        Assert.Equal(intentCount, onlyIntents.Count);
        Assert.Empty(noReceipts);
        Assert.Empty(noRecords);
    }

    /// <summary>
    /// Receipts that share one key must never straddle a page: the resume cursor is strictly-after
    /// by key, so a split key's remaining items would be skipped by the next page. A page may
    /// exceed the cap to keep the key whole.
    /// </summary>
    [Fact]
    public async Task Page_KeyWithManyReceipts_NeverStraddlesPages()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft[] rafts, _, KahunaManager driver, KahunaManager sourceLeader, int sourcePartition) =
            await Setup(ct);

        // Keys a < b < c; b carries three receipts, the page cap is two.
        List<CompletionReceiptRecord> receipts = [];
        foreach ((string key, int copies) in new[] { (Space + "/a", 1), (Space + "/b", 3), (Space + "/c", 1) })
        {
            for (int i = 0; i < copies; i++)
            {
                HLCTimestamp txId = rafts[0].HybridLogicalClock.TrySendOrLocalEvent(rafts[0].GetLocalNodeId());
                receipts.Add(new CompletionReceiptRecord(txId, key, key, KeyValueDurability.Persistent));
            }
        }

        Assert.True(await driver.KeyValues.ImportCompletionReceiptsToPartitionLeaderAsync(
            sourcePartition, receipts, ct));

        await WaitUntilAsync(() =>
            sourceLeader.KeyValues.GetLocalCompletionReceiptsForRange(Space + "/", Space + "0").Count == receipts.Count,
            timeoutMs: 30_000);

        List<CompletionReceiptRecord> collected = [];
        string? cursor = null;

        while (true)
        {
            (bool ok, List<CompletionReceiptRecord> page, _, _, bool hasMore, string? nextCursor) =
                await sourceLeader.KeyValues.GetRangeTransactionStateLocal(
                    sourcePartition, Space + "/", Space + "0", KeyValueRangeStateKinds.Receipts, cursor, 2, ct);

            Assert.True(ok);

            // No key may straddle pages: a key seen on this page must not already be collected.
            foreach (string pageKey in page.Select(r => r.Key).Distinct())
                Assert.DoesNotContain(collected, r => r.Key == pageKey);

            collected.AddRange(page);

            if (!hasMore || nextCursor is null)
                break;

            cursor = nextCursor;
        }

        Assert.Equal(receipts.Count, collected.Count);
        Assert.Equal(3, collected.Count(r => r.Key == Space + "/b"));
    }
}

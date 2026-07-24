using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// A transaction that scans a key range and then point-reads the same keys must see a committed-but-unsettled
/// durable value on both — the scan must not bind the reading transaction to the stale (pre-materialization) base
/// entry. Under deferred settlement a committed value lingers as a prepared intent until it settles into MVCC; a
/// scan that snapshots the raw base would leave the transaction seeing the row as absent (DoesNotExist) or, once
/// the base later materializes, conflicting (Aborted) on the follow-up read. This mirrors CamusDB's UPDATE/DELETE
/// path (scan to locate rows, then batch-read them to mutate) which regressed when deferred settlement shipped on.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestBatchReadAfterUnsettledCommit
{
    private readonly ILoggerFactory loggerFactory;

    public TestBatchReadAfterUnsettledCommit(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static async Task<EmbeddedKahunaNode> StartNode(ILoggerFactory loggerFactory, CancellationToken ct)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            DurableDeferredSettlement = true
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("scan/row-0", ct);
        return node;
    }

    // Commits a batch of rows in one durable transaction; commit returns before deferred settlement materializes
    // them into base MVCC. Returns the committed keys (with a common prefix for range scanning).
    private static async Task<List<string>> DurableInsert(EmbeddedKahunaNode node, string prefix, int rows, CancellationToken ct)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await node.Kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = $"{prefix}-tx",
                Locking = KeyValueTransactionLocking.Pessimistic,
                DecisionDurability = DecisionDurability.Durable,
                Timeout = 10_000
            }, ct);
        Assert.Equal(KeyValueResponseType.Set, startType);

        List<string> keys = [];
        for (int r = 0; r < rows; r++)
        {
            string key = $"{prefix}/row-{r:D3}";
            (KeyValueResponseType w, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                handle.TransactionId, key, Encoding.UTF8.GetBytes($"v{r}"), null, -1,
                KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct,
                coordinatorKey: handle.CoordinatorKey, operationId: TransactionOperationId.NewRandom());
            Assert.Equal(KeyValueResponseType.Set, w);
            keys.Add(key);
        }

        (KeyValueResponseType commit, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);
        Assert.Equal(KeyValueResponseType.Committed, commit);
        return keys;
    }

    [Fact]
    public async Task ScanThenBatchRead_OverUnsettledDurableCommit_SeesEveryRow()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = await StartNode(loggerFactory, ct);

        int misses = 0, aborts = 0, scanShort = 0;

        for (int round = 0; round < 40; round++)
        {
            const int rows = 6;
            string prefix = $"scan/g{round}";
            List<string> keys = await DurableInsert(node, prefix, rows, ct);

            // Reader transaction: scan the range (binds an MVCC snapshot per row), then batch-read the same rows —
            // exactly the shape that regressed. Neither step may lose a committed row.
            HLCTimestamp readerTx = new(1, 3_000_000 + round, 0);

            int seen = 0;
            await foreach ((string _, ReadOnlyKeyValueEntry _) in node.Kahuna.LocateAndScanRange(
                               readerTx, prefix, prefix + "/", true, prefix + "/￿", true, 100,
                               HLCTimestamp.Zero, KeyValueDurability.Persistent, ct))
                seen++;
            if (seen < rows) scanShort++;

            List<(string, long, KeyValueDurability)> readKeys = [];
            foreach (string k in keys) readKeys.Add((k, -1, KeyValueDurability.Persistent));

            List<(KeyValueResponseType, string, KeyValueDurability, ReadOnlyKeyValueEntry?)> batch =
                await node.Kahuna.LocateAndTryGetManyValues(readerTx, HLCTimestamp.Zero, readKeys, ct);

            Assert.Equal(rows, batch.Count);
            foreach ((KeyValueResponseType type, string _, KeyValueDurability _, ReadOnlyKeyValueEntry? entry) in batch)
            {
                if (type == KeyValueResponseType.Aborted) aborts++;
                else if (type != KeyValueResponseType.Get || entry is null) misses++;
            }
        }

        Assert.True(scanShort == 0 && aborts == 0 && misses == 0,
            $"scanShort={scanShort} aborts={aborts} misses={misses} — a committed row was lost across scan/batch-read");
    }
}

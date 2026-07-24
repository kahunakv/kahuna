using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The age backstop that bounds the completion-receipt store on its own terms. The record-driven release can only
/// reclaim a receipt whose transaction record still exists; replaying a committed persistent mutation after that
/// record was reclaimed — which every cold restart and partition leader change does over the retained log —
/// re-creates a receipt no acknowledgement will ever release. These tests prove the orphan is really created, that
/// the record sweep alone leaves it retained forever, and that the age sweep reclaims it once past its window.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestCompletionReceiptRetention
{
    private readonly ILoggerFactory loggerFactory;

    public TestCompletionReceiptRetention(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    // ── store: the age predicate ────────────────────────────────────────────────

    [Fact]
    public void CollectExpired_RemovesOnlyReceiptsPastTheWindow()
    {
        CompletionReceiptStore store = new();
        HLCTimestamp old = new(1, 1_000, 0);
        HLCTimestamp recent = new(1, 9_500, 0);
        store.Record(old, "k1", "anchor", KeyValueDurability.Persistent);
        store.Record(recent, "k2", "anchor", KeyValueDurability.Persistent);

        // At 10_000 the first receipt is 9s old and the second 0.5s: a 5s window collects exactly the first.
        int removed = store.CollectExpired(new HLCTimestamp(1, 10_000, 0), TimeSpan.FromSeconds(5));

        Assert.Equal(1, removed);
        Assert.False(store.Contains(old, "k1", KeyValueDurability.Persistent));
        Assert.True(store.Contains(recent, "k2", KeyValueDurability.Persistent));
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void CollectExpired_NonPositiveTtl_DisablesTheBackstop()
    {
        CompletionReceiptStore store = new();
        HLCTimestamp tx = new(1, 1_000, 0);
        store.Record(tx, "k1", null, KeyValueDurability.Persistent);

        Assert.Equal(0, store.CollectExpired(new HLCTimestamp(1, 10_000, 0), TimeSpan.Zero));
        Assert.Equal(0, store.CollectExpired(new HLCTimestamp(1, 10_000, 0), TimeSpan.FromSeconds(-1)));
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void CollectExpired_IsIdempotent_AndLeavesAFreshRecordAlone()
    {
        CompletionReceiptStore store = new();
        HLCTimestamp tx = new(1, 1_000, 0);
        store.Record(tx, "k1", null, KeyValueDurability.Persistent);

        Assert.Equal(1, store.CollectExpired(new HLCTimestamp(1, 10_000, 0), TimeSpan.FromSeconds(5)));
        Assert.Equal(0, store.CollectExpired(new HLCTimestamp(1, 10_000, 0), TimeSpan.FromSeconds(5)));

        // A receipt re-recorded by a later replay is a fresh entry, and is only collected on its own age.
        store.Record(new HLCTimestamp(1, 9_900, 0), "k1", null, KeyValueDurability.Persistent);
        Assert.Equal(0, store.CollectExpired(new HLCTimestamp(1, 10_000, 0), TimeSpan.FromSeconds(5)));
        Assert.Equal(1, store.Count);
    }

    // ── end-to-end: the orphan a log replay creates ─────────────────────────────

    [Fact]
    public async Task ReplayedCommit_AfterRecordReclaimed_OrphansReceipt_CollectedByAgeBackstop()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            // Tiny windows so both the record sweep and the age backstop are eligible immediately. The periodic
            // tick is a minute away, so every sweep in this test is one we drive explicitly.
            TransactionOutcomeRetentionTtl = TimeSpan.FromMilliseconds(1),
            CompletionReceiptRetentionTtl = TimeSpan.FromMilliseconds(1)
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("orphan/row-1", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `orphan/row-1` 'v1' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count > 0);
        await WaitUntil(() => kahuna.CompletionReceiptStore.Count > 0);

        // Reclaim the record the ordinary way; that releases the receipts it owns.
        await Task.Delay(50, ct);
        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count == 0);
        await WaitUntil(() => kahuna.CompletionReceiptStore.Count == 0);

        // Now replay that transaction's committed mutation through the real restore path, exactly as a cold
        // restart or a partition leader change replays the retained log. The receipt comes back — but its
        // record is already gone, so no coordinator acknowledgement will ever release it again.
        HLCTimestamp replayedTxId = new(0, 500, 0);
        Assert.True(await ReplayCommittedSet(node, kahuna, "orphan/row-1", replayedTxId));
        Assert.Equal(1, kahuna.CompletionReceiptStore.Count);

        // The record sweep cannot touch it: it walks transaction records, and this receipt has none. Without an
        // age bound this is the receipt that stays for the node's lifetime.
        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);
        Assert.Equal(1, kahuna.CompletionReceiptStore.Count);

        // The age backstop collects it.
        kahuna.KeyValues.CollectExpiredCompletionReceipts();
        Assert.Equal(0, kahuna.CompletionReceiptStore.Count);

        // Metadata only: the committed value is untouched.
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "orphan/row-1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal("v1"u8.ToArray(), entry!.Value);
    }

    [Fact]
    public async Task LiveReceipt_WithinBackstopWindow_IsRetained()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 4,
            TransactionOutcomeRetentionTtl = TimeSpan.FromMinutes(30),
            CompletionReceiptRetentionTtl = TimeSpan.FromMinutes(30)
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("keep-receipt/row-1", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `keep-receipt/row-1` 'v1' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        await WaitUntil(() => kahuna.CompletionReceiptStore.Count > 0);
        int receiptsBefore = kahuna.CompletionReceiptStore.Count;

        kahuna.KeyValues.CollectExpiredCompletionReceipts();

        // Inside the window the backstop must not preempt the acknowledgement-driven release — a live receipt
        // still has to answer a re-delivered commit with Committed rather than an ambiguous MustRetry.
        Assert.Equal(receiptsBefore, kahuna.CompletionReceiptStore.Count);
    }

    // Replays one committed persistent SET through the Raft-log restore path, which is what re-records a
    // completion receipt on a cold restart or partition leader change.
    private static async Task<bool> ReplayCommittedSet(EmbeddedKahunaNode node, KahunaManager kahuna, string key, HLCTimestamp transactionId)
    {
        KeyValueMessage message = new()
        {
            Type = (int)KeyValueRequestType.TrySet,
            Key = key,
            Value = Google.Protobuf.ByteString.CopyFrom("v1"u8.ToArray()),
            Revision = 0,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            RecordAnchorKey = key
        };

        int partitionId = kahuna.KeyValues.LocateRange(key).PartitionId;

        return await node.Kahuna.OnLogRestored(partitionId, new RaftLog
        {
            LogType = ReplicationTypes.KeyValues,
            LogData = ReplicationSerializer.Serialize(message)
        });
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 5000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(10);
        }
        Assert.True(predicate(), "condition not met in time");
    }
}

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
/// Retention GC of durable-2PC metadata. The store-level tests prove the purge transition removes a terminal
/// record and round-trips through replication; the end-to-end test proves the periodic sweep, once the retention
/// window has elapsed, reclaims a committed transaction's canonical record <b>and</b> releases its participant
/// completion receipts — so neither store grows for the node's lifetime.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTransactionRecordGc
{
    private readonly ILoggerFactory loggerFactory;

    public TestTransactionRecordGc(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);
    private static readonly HLCTimestamp TxId = Ts(1000);
    private const long Epoch = 1;
    private const string Anchor = "acct/42";

    private static IReadOnlyList<TransactionParticipantRef> Manifest() =>
        [new(Anchor, KeyValueDurability.Persistent)];

    private static InitializeTransactionCommand Init() =>
        new(TxId, Epoch, "coord/1", Anchor, Ts(2000), Ts(9000),
            TransactionManifest.ComputeHash(TxId, Epoch, Anchor, Ts(2000), Manifest()),
            Manifest(), OpId: Ts(500), CreatedAt: Ts(400));

    // ── store: purge apply + replication round-trip ──────────────────────────────

    [Fact]
    public void Store_PurgeCommittedRecord_RemovesIt()
    {
        TransactionRecordStore store = new();
        InitializeTransactionCommand init = Init();
        store.Apply(init);
        store.Apply(new CommitTransactionCommand(TxId, Epoch, init.ManifestHash, Ts(500), Ts(2100)));
        Assert.NotNull(store.Get(TxId, Epoch));

        TransactionRecordApplyResult result = store.Apply(new PurgeTransactionCommand(TxId, Epoch));

        Assert.Equal(TransactionApplyOutcome.Removed, result.Outcome);
        Assert.Null(store.Get(TxId, Epoch));
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public void Store_PurgeUndecidedRecord_Rejected_Retained()
    {
        TransactionRecordStore store = new();
        store.Apply(Init());

        TransactionRecordApplyResult result = store.Apply(new PurgeTransactionCommand(TxId, Epoch));

        Assert.Equal(TransactionApplyOutcome.Rejected, result.Outcome);
        Assert.NotNull(store.Get(TxId, Epoch)); // an in-flight record is never dropped
    }

    [Fact]
    public void Store_PurgeDelta_Replicates_RemovesRecordOnEveryReplica()
    {
        // Two replicas apply the same init+commit; a purge delta replicated to both converges them to removed.
        TransactionRecordStore a = new();
        TransactionRecordStore b = new();
        InitializeTransactionCommand init = Init();
        foreach (TransactionRecordStore s in new[] { a, b })
        {
            s.Apply(init);
            s.Apply(new CommitTransactionCommand(TxId, Epoch, init.ManifestHash, Ts(500), Ts(2100)));
        }

        byte[] delta = TransactionRecordStore.SerializeDelta([new PurgeTransactionCommand(TxId, Epoch)]);
        Kommander.Data.RaftLog log = new() { LogType = Kahuna.Server.Replication.ReplicationTypes.TransactionRecord, LogData = delta };

        Assert.True(a.Replicate(0, log));
        Assert.True(b.Restore(0, log)); // restore path applies the same purge on a restarting replica

        Assert.Equal(0, a.Count);
        Assert.Equal(0, b.Count);
    }

    // ── end-to-end: sweep reclaims record + receipts after retention ─────────────

    [Fact]
    public async Task DurableCommit_AfterRetention_Sweep_ReclaimsRecordAndReleasesReceipts()
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
            // A tiny retention window so the record is eligible for GC almost immediately after commit.
            TransactionOutcomeRetentionTtl = TimeSpan.FromMilliseconds(1)
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("gc/row-1", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        // A persistent script transaction takes the durable path: it writes a canonical record and, via
        // materialization of the committed values, a completion receipt per persistent key.
        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `gc/row-1` 'v1' SET `gc/row-2` 'v2' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count > 0);
        await WaitUntil(() => kahuna.CompletionReceiptStore.Count > 0); // receipts written for the two persistent keys
        int receiptsBefore = kahuna.CompletionReceiptStore.Count;
        Assert.True(receiptsBefore > 0);

        // Let the retention window elapse, then drive the sweep directly (rather than waiting a collection tick).
        await Task.Delay(100, ct);
        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);

        // The terminal record was purged and its participant receipts released — both stores back to their floor.
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count == 0);
        await WaitUntil(() => kahuna.CompletionReceiptStore.Count == 0);

        // The committed values are still readable — GC reclaims metadata only, never the data.
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.Kahuna.LocateAndTryGetValue(
            HLCTimestamp.Zero, "gc/row-1", -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct);
        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal("v1"u8.ToArray(), entry!.Value);
    }

    [Fact]
    public async Task DurableCommit_WithinRetention_Sweep_RetainsRecordAndReceipts()
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
            // A long retention window: the record is NOT yet eligible when we sweep, so it must be retained.
            TransactionOutcomeRetentionTtl = TimeSpan.FromMinutes(30)
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("keep/row-1", ct);

        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
            Encoding.UTF8.GetBytes("BEGIN SET `keep/row-1` 'v1' COMMIT END"), null, null);
        Assert.Equal(KeyValueResponseType.Set, result.Type);

        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count > 0);
        // Wait for the receipt too, not just the record: it is written on the committed value's apply, which lands
        // after the record. Capturing the count before then would compare against a not-yet-written receipt.
        await WaitUntil(() => kahuna.CompletionReceiptStore.Count > 0);
        int recordsBefore = kahuna.DurableTransactionRecordStore.Count;
        int receiptsBefore = kahuna.CompletionReceiptStore.Count;

        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);

        // Within the retention window nothing is reclaimed — a live receipt must still answer a re-delivered commit.
        Assert.Equal(recordsBefore, kahuna.DurableTransactionRecordStore.Count);
        Assert.Equal(receiptsBefore, kahuna.CompletionReceiptStore.Count);
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

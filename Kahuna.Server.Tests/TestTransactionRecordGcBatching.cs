using System.Collections.Concurrent;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The retention GC releases every reclaimed record's completion receipts in one replicated forget per
/// participant partition, rather than one per record. These tests pin the cost model — a pass costs round trips
/// proportional to the partitions it touches, not the records it reclaims — and the partial-failure behaviour
/// that batching has to preserve: a partition whose forget was not durable retains only the records that depend
/// on it, while records whose receipts all landed elsewhere still purge in the same pass.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestTransactionRecordGcBatching
{
    private readonly ILoggerFactory loggerFactory;

    public TestTransactionRecordGcBatching(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private const int TransactionCount = 24;

    // Distinct key spaces the transactions are spread over. Well below TransactionCount so several records share
    // each participant partition — the whole point being that they collapse into one forget per partition.
    private const int KeySpaceCount = 4;

    [Fact]
    public async Task Sweep_ReleasesReceipts_OncePerPartition_NotOncePerRecord()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        List<string> keys = await CommitTransactionsAsync(node, TransactionCount, "batch");
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count >= TransactionCount);

        HashSet<int> participantPartitions = [];
        foreach (string key in keys)
            participantPartitions.Add(kahuna.KeyValues.LocateRange(key).PartitionId);

        // Guard the premise: with everything on one partition the count below would be trivially satisfiable.
        Assert.True(participantPartitions.Count > 1, "test needs the keys to span more than one partition");
        Assert.True(participantPartitions.Count < TransactionCount, "test needs partitions to be shared by records");

        // The hook never faults; it just counts, because it is invoked exactly once per replicated forget.
        ConcurrentBag<int> forgetCalls = [];
        kahuna.KeyValues.ReplicateReceiptForgetFault = partitionId =>
        {
            forgetCalls.Add(partitionId);
            return false;
        };

        await Task.Delay(50, ct);
        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);

        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count == 0);
        await WaitUntil(() => kahuna.CompletionReceiptStore.Count == 0);

        // The whole backlog was released in one forget per participant partition. Before batching this pass cost
        // one round trip per record, so the count would have been TransactionCount or more.
        Assert.NotEmpty(forgetCalls);
        Assert.Equal(participantPartitions.Count, forgetCalls.Count);
        Assert.True(forgetCalls.Count < TransactionCount,
            $"expected fewer forget replications than records; got {forgetCalls.Count} for {TransactionCount} records");

        // Each partition was asked exactly once — the batch per partition is a single replication, not a retry loop.
        Assert.Equal(forgetCalls.Count, forgetCalls.Distinct().Count());
    }

    [Fact]
    public async Task Sweep_PartitionForgetFails_RetainsOnlyDependentRecords_AndPurgesTheRest()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(ct);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        List<string> keys = await CommitTransactionsAsync(node, TransactionCount, "partial");
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count >= TransactionCount);

        // Pick one participant partition to fail, and count how many records depend on it. Each transaction here
        // writes a single key, so a record depends on exactly the partition its key routes to.
        Dictionary<int, int> recordsByPartition = [];
        foreach (string key in keys)
        {
            int partitionId = kahuna.KeyValues.LocateRange(key).PartitionId;
            recordsByPartition[partitionId] = recordsByPartition.GetValueOrDefault(partitionId) + 1;
        }

        Assert.True(recordsByPartition.Count > 1, "test needs the keys to span more than one partition");

        (int faultedPartition, int retainedCount) = recordsByPartition.OrderByDescending(p => p.Value).First();
        kahuna.KeyValues.ReplicateReceiptForgetFault = partitionId => partitionId == faultedPartition;

        await Task.Delay(50, ct);
        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);

        // Exactly the records whose receipts live on the faulted partition are retained; every other record was
        // purged in the same pass rather than being held back by its neighbour's failure.
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count == retainedCount);
        Assert.Equal(retainedCount, kahuna.DurableTransactionRecordStore.Count);

        foreach (TransactionRecord record in kahuna.DurableTransactionRecordStore.Snapshot())
            Assert.Equal(faultedPartition, kahuna.KeyValues.LocateRange(record.RecordAnchorKey).PartitionId);

        // Once the partition can forget again, the retained records drain on the next pass.
        kahuna.KeyValues.ReplicateReceiptForgetFault = null;
        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);

        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count == 0);
        await WaitUntil(() => kahuna.CompletionReceiptStore.Count == 0);
    }

    [Fact]
    public async Task Sweep_CapBoundsSelectedRecords_AndSuccessivePassesDrainTheBacklog()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = await StartNodeAsync(ct, gcMaxPerPass: 5);
        KahunaManager kahuna = (KahunaManager)node.Kahuna;

        await CommitTransactionsAsync(node, TransactionCount, "capped");
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count >= TransactionCount);
        int before = kahuna.DurableTransactionRecordStore.Count;

        await Task.Delay(50, ct);
        await kahuna.KeyValues.CollectDurableTransactionRecords(ct);

        // One pass reclaims at most the cap, so the backlog shrinks by five rather than draining outright.
        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count == before - 5);
        Assert.Equal(before - 5, kahuna.DurableTransactionRecordStore.Count);

        // Successive passes drain the rest — the cap paces the sweep, it does not strand records.
        for (int pass = 0; pass < 10 && kahuna.DurableTransactionRecordStore.Count > 0; pass++)
            await kahuna.KeyValues.CollectDurableTransactionRecords(ct);

        await WaitUntil(() => kahuna.DurableTransactionRecordStore.Count == 0);
        await WaitUntil(() => kahuna.CompletionReceiptStore.Count == 0);
    }

    private async Task<EmbeddedKahunaNode> StartNodeAsync(CancellationToken ct, int gcMaxPerPass = 4096)
    {
        EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 8,
            // Everything is immediately eligible; the periodic tick is a minute out, so each sweep is one we drive.
            TransactionOutcomeRetentionTtl = TimeSpan.FromMilliseconds(1),
            // Long enough that the age backstop never fires here — these tests are about the record-driven release.
            CompletionReceiptRetentionTtl = TimeSpan.FromMinutes(30),
            DurableRecordGcMaxPerPass = gcMaxPerPass
        }, loggerFactory);

        await node.StartAsync(ct);
        return node;
    }

    // Commits one single-key durable transaction per key, spread over several key spaces. Default routing hashes
    // the key space (the part before the last '/') to a data partition, so distinct spaces are what put the pass's
    // receipts on more than one participant partition — which is what makes "one forget per partition"
    // distinguishable from "one forget per record", and what lets one partition's failure be isolated. Each
    // transaction writes a single key, so a record depends on exactly the partition its key routes to.
    private static async Task<List<string>> CommitTransactionsAsync(EmbeddedKahunaNode node, int count, string prefix)
    {
        List<string> keys = [];

        for (int i = 0; i < count; i++)
        {
            string key = $"{prefix}-{i % KeySpaceCount}/row-{i:D3}";
            await node.WaitForLeaderForKeyAsync(key, CancellationToken.None);

            KeyValueTransactionResult result = await node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"BEGIN SET `{key}` 'v{i}' COMMIT END"), null, null);
            Assert.Equal(KeyValueResponseType.Set, result.Type);

            keys.Add(key);
        }

        return keys;
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 10_000)
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

using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues.Writes;
using Kommander;
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Deterministic guard that the durable-intent path enqueues records through the shared scheduler without an
/// await-spanning per-partition gate, so concurrent transactions to the same partition coalesce
/// into shared proposals instead of serializing one Raft call at a time. Many concurrent durable transactions on one
/// partition must issue far fewer <c>ReplicateEntries</c> proposals than they would if each transaction's records
/// (init, prepare, decision, resolution) were proposed separately.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableSchedulerCoalescing
{
    private readonly ILoggerFactory loggerFactory;

    public TestDurableSchedulerCoalescing(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private sealed class CountingExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        public long Calls;

        public CountingExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        public Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);
            return inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    [Fact]
    public async Task ConcurrentDurableTransactions_CoalesceIntoFewerProposals()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        CountingExecutor? counter = null;
        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            ReadIOThreads = 1,
            WriteIOThreads = 1,
            PartitionExecutorPoolSize = 1,
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1,
            KeyValueWriteLingerMs = 5,
            WriteBatchExecutorDecorator = inner => counter = new CountingExecutor(inner)
        }, loggerFactory);
        await node.StartAsync(ct);
        await node.WaitForLeaderForKeyAsync("coa/data/k0", ct);

        const int concurrency = 32;

        Task[] tasks = new Task[concurrency];
        for (int i = 0; i < concurrency; i++)
        {
            string key = $"coa/data/k{i}"; // distinct keys, one bucket → one partition
            tasks[i] = node.Kahuna.TryExecuteTransactionScript(
                Encoding.UTF8.GetBytes($"BEGIN SET `{key}` 'v' COMMIT END"), null, null);
        }
        await Task.WhenAll(tasks);

        // Let the deferred resolutions complete so all proposals (including resolution) are counted.
        await Task.Delay(1500, ct);

        long proposals = counter!.Calls;

        // Serialized (one Raft call per record) would be at least ~3 barriers × 32 transactions ≈ 96 proposals.
        // Coalesced, concurrent transactions share proposals, so the total is a fraction of the transaction count.
        Assert.True(proposals < concurrency, $"expected coalescing (< {concurrency} proposals for {concurrency} concurrent txns), got {proposals}");
    }
}

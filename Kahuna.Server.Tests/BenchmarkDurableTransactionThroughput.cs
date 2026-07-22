using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kommander;
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Measures same-partition coalescing for concurrent one-key durable transactions. It drives real transactions
/// through the coordinator on an embedded node with a RocksDB WAL and synchronous writes, then counts the
/// <c>ReplicateEntries</c> proposals issued by the shared partition write scheduler. It prints a report at several
/// concurrency levels and asserts the structural coalescing trend without imposing a machine-speed threshold.
/// </summary>
[Collection("ClusterTests")]
public sealed class BenchmarkDurableTransactionThroughput
{
    private readonly ITestOutputHelper output;
    private readonly ILoggerFactory loggerFactory;

    public BenchmarkDurableTransactionThroughput(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    /// <summary>Counts the proposals (ReplicateEntries calls) and total entries the scheduler issues.</summary>
    private sealed class CountingExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;
        public long Calls;
        public long Entries;

        public CountingExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        public Task<RaftReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries)
        {
            Interlocked.Increment(ref Calls);
            Interlocked.Add(ref Entries, entries.Count);
            return inner.ReplicateAsync(partitionId, entries);
        }
    }

    private static string F(double v) => v.ToString("F2", CultureInfo.InvariantCulture);

    [Fact]
    [Trait("Category", "Performance")]
    public async Task Benchmark_DurableTransactionCoalescing_ByConcurrency()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        output.WriteLine("=== One-key durable transactions (rocksdb WAL, sync writes) ===");
        output.WriteLine($"{"concurrency",-12}{"txns",-8}{"proposals",-12}{"prop/txn",-10}{"entries",-10}{"ent/prop",-10}{"txns/s",-10}");
        List<double> proposalsPerTransaction = new();

        foreach (int concurrency in new[] { 1, 4, 16, 64 })
        {
            CountingExecutor? counter = null;
            string walPath = Path.Combine(Path.GetTempPath(), "kahuna-durbench-" + Guid.NewGuid().ToString("N"));

            await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
            {
                Storage = "memory",
                WalStorage = "rocksdb",
                WalPath = walPath,
                WalSyncWrites = true,
                InitialPartitions = 1,
                KeyValueWriteLingerMs = 5,
                WriteBatchExecutorDecorator = inner => counter = new CountingExecutor(inner)
            }, loggerFactory);
            await node.StartAsync(ct);
            await node.WaitForLeaderForKeyAsync("bench/data/k0", ct);

            const int wavesWarmup = 3;
            const int wavesMeasured = 10;
            int keyId = 0;

            async Task RunWave()
            {
                Task[] tasks = new Task[concurrency];
                for (int c = 0; c < concurrency; c++)
                {
                    string key = $"bench/data/k{Interlocked.Increment(ref keyId)}";
                    tasks[c] = node.Kahuna.TryExecuteTransactionScript(
                        Encoding.UTF8.GetBytes($"BEGIN SET `{key}` 'v' COMMIT END"), null, null);
                }
                await Task.WhenAll(tasks);
            }

            for (int w = 0; w < wavesWarmup; w++)
                await RunWave();

            // Let warmup work drain so it does not affect the measured proposal count.
            await Task.Delay(800, ct);

            long callsBefore = counter!.Calls;
            long entriesBefore = counter.Entries;
            Stopwatch sw = Stopwatch.StartNew();

            for (int w = 0; w < wavesMeasured; w++)
                await RunWave();

            sw.Stop();

            // Allow any scheduler work still completing after the client result to be counted.
            await Task.Delay(1500, ct);

            long proposals = counter.Calls - callsBefore;
            long entries = counter.Entries - entriesBefore;
            int txns = concurrency * wavesMeasured;
            double perTxn = proposals / (double)txns;
            double entPerProp = proposals == 0 ? 0 : entries / (double)proposals;
            double txnsPerSec = txns / sw.Elapsed.TotalSeconds;
            long records = ((KahunaManager)node.Kahuna).DurableTransactionRecordStore.Count;
            proposalsPerTransaction.Add(perTxn);

            output.WriteLine($"{concurrency,-12}{txns,-8}{proposals,-12}{F(perTxn),-10}{entries,-10}{F(entPerProp),-10}{F(txnsPerSec),-10}  [totalCalls={counter.Calls} durableRecords={records}]");
        }

        output.WriteLine("");
        output.WriteLine("Proposal calls per transaction should fall as concurrency rises because records at the");
        output.WriteLine("same protocol barrier can share one proposal.");

        Assert.True(proposalsPerTransaction[^1] < proposalsPerTransaction[0]);
    }
}

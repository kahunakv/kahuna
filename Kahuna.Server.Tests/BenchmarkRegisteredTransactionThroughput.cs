using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Kahuna;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

[Collection("ClusterTests")]
public sealed class BenchmarkRegisteredTransactionThroughput
{
    private readonly ITestOutputHelper output;
    private readonly ILoggerFactory loggerFactory;

    public BenchmarkRegisteredTransactionThroughput(ITestOutputHelper output)
    {
        this.output = output;
        loggerFactory = NullLoggerFactory.Instance;
    }

    private sealed class CountingExecutor : IPartitionBatchExecutor
    {
        private readonly IPartitionBatchExecutor inner;

        public CountingExecutor(IPartitionBatchExecutor inner) => this.inner = inner;

        public long Calls;
        public long Entries;

        public Task<RaftBatchReplicationResult> ReplicateAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref Calls);
            Interlocked.Add(ref Entries, entries.Count);
            return inner.ReplicateAsync(partitionId, entries, cancellationToken);
        }
    }

    [Fact]
    [Trait("Category", "Performance")]
    public async Task RegisteredSetManyCommit_ReportsPersistentLatencyAndBatching()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        CountingExecutor? counter = null;
        string rootPath = Path.Combine(
            Path.GetTempPath(), "kahuna-registered-benchmark-" + Guid.NewGuid().ToString("N"));

        try
        {
            await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
            {
                Storage = "rocksdb",
                StoragePath = Path.Combine(rootPath, "data"),
                WalStorage = "rocksdb",
                WalPath = Path.Combine(rootPath, "wal"),
                WalSyncWrites = true,
                InitialPartitions = 1,
                WriteBatchExecutorDecorator = inner => counter = new CountingExecutor(inner)
            }, loggerFactory);
            await node.StartAsync(cancellationToken);
            await node.WaitForLeaderForKeyAsync("registered-benchmark/0/0", cancellationToken);

            KahunaManager kahuna = (KahunaManager)node.Kahuna;
            output.WriteLine("=== Registered persistent SetMany commits (RocksDB KV + sync RocksDB WAL) ===");
            output.WriteLine(
                $"{"keys",-6}{"conc",-6}{"txns",-6}{"p50ms",-10}{"p95ms",-10}{"p99ms",-10}" +
                $"{"rows/s",-10}{"prop/tx",-10}{"entries",-10}{"intents",-8}");

            foreach (int keyCount in new[] { 1, 10, 100 })
            {
                foreach ((int concurrency, int transactionCount) in new[] { (1, 100), (8, 104) })
                {
                    await CommitSetMany(
                        kahuna, keyCount, $"warmup-{concurrency}", cancellationToken);

                    // The warm-up's own settlement runs after its commit returned; let it finish so its Raft work
                    // is not charged to the measured round.
                    await WaitForSettledIntents(kahuna, cancellationToken);

                    long callsBefore = counter!.Calls;
                    long entriesBefore = counter.Entries;
                    ConcurrentBag<double> commitLatenciesMs = new();
                    Stopwatch workload = Stopwatch.StartNew();

                    int completed = 0;
                    while (completed < transactionCount)
                    {
                        int waveSize = Math.Min(concurrency, transactionCount - completed);
                        Task<double>[] wave = Enumerable.Range(0, waveSize)
                            .Select(i => CommitSetMany(
                                kahuna,
                                keyCount,
                                $"measured-{concurrency}-{completed + i}",
                                cancellationToken))
                            .ToArray();
                        double[] latencies = await Task.WhenAll(wave);
                        foreach (double latency in latencies)
                            commitLatenciesMs.Add(latency);
                        completed += waveSize;
                    }

                    workload.Stop();

                    // A commit reports success as soon as its decision is durable; the intents it prepared are
                    // materialized, applied and removed afterwards, off the caller's critical path. Drain that tail
                    // before reading the counters — the latency window is already closed, and counting first would
                    // both credit the round with less Raft work than it caused and read the intent store mid-flight.
                    // The leak check is that the store drains, not that it is empty the instant a commit returns.
                    int unresolvedIntents = await WaitForSettledIntents(kahuna, cancellationToken);
                    long proposals = counter.Calls - callsBefore;
                    long entries = counter.Entries - entriesBefore;
                    double[] sortedLatencies = commitLatenciesMs.Order().ToArray();
                    double rowsPerSecond = transactionCount / workload.Elapsed.TotalSeconds;

                    output.WriteLine(
                        $"{keyCount,-6}{concurrency,-6}{transactionCount,-6}" +
                        $"{Percentile(sortedLatencies, 0.50).ToString("F2", CultureInfo.InvariantCulture),-10}" +
                        $"{Percentile(sortedLatencies, 0.95).ToString("F2", CultureInfo.InvariantCulture),-10}" +
                        $"{Percentile(sortedLatencies, 0.99).ToString("F2", CultureInfo.InvariantCulture),-10}" +
                        $"{rowsPerSecond.ToString("F2", CultureInfo.InvariantCulture),-10}" +
                        $"{(proposals / (double)transactionCount).ToString("F2", CultureInfo.InvariantCulture),-10}" +
                        $"{entries,-10}{unresolvedIntents,-8}");

                    Assert.Equal(transactionCount, sortedLatencies.Length);
                    Assert.All(sortedLatencies, latency => Assert.True(double.IsFinite(latency) && latency >= 0));
                    Assert.Equal(0, unresolvedIntents);
                    Assert.True(entries >= (long)transactionCount * keyCount);
                    if (keyCount > 1)
                    {
                        long serializedProposalCount = (long)transactionCount * (keyCount + 4);
                        Assert.True(
                            proposals < serializedProposalCount,
                            $"expected fewer than {serializedProposalCount} serialized proposals, got {proposals}");
                    }
                }
            }
        }
        finally
        {
            if (Directory.Exists(rootPath))
                Directory.Delete(rootPath, recursive: true);
        }
    }

    private static async Task<double> CommitSetMany(
        KahunaManager kahuna,
        int keyCount,
        string transactionName,
        CancellationToken cancellationToken)
    {
        string transactionPrefix = $"registered-benchmark/{keyCount}/{transactionName}/{Guid.NewGuid():N}";
        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = transactionPrefix,
                Locking = KeyValueTransactionLocking.Pessimistic,
                AsyncRelease = true,
                Timeout = 30_000,
                DecisionDurability = DecisionDurability.BestEffort
            }, cancellationToken);
        Assert.Equal(KeyValueResponseType.Set, startType);

        List<KahunaSetKeyValueRequestItem> items = new(keyCount);
        for (int keyNumber = 0; keyNumber < keyCount; keyNumber++)
        {
            items.Add(new()
            {
                TransactionId = handle.TransactionId,
                Key = $"{transactionPrefix}/{keyNumber}",
                Value = "v"u8.ToArray(),
                ExpiresMs = 0,
                Flags = KeyValueFlags.None,
                Durability = KeyValueDurability.Persistent
            });
        }

        List<KahunaSetKeyValueResponseItem> writes = await kahuna.LocateAndTrySetManyKeyValue(
            items, cancellationToken, handle.CoordinatorKey, TransactionOperationId.NewRandom());
        Assert.All(writes, write => Assert.Equal(KeyValueResponseType.Set, write.Type));

        Stopwatch commit = Stopwatch.StartNew();
        (KeyValueResponseType commitType, _) = await kahuna.LocateAndCommitTransaction(handle, cancellationToken);
        commit.Stop();
        Assert.Equal(KeyValueResponseType.Committed, commitType);
        return commit.Elapsed.TotalMilliseconds;
    }

    /// <summary>
    /// Polls the durable prepared-intent store until it drains, returning the final count so the caller asserts on
    /// it. A count that never reaches zero is returned as-is and fails the caller's assertion — the deadline bounds
    /// the wait without hiding a genuine leak.
    /// </summary>
    private static async Task<int> WaitForSettledIntents(KahunaManager kahuna, CancellationToken cancellationToken)
    {
        long deadline = Environment.TickCount64 + 30_000;

        int remaining = kahuna.DurablePreparedIntentStore.Count;
        while (remaining > 0 && Environment.TickCount64 < deadline)
        {
            await Task.Delay(25, cancellationToken);
            remaining = kahuna.DurablePreparedIntentStore.Count;
        }

        return remaining;
    }

    private static double Percentile(IReadOnlyList<double> sortedValues, double percentile)
    {
        int index = Math.Max(0, (int)Math.Ceiling(sortedValues.Count * percentile) - 1);
        return sortedValues[index];
    }
}

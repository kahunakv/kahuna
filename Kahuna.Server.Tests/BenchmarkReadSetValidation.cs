using System.Diagnostics;
using System.Globalization;
using Kahuna;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Measures what a transaction pays at commit time for the size of its read set.
///
/// An optimistic commit validates its read dependencies in two passes: existence/revision validation, which is
/// already issued as one batched many-key read, and the concurrent-writer probe, which is issued as one routed
/// call per read key. This benchmark quantifies the second pass — both in isolation against the batched shape it
/// would take if it were grouped by owning partition, and as a share of a real commit at read-set sizes of
/// 1, 10, 100 and 1,000 folded dependencies.
///
/// Reported, not asserted: absolute latencies are machine-dependent. The assertions cover only that every
/// measured path returned the expected responses and that the numbers are well-formed, so the report stays
/// meaningful without becoming a speed test.
/// </summary>
[Collection("ClusterTests")]
public sealed class BenchmarkReadSetValidation : BaseCluster
{
    private static readonly int[] ReadSetSizes = [1, 10, 100, 1000];

    private readonly ITestOutputHelper output;
    private readonly ILoggerFactory loggerFactory;
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public BenchmarkReadSetValidation(ITestOutputHelper output)
    {
        this.output = output;
        loggerFactory = NullLoggerFactory.Instance;
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static string F(double v) => v.ToString("F3", CultureInfo.InvariantCulture);

    /// <summary>
    /// Isolates the commit-time concurrent-writer probe from everything else a commit does: it drives the same
    /// per-key fan-out the coordinator issues, against the same keys read as one batched many-key call. The
    /// batched arm is the shape a partition-grouped probe would take, so the gap between the two arms bounds
    /// what grouping the probe could recover.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public async Task WriteIntentProbe_PerKeyFanoutVersusBatchedShape()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(cancellationToken);
        await node.WaitForLeaderForKeyAsync("readset-probe/seed", cancellationToken);

        IKahuna kahuna = node.Kahuna;

        output.WriteLine("=== Commit-time read-set probe, isolated (memory KV, memory WAL, 1 partition) ===");
        output.WriteLine(
            $"{"keys",-8}{"perKey.p50ms",-14}{"perKey.p99ms",-14}{"batched.p50ms",-15}{"batched.p99ms",-15}" +
            $"{"ratio.p50",-11}{"perKey.us/key",-15}{"perKey.KB",-12}{"batched.KB",-12}");

        foreach (int readSetSize in ReadSetSizes)
        {
            string prefix = $"readset-probe/{readSetSize}";
            List<(string key, long revision, KeyValueDurability durability)> keys =
                await SeedKeys(kahuna, prefix, readSetSize, cancellationToken);

            HLCTimestamp probeTransactionId = await StartAndAbandonTransaction(kahuna, prefix, cancellationToken);

            // Warm the routing tables, the actor mailboxes and the JIT before measuring either arm.
            for (int warmup = 0; warmup < 3; warmup++)
            {
                await ProbePerKey(kahuna, probeTransactionId, keys, cancellationToken);
                await ProbeBatched(kahuna, probeTransactionId, keys, cancellationToken);
            }

            const int iterations = 20;
            double[] perKeyMs = new double[iterations];
            double[] batchedMs = new double[iterations];

            long perKeyAllocated = GC.GetTotalAllocatedBytes(precise: true);
            for (int i = 0; i < iterations; i++)
            {
                Stopwatch sw = Stopwatch.StartNew();
                await ProbePerKey(kahuna, probeTransactionId, keys, cancellationToken);
                sw.Stop();
                perKeyMs[i] = sw.Elapsed.TotalMilliseconds;
            }
            perKeyAllocated = GC.GetTotalAllocatedBytes(precise: true) - perKeyAllocated;

            long batchedAllocated = GC.GetTotalAllocatedBytes(precise: true);
            for (int i = 0; i < iterations; i++)
            {
                Stopwatch sw = Stopwatch.StartNew();
                await ProbeBatched(kahuna, probeTransactionId, keys, cancellationToken);
                sw.Stop();
                batchedMs[i] = sw.Elapsed.TotalMilliseconds;
            }
            batchedAllocated = GC.GetTotalAllocatedBytes(precise: true) - batchedAllocated;

            double[] perKeySorted = perKeyMs.Order().ToArray();
            double[] batchedSorted = batchedMs.Order().ToArray();
            double perKeyP50 = Percentile(perKeySorted, 0.50);
            double batchedP50 = Percentile(batchedSorted, 0.50);

            output.WriteLine(
                $"{readSetSize,-8}{F(perKeyP50),-14}{F(Percentile(perKeySorted, 0.99)),-14}" +
                $"{F(batchedP50),-15}{F(Percentile(batchedSorted, 0.99)),-15}" +
                $"{F(batchedP50 <= 0 ? 0 : perKeyP50 / batchedP50),-11}" +
                $"{F(perKeyP50 * 1000 / readSetSize),-15}" +
                $"{F(perKeyAllocated / (double)iterations / 1024),-12}" +
                $"{F(batchedAllocated / (double)iterations / 1024),-12}");

            Assert.All(perKeySorted, latency => Assert.True(double.IsFinite(latency) && latency >= 0));
            Assert.All(batchedSorted, latency => Assert.True(double.IsFinite(latency) && latency >= 0));
        }

        output.WriteLine("");
        output.WriteLine("perKey  = one LocateAndTryCheckWriteIntent per read key, awaited together (what commit issues today).");
        output.WriteLine("batched = one LocateAndTryExistsManyValues over the same keys (the grouped shape, as a floor).");
        output.WriteLine("KB columns are process-wide allocations per iteration and include the harness' own overhead.");
    }

    /// <summary>
    /// Drives real optimistic commits whose read set was folded by one registered batch read, at growing read-set
    /// sizes, against a control that folds the identical read set but does not validate it at commit. The gap
    /// between the two arms is the whole commit-time read-validation cost — both passes — as a share of a commit
    /// that pays real durable I/O.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public async Task OptimisticCommit_CostOfFoldedReadDependencies()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;
        string rootPath = Path.Combine(Path.GetTempPath(), "kahuna-readset-benchmark-" + Guid.NewGuid().ToString("N"));

        try
        {
            await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
            {
                Storage = "rocksdb",
                StoragePath = Path.Combine(rootPath, "data"),
                WalStorage = "rocksdb",
                WalPath = Path.Combine(rootPath, "wal"),
                WalSyncWrites = true,
                InitialPartitions = 1
            }, loggerFactory);

            await node.StartAsync(cancellationToken);
            await node.WaitForLeaderForKeyAsync("readset-commit/seed", cancellationToken);

            KahunaManager kahuna = (KahunaManager)node.Kahuna;

            output.WriteLine("=== Optimistic commit by folded read-set size (RocksDB KV + sync RocksDB WAL) ===");
            output.WriteLine(
                $"{"reads",-8}{"txns",-7}{"validated.p50ms",-17}{"validated.p99ms",-17}" +
                $"{"control.p50ms",-15}{"control.p99ms",-15}{"delta.p50ms",-13}{"delta.share",-12}{"us/readkey",-11}");

            foreach (int readSetSize in ReadSetSizes)
            {
                string prefix = $"readset-commit/{readSetSize}";
                List<(string key, long revision, KeyValueDurability durability)> keys =
                    await SeedKeys(kahuna, prefix, readSetSize, cancellationToken);

                const int transactionCount = 25;

                await CommitWithReadSet(kahuna, prefix, keys, validate: true, "warmup", cancellationToken);
                await CommitWithReadSet(kahuna, prefix, keys, validate: false, "warmup", cancellationToken);

                double[] validated = new double[transactionCount];
                for (int i = 0; i < transactionCount; i++)
                    validated[i] = await CommitWithReadSet(kahuna, prefix, keys, validate: true, $"v{i}", cancellationToken);

                double[] control = new double[transactionCount];
                for (int i = 0; i < transactionCount; i++)
                    control[i] = await CommitWithReadSet(kahuna, prefix, keys, validate: false, $"c{i}", cancellationToken);

                double[] validatedSorted = validated.Order().ToArray();
                double[] controlSorted = control.Order().ToArray();
                double validatedP50 = Percentile(validatedSorted, 0.50);
                double controlP50 = Percentile(controlSorted, 0.50);
                double delta = validatedP50 - controlP50;

                output.WriteLine(
                    $"{readSetSize,-8}{transactionCount,-7}" +
                    $"{F(validatedP50),-17}{F(Percentile(validatedSorted, 0.99)),-17}" +
                    $"{F(controlP50),-15}{F(Percentile(controlSorted, 0.99)),-15}" +
                    $"{F(delta),-13}{F(validatedP50 <= 0 ? 0 : delta / validatedP50),-12}" +
                    $"{F(delta * 1000 / readSetSize),-11}");

                Assert.All(validatedSorted, latency => Assert.True(double.IsFinite(latency) && latency >= 0));
                Assert.All(controlSorted, latency => Assert.True(double.IsFinite(latency) && latency >= 0));
            }

            output.WriteLine("");
            output.WriteLine("validated = optimistic transaction: one registered batch read folds N dependencies, one key written.");
            output.WriteLine("control   = identical transaction and identical folded read set, read validation not requested.");
            output.WriteLine("delta.share is the fraction of commit latency attributable to validating the read set.");
        }
        finally
        {
            if (Directory.Exists(rootPath))
                Directory.Delete(rootPath, recursive: true);
        }
    }

    /// <summary>
    /// Counts the inter-node RPCs each shape costs when the read set is spread over a three-node cluster. This
    /// is the measurement that does not depend on machine speed: the per-key probe issues one RPC per remotely
    /// owned read key, while the many-key read issues one RPC per remote node holding any of them. On a single
    /// embedded node both shapes stay local and this difference does not exist, which is why the isolated
    /// single-node arm shows no gap.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public async Task WriteIntentProbe_RemoteCallCountAcrossCluster()
    {
        CancellationToken cancellationToken = TestContext.Current.CancellationToken;

        (IRaft raft1, IRaft raft2, IRaft raft3,
         IKahuna kahuna1, IKahuna _, IKahuna __,
         MemoryInterNodeCommmunication transport) =
            await AssembleThreNodeClusterWithTransport("memory", 8, raftLogger, kahunaLogger);

        try
        {
            output.WriteLine("=== Commit-time read-set probe, inter-node RPC count (3 nodes, 8 partitions) ===");
            output.WriteLine($"{"keys",-8}{"remote",-9}{"perKey.RPCs",-14}{"batched.RPCs",-14}{"RPCs saved",-13}{"perKey.p50ms",-14}{"batched.p50ms",-14}");

            foreach (int readSetSize in ReadSetSizes)
            {
                string prefix = $"readset-cluster/{readSetSize}";
                List<(string key, long revision, KeyValueDurability durability)> keys =
                    await SeedKeys(kahuna1, prefix, readSetSize, cancellationToken);

                HLCTimestamp probeTransactionId = await StartAndAbandonTransaction(kahuna1, prefix, cancellationToken);

                await ProbePerKey(kahuna1, probeTransactionId, keys, cancellationToken);
                await ProbeBatched(kahuna1, probeTransactionId, keys, cancellationToken);

                int perKeyRpcsBefore = transport.CheckWriteIntentCallCount;
                Stopwatch perKeySw = Stopwatch.StartNew();
                await ProbePerKey(kahuna1, probeTransactionId, keys, cancellationToken);
                perKeySw.Stop();
                int perKeyRpcs = transport.CheckWriteIntentCallCount - perKeyRpcsBefore;

                int batchedRpcsBefore = transport.ExistsManyCallCount;
                Stopwatch batchedSw = Stopwatch.StartNew();
                await ProbeBatched(kahuna1, probeTransactionId, keys, cancellationToken);
                batchedSw.Stop();
                int batchedRpcs = transport.ExistsManyCallCount - batchedRpcsBefore;

                // Every probed key the local node does not lead costs one RPC, so the per-key RPC count is also
                // the number of remotely owned keys. A round where the local node happens to lead every
                // partition holding the set reports zero for both shapes: nothing crossed the network, so
                // there was nothing for grouping to save.
                output.WriteLine(
                    $"{readSetSize,-8}{$"{perKeyRpcs}/{readSetSize}",-9}{perKeyRpcs,-14}{batchedRpcs,-14}{perKeyRpcs - batchedRpcs,-13}" +
                    $"{F(perKeySw.Elapsed.TotalMilliseconds),-14}{F(batchedSw.Elapsed.TotalMilliseconds),-14}");

                // The grouped shape can never need more remote calls than there are remote nodes, so it can
                // never exceed the per-key fan-out. This is the structural claim; the latencies alongside it are
                // reported only for context because the in-memory transport carries no real network cost.
                Assert.True(
                    batchedRpcs <= perKeyRpcs,
                    $"grouped shape issued {batchedRpcs} RPCs against {perKeyRpcs} for the per-key fan-out");
            }

            output.WriteLine("");
            output.WriteLine("The transport is in-memory: RPC counts are exact, but the latency columns omit real network cost,");
            output.WriteLine("so the saving a grouped probe would deliver on a real cluster is understated here.");
        }
        finally
        {
            await LeaveCluster(raft1, raft2, raft3);
        }
    }

    /// <summary>
    /// Issues the concurrent-writer probe the way the coordinator does today: one routed call per read key,
    /// all awaited together.
    /// </summary>
    private static async Task ProbePerKey(
        IKahuna kahuna,
        HLCTimestamp transactionId,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken)
    {
        Task<KeyValueResponseType>[] tasks = new Task<KeyValueResponseType>[keys.Count];

        for (int i = 0; i < keys.Count; i++)
            tasks[i] = kahuna.LocateAndTryCheckWriteIntent(transactionId, keys[i].key, keys[i].durability, cancellationToken);

        KeyValueResponseType[] results = await Task.WhenAll(tasks);

        // No competing transaction exists, so every probe must report "no conflicting writer". A different
        // response would mean the arm measured a rejection path instead of the probe path.
        Assert.All(results, result => Assert.Equal(KeyValueResponseType.DoesNotExist, result));
    }

    /// <summary>
    /// Reads the same keys through the existing many-key batched call — the routing and grouping shape a
    /// partition-grouped probe would reuse.
    /// </summary>
    private static async Task ProbeBatched(
        IKahuna kahuna,
        HLCTimestamp transactionId,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        CancellationToken cancellationToken)
    {
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> results =
            await kahuna.LocateAndTryExistsManyValues(transactionId, HLCTimestamp.Zero, keys, cancellationToken);

        Assert.Equal(keys.Count, results.Count);
        Assert.All(results, result => Assert.Equal(KeyValueResponseType.Exists, result.type));
    }

    /// <summary>
    /// Runs one transaction that folds <paramref name="keys"/> as read dependencies through a single registered
    /// batch read and writes one key outside that set, returning the commit latency in milliseconds. With
    /// <paramref name="validate"/> the commit validates the folded read set; without it the same set is folded
    /// and then ignored, which is the control.
    /// </summary>
    private static async Task<double> CommitWithReadSet(
        KahunaManager kahuna,
        string prefix,
        List<(string key, long revision, KeyValueDurability durability)> keys,
        bool validate,
        string transactionName,
        CancellationToken cancellationToken)
    {
        string coordinatorKey = $"{prefix}/tx/{transactionName}/{Guid.NewGuid():N}";

        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = coordinatorKey,
                Locking = validate ? KeyValueTransactionLocking.Optimistic : KeyValueTransactionLocking.Pessimistic,
                ReadValidation = validate ? ReadValidation.TrackAndValidate : ReadValidation.None,
                AsyncRelease = true,
                Timeout = 60_000,
                DecisionDurability = DecisionDurability.BestEffort
            }, cancellationToken);

        Assert.Equal(KeyValueResponseType.Set, startType);

        List<(KeyValueResponseType type, string key, KeyValueDurability durability, ReadOnlyKeyValueEntry? entry)> reads =
            await kahuna.LocateAndTryGetManyValues(
                handle.TransactionId, HLCTimestamp.Zero, keys, cancellationToken,
                handle.CoordinatorKey, TransactionOperationId.NewRandom());

        Assert.All(reads, read => Assert.Equal(KeyValueResponseType.Get, read.type));

        // The written key is deliberately outside the read set: a key the transaction modifies is validated as a
        // write, not as a read dependency, so writing into the set would shrink what commit has to validate.
        List<KahunaSetKeyValueRequestItem> items =
        [
            new()
            {
                TransactionId = handle.TransactionId,
                Key = $"{coordinatorKey}/written",
                Value = "v"u8.ToArray(),
                ExpiresMs = 0,
                Flags = KeyValueFlags.None,
                Durability = KeyValueDurability.Persistent
            }
        ];

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
    /// Writes <paramref name="count"/> keys outside any transaction and returns them in the shape the many-key
    /// read and probe paths take.
    /// </summary>
    private static async Task<List<(string key, long revision, KeyValueDurability durability)>> SeedKeys(
        IKahuna kahuna,
        string prefix,
        int count,
        CancellationToken cancellationToken)
    {
        const int seedBatchSize = 200;
        List<(string key, long revision, KeyValueDurability durability)> keys = new(count);

        for (int start = 0; start < count; start += seedBatchSize)
        {
            List<KahunaSetKeyValueRequestItem> batch = [];

            for (int i = start; i < Math.Min(start + seedBatchSize, count); i++)
            {
                string key = $"{prefix}/key/{i}";
                keys.Add((key, -1, KeyValueDurability.Persistent));
                batch.Add(new()
                {
                    TransactionId = HLCTimestamp.Zero,
                    Key = key,
                    Value = "seed"u8.ToArray(),
                    ExpiresMs = 0,
                    Flags = KeyValueFlags.None,
                    Durability = KeyValueDurability.Persistent
                });
            }

            // A seed write can meet the retryable MustRetry (a same-key committed-but-unsettled intent, or a
            // partition whose leadership is still settling on a freshly assembled cluster). Retry the whole
            // batch — a MustRetry attempt had no durable effect, so re-issuing it is what a real client does.
            List<KahunaSetKeyValueResponseItem> written =
                await kahuna.LocateAndTrySetManyKeyValue(batch, cancellationToken);

            for (int attempt = 0;
                 attempt < 20 && written.Any(item => item.Type != KeyValueResponseType.Set);
                 attempt++)
            {
                await Task.Delay(25, cancellationToken);
                written = await kahuna.LocateAndTrySetManyKeyValue(batch, cancellationToken);
            }

            Assert.All(written, item => Assert.Equal(KeyValueResponseType.Set, item.Type));
        }

        return keys;
    }

    /// <summary>
    /// Starts a transaction only to obtain a real transaction identity for the isolated probe arm, then rolls it
    /// back so it holds nothing. The probe must run under an id that owns no intent on the probed keys, which is
    /// exactly the state a rolled-back transaction leaves behind.
    /// </summary>
    private static async Task<HLCTimestamp> StartAndAbandonTransaction(
        IKahuna kahuna,
        string prefix,
        CancellationToken cancellationToken)
    {
        (KeyValueResponseType startType, TransactionHandle handle) = await kahuna.LocateAndStartTransaction(
            new KeyValueTransactionOptions
            {
                CoordinatorKey = $"{prefix}/probe-tx/{Guid.NewGuid():N}",
                Locking = KeyValueTransactionLocking.Optimistic,
                ReadValidation = ReadValidation.TrackAndValidate,
                AsyncRelease = true,
                Timeout = 60_000
            }, cancellationToken);

        Assert.Equal(KeyValueResponseType.Set, startType);

        await kahuna.LocateAndRollbackTransaction(handle, cancellationToken);

        return handle.TransactionId;
    }

    private static double Percentile(IReadOnlyList<double> sortedValues, double percentile)
    {
        int index = Math.Max(0, (int)Math.Ceiling(sortedValues.Count * percentile) - 1);
        return sortedValues[index];
    }
}

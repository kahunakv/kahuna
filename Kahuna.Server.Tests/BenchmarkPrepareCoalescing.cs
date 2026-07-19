using System.Diagnostics;
using System.Globalization;
using Kahuna;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Decision benchmark for the "fold one trailing prepare into the auto-commit proposal" slice of
/// spec-heterogeneous-write-coalescing-adoption.md. It measures, at the <see cref="IRaft"/> level on an
/// embedded node with a REAL rocksdb WAL and sync writes (so each proposal pays a real fsync), three shapes:
///
///   sep-auto   : one <c>ReplicateEntries</c> of N auto-commit records (what the aggregator does today).
///   sep-prepare: one lone prepare (<c>ReplicateLogs(autoCommit:false)</c>) — the separate-world prepare cost.
///   coalesced  : one <c>ReplicateEntries</c> of N auto records + one trailing manual prepare entry.
///
/// The decision hinges on two facts this quantifies:
///  1. Proposal/fsync count does NOT drop when a prepare is coalesced — Kommander proposes the auto group and
///     commits it BEFORE the manual group, so coalesced is still two proposals, exactly like separate.
///  2. Coalesced prepare latency ~= separate prepare latency + one auto-commit round trip (the prepare waits
///     for the auto group's commit). This is the added cost on the 2PC critical path.
///
/// This is a manual benchmark (run via --filter), not an assertion of a fixed threshold; it prints a report.
/// A single embedded node cannot measure the network round-trip savings coalescing would give in a real
/// multi-node cluster — it isolates the LOCAL fsync/proposal-overhead and the prepare-latency cost, which are
/// the factors that decide whether the slice is worth its complexity.
/// </summary>
[Collection("ClusterTests")]
public sealed class BenchmarkPrepareCoalescing
{
    private readonly ITestOutputHelper output;
    private readonly ILoggerFactory loggerFactory;

    public BenchmarkPrepareCoalescing(ITestOutputHelper outputHelper)
    {
        output = outputHelper;
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    private static byte[] SerializeSet(string key, long revision)
    {
        KeyValueMessage kvm = new()
        {
            Type = (int)KeyValueRequestType.TrySet,
            Key = key,
            Revision = revision
        };

        return ReplicationSerializer.Serialize(kvm);
    }

    private static RaftProposalEntry[] AutoBatch(int iteration, int count)
    {
        RaftProposalEntry[] entries = new RaftProposalEntry[count];
        for (int i = 0; i < count; i++)
            entries[i] = new RaftProposalEntry(
                ReplicationTypes.KeyValues, SerializeSet($"bench/auto/{iteration}/{i}", 0), AutoCommit: true, ExpectedGeneration: 0);
        return entries;
    }

    private static RaftProposalEntry[] AutoBatchPlusPrepare(int iteration, int count)
    {
        RaftProposalEntry[] entries = new RaftProposalEntry[count + 1];
        for (int i = 0; i < count; i++)
            entries[i] = new RaftProposalEntry(
                ReplicationTypes.KeyValues, SerializeSet($"bench/comb/{iteration}/{i}", 0), AutoCommit: true, ExpectedGeneration: 0);
        entries[count] = new RaftProposalEntry(
            ReplicationTypes.KeyValues, SerializeSet($"bench/comb/{iteration}/prepare", 0), AutoCommit: false, ExpectedGeneration: 0);
        return entries;
    }

    private static double Percentile(List<double> sorted, double p)
    {
        if (sorted.Count == 0) return 0;
        int idx = (int)Math.Ceiling(p / 100.0 * sorted.Count) - 1;
        return sorted[Math.Clamp(idx, 0, sorted.Count - 1)];
    }

    private static string Ms(double v) => v.ToString("F3", CultureInfo.InvariantCulture);

    private void Report(string name, List<double> samples)
    {
        samples.Sort();
        double mean = samples.Count == 0 ? 0 : samples.Sum() / samples.Count;
        double max = samples.Count > 0 ? samples[^1] : 0;
        output.WriteLine(
            $"{name,-16} n={samples.Count,4}  mean={Ms(mean),8}ms  p50={Ms(Percentile(samples, 50)),8}ms  " +
            $"p99={Ms(Percentile(samples, 99)),8}ms  max={Ms(max),8}ms");
    }

    [Fact]
    public async Task Benchmark_PrepareCoalescing_SeparateVsCombined()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        const int partitionId = 1;
        const int autoCount = 16;   // auto-commit records per burst (aggregator-sized)
        const int warmup = 20;
        const int iters = 200;

        string walPath = Path.Combine(Path.GetTempPath(), "kahuna-bench-wal-" + Guid.NewGuid().ToString("N"));

        await using EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = "memory",
            WalStorage = "rocksdb",
            WalPath = walPath,
            WalSyncWrites = true,
            InitialPartitions = 1
        }, loggerFactory);
        await node.StartAsync(ct);

        await node.Raft.WaitForLeader(partitionId, ct);
        IRaft raft = node.Raft;

        List<double> sepAuto = new(iters);
        List<double> sepPrepare = new(iters);
        List<double> coalesced = new(iters);

        Stopwatch sw = new();

        for (int i = -warmup; i < iters; i++)
        {
            bool measure = i >= 0;

            // ── separate world: an auto-commit batch and a lone prepare, independent proposals ──
            sw.Restart();
            RaftBatchReplicationResult auto = await raft.ReplicateEntries(partitionId, AutoBatch(i, autoCount), ct);
            double autoMs = sw.Elapsed.TotalMilliseconds;

            sw.Restart();
            RaftReplicationResult prepare = await raft.ReplicateLogs(
                partitionId, ReplicationTypes.KeyValues, SerializeSet($"bench/prep/{i}", 0), autoCommit: false, expectedGeneration: 0, cancellationToken: ct);
            double prepMs = sw.Elapsed.TotalMilliseconds;

            // Clean up the lone prepare's uncommitted suffix so the WAL does not accumulate live tickets.
            if (prepare.Success)
                await raft.RollbackLogs(partitionId, prepare.TicketId, ct);

            // ── coalesced world: the same auto records + one trailing prepare in one call ──
            sw.Restart();
            RaftBatchReplicationResult combined = await raft.ReplicateEntries(partitionId, AutoBatchPlusPrepare(i, autoCount), ct);
            double combinedMs = sw.Elapsed.TotalMilliseconds;

            // The manual ticket is the trailing group's; roll it back (truncates only the manual suffix).
            if (combined.Success && combined.TicketId != HLCTimestamp.Zero)
                await raft.RollbackLogs(partitionId, combined.TicketId, ct);

            if (measure)
            {
                sepAuto.Add(autoMs);
                sepPrepare.Add(prepMs);
                coalesced.Add(combinedMs);
            }
        }

        output.WriteLine($"=== Prepare-coalescing benchmark (rocksdb WAL, sync writes, autoCount={autoCount}, iters={iters}) ===");
        Report("sep-auto", sepAuto);
        Report("sep-prepare", sepPrepare);
        Report("coalesced", coalesced);

        double sepPrepareP50 = Percentile(sepPrepare, 50);
        double coalescedP50 = Percentile(coalesced, 50);
        double addedP50 = coalescedP50 - sepPrepareP50;
        output.WriteLine("");
        output.WriteLine("Proposals per unit of work: separate = 2 (auto batch + prepare); coalesced = 2 (auto group + manual group, sequential).");
        output.WriteLine("  => Coalescing a prepare saves NO proposal and NO fsync locally; Kommander keeps the two groups as two proposals.");
        output.WriteLine($"Prepare latency: separate p50={Ms(sepPrepareP50)}ms, coalesced p50={Ms(coalescedP50)}ms => added {Ms(addedP50)}ms on the 2PC critical path (~ one auto-commit).");

        Assert.NotEmpty(coalesced);
    }
}

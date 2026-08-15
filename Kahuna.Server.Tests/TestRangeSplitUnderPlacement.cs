
using System.Text;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;

using Kommander;
using Kommander.Communication.Memory;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;

using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Split and merge under per-partition replica placement: on a three-node RF=1 cluster each data
/// partition lives on exactly one node, so the meta-partition leader that drives a split/merge can
/// provably host neither the source range nor the destination. The data copy must page the source
/// through its leader and replicate every page onto the destination partition's Raft log, and the
/// transaction-state gather must read the source leader's stores — a local read on the driver
/// would see an empty range and silently lose the moved keys.
/// </summary>
public sealed class TestRangeSplitUnderPlacement : BaseCluster
{
    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestRangeSplitUnderPlacement(ITestOutputHelper outputHelper)
    {
        Microsoft.Extensions.Logging.ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IRaft>(loggerFactory);
        kahunaLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IKahuna>(loggerFactory);
    }

    private const int Partitions = 6;

    private const string Space = "rp:s";

    private static readonly double TimingScale = GetTimingScale();

    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double s) && s >= 1.0 ? s : 1.0;
    }

    private async Task<(IRaft[] Rafts, KahunaManager[] Kahunas, CancellationTokenSource Cts)> AssembleRf1Cluster()
    {
        InMemoryCommunication raftComm = new();
        MemoryInterNodeCommmunication interComm = new();

        (IRaft raft1, IKahuna kahuna1) = BuildNode(interComm, raftComm, "memory", 1, 8001, ["localhost:8002", "localhost:8003"], raftLogger, kahunaLogger, Partitions, replicationFactor: 1);
        (IRaft raft2, IKahuna kahuna2) = BuildNode(interComm, raftComm, "memory", 2, 8002, ["localhost:8001", "localhost:8003"], raftLogger, kahunaLogger, Partitions, replicationFactor: 1);
        (IRaft raft3, IKahuna kahuna3) = BuildNode(interComm, raftComm, "memory", 3, 8003, ["localhost:8001", "localhost:8002"], raftLogger, kahunaLogger, Partitions, replicationFactor: 1);

        interComm.SetNodes(new()
        {
            { "localhost:8001", kahuna1 },
            { "localhost:8002", kahuna2 },
            { "localhost:8003", kahuna3 }
        });

        raftComm.SetNodes(new()
        {
            { "localhost:8001", raft1 },
            { "localhost:8002", raft2 },
            { "localhost:8003", raft3 }
        });

        CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(180 * TimingScale));
        CancellationToken ct = cts.Token;

        IRaft[] rafts = [raft1, raft2, raft3];
        KahunaManager[] kahunas = [(KahunaManager)kahuna1, (KahunaManager)kahuna2, (KahunaManager)kahuna3];

        await Task.WhenAll(raft1.JoinCluster(ct), raft2.JoinCluster(ct), raft3.JoinCluster(ct));

        for (int partitionId = 0; partitionId <= Partitions; partitionId++)
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();

                bool anyLeader = false;
                foreach (IRaft raft in rafts)
                {
                    if (await raft.AmILeaderIfHosted(partitionId, ct))
                    {
                        anyLeader = true;
                        break;
                    }
                }

                if (anyLeader)
                    break;

                await Task.Delay(50, ct);
            }
        }

        foreach (IRaft raft in rafts)
        {
            for (int partitionId = 1; partitionId <= Partitions; partitionId++)
            {
                while (raft.GetPartitionReplicas(partitionId).Count == 0)
                {
                    ct.ThrowIfCancellationRequested();
                    await Task.Delay(50, ct);
                }
            }
        }

        return (rafts, kahunas, cts);
    }

    private static async Task<(IRaft Raft, KahunaManager Kahuna, int Index)> LeaderOf(
        int partition, IRaft[] rafts, KahunaManager[] kahunas, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < rafts.Length; i++)
                if (await rafts[i].AmILeaderIfHosted(partition, ct))
                    return (rafts[i], kahunas[i], i);
            await Task.Delay(50, ct);
        }
    }

    private static async Task<T> RetryKv<T>(Func<Task<T>> operation, Func<T, object> classify, CancellationToken ct)
    {
        T result = await operation();

        for (int attempt = 0; attempt < 100; attempt++)
        {
            object outcome = classify(result);

            if (outcome is not (KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication))
                return result;

            await Task.Delay(Math.Min(10 * (attempt + 1), 100), ct);
            result = await operation();
        }

        return result;
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public async Task SplitThenMerge_DrivenFromNodeNotHostingTheSource_MovesEveryKeyBothWays()
    {
        (IRaft[] rafts, KahunaManager[] kahunas, CancellationTokenSource cts) = await AssembleRf1Cluster();
        using CancellationTokenSource ctsGuard = cts;
        CancellationToken ct = cts.Token;

        foreach (KahunaManager kahuna in kahunas)
            kahuna.RegisterKeyRange(Space);

        // The split/merge driver is the meta-partition leader; the source range is deliberately
        // seeded onto a data partition that driver does NOT host, so a local read of the range on
        // the driver would be empty and only leader-routed copies can move the data.
        (IRaft driverRaft, KahunaManager driver, _) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);

        int sourcePartition = 0;
        for (int partitionId = 1; partitionId <= Partitions; partitionId++)
        {
            if (!driverRaft.HostsPartition(partitionId))
            {
                sourcePartition = partitionId;
                break;
            }
        }
        Assert.NotEqual(0, sourcePartition);

        bool seeded = await driver.RangeMapStore.MutateAsync(
            _ => [new RangeDescriptor { KeySpace = Space, PartitionId = sourcePartition, Generation = 1 }], ct);
        Assert.True(seeded);

        foreach (KahunaManager kahuna in kahunas)
            await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1, timeoutMs: 30_000);

        string[] keysBelow = [$"{Space}/a1", $"{Space}/b2", $"{Space}/c3"];
        string[] keysAbove = [$"{Space}/n1", $"{Space}/p2", $"{Space}/z3"];
        string splitKey = $"{Space}/m";

        // Writes go through the locator from the (non-hosting) driver — forwarding routes them
        // to the source partition's leader.
        foreach (string key in keysBelow.Concat(keysAbove))
        {
            (KeyValueResponseType setType, _, _) = await RetryKv(
                () => driver.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, V(key), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
                r => r.Item1, ct);
            Assert.Equal(KeyValueResponseType.Set, setType);
        }

        // ── Split, driven from the meta leader that hosts no part of the source range ─────────
        SplitOutcome splitOutcome = default;

        for (int attempt = 0; attempt < 5; attempt++)
        {
            (driverRaft, driver, _) = await LeaderOf(RangeMapStore.MetaPartitionId, rafts, kahunas, ct);

            int newPartitionId = RangeSplitter.ComputeNextPartitionId(driver.RangeMapStore.Current);

            RaftPartitionLifecycleResult createResult =
                await driverRaft.CreatePartitionAsync(newPartitionId, RaftRoutingMode.Unrouted, null, ct);
            Assert.True(createResult.Success);

            splitOutcome = await driver.RangeSplitter.SplitAsync(Space, splitKey, newPartitionId, ct);

            if (splitOutcome.Status != SplitStatus.CutoverFailed)
                break;

            await Task.Delay(100, ct);
        }

        Assert.Equal(SplitStatus.Succeeded, splitOutcome.Status);

        RangeDescriptor? left = driver.RangeMapStore.Current.Find(Space, $"{Space}/a1");
        RangeDescriptor? right = driver.RangeMapStore.Current.Find(Space, $"{Space}/z3");
        Assert.NotNull(left);
        Assert.NotNull(right);
        Assert.Equal(sourcePartition, left!.PartitionId);
        Assert.Equal(splitOutcome.NewPartitionId, right!.PartitionId);

        // Every key survives the split and is readable through every node (forwarding included).
        foreach (string key in keysBelow.Concat(keysAbove))
        {
            foreach (KahunaManager kahuna in kahunas)
            {
                (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await RetryKv(
                    () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                    r => r.Item1, ct);
                Assert.Equal(KeyValueResponseType.Get, getType);
                Assert.NotNull(entry);
                Assert.Equal(V(key), entry!.Value);
            }
        }

        // ── Merge the halves back, driven from the same meta leader ───────────────────────────
        MergeOutcome mergeOutcome = await driver.RangeMerger.MergeAsync(Space, left, right, ct);
        Assert.Equal(MergeStatus.Succeeded, mergeOutcome.Status);
        Assert.Equal(right.PartitionId, mergeOutcome.RetiredPartitionId);

        RangeDescriptor? merged = driver.RangeMapStore.Current.Find(Space, $"{Space}/z3");
        Assert.NotNull(merged);
        Assert.Equal(sourcePartition, merged!.PartitionId);

        // Every key survives the merge too, from every node.
        foreach (string key in keysBelow.Concat(keysAbove))
        {
            foreach (KahunaManager kahuna in kahunas)
            {
                (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await RetryKv(
                    () => kahuna.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, KeyValueDurability.Persistent, ct),
                    r => r.Item1, ct);
                Assert.Equal(KeyValueResponseType.Get, getType);
                Assert.NotNull(entry);
                Assert.Equal(V(key), entry!.Value);
            }
        }
    }

    [Fact]
    public async Task RangeTransactionStateGather_ReadsTheSourceLeader_AndRefusesOnNonLeaders()
    {
        (IRaft[] rafts, KahunaManager[] kahunas, CancellationTokenSource cts) = await AssembleRf1Cluster();
        using CancellationTokenSource ctsGuard = cts;
        CancellationToken ct = cts.Token;

        // Pick a data partition and identify its (sole) hosting leader plus a non-hosting node.
        const int partitionId = 1;
        (IRaft leaderRaft, KahunaManager leader, int leaderIndex) = await LeaderOf(partitionId, rafts, kahunas, ct);

        int otherIndex = (leaderIndex + 1) % 3;
        if (rafts[otherIndex].HostsPartition(partitionId))
            otherIndex = (leaderIndex + 2) % 3;
        KahunaManager nonHosting = kahunas[otherIndex];
        Assert.False(rafts[otherIndex].HostsPartition(partitionId));

        // Plant a receipt in the leader's store — the state a moving range must carry.
        HLCTimestamp txId = new(0, 777, 0);
        CompletionReceiptRecord receipt = new(txId, "gather/key", null, KeyValueDurability.Persistent);
        leader.KeyValues.ImportCompletionReceipts([receipt]);

        // A non-leader must refuse to answer from its own (possibly lagging, here empty) stores.
        (bool localOk, _, _, _) = await nonHosting.KeyValues.GetRangeTransactionStateLocal(partitionId, null, null, ct);
        Assert.False(localOk);

        // The leader-routed gather from the non-hosting node reads the authoritative stores.
        (bool ok, IReadOnlyCollection<CompletionReceiptRecord> receipts, _, _) =
            await nonHosting.KeyValues.GetRangeTransactionStateFromPartitionLeaderAsync(partitionId, null, null, ct);

        Assert.True(ok);
        Assert.Contains(receipts, r => r.TransactionId == txId && r.Key == "gather/key");
    }
}

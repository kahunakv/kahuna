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

namespace Kahuna.Server.Tests;

/// <summary>
/// A split or merge copies one revision per key, and a replica of the destination that never hosted the source has no
/// other. On a three-node RF=1 cluster each partition lives on one node, so the destination here is exactly that replica.
/// A read at a snapshot hold must answer from the revisions the move carried, a read at any other older timestamp must be
/// refused rather than answered from what happens to be there, and a key deleted after the copy saw it must stay deleted.
/// </summary>
public sealed class TestRangeMoveKeepsHeldHistory : BaseCluster
{
    private const int Partitions = 6;

    private const string Space = "rh:s";

    private const string SplitKey = $"{Space}/m";

    private const string Below = $"{Space}/a";

    private const string Untouched = $"{Space}/n";

    private const string Updated = $"{Space}/o";

    private const string Deleted = $"{Space}/p";

    private const string Inserted = $"{Space}/q";

    private readonly Microsoft.Extensions.Logging.ILogger<IRaft> raftLogger;

    private readonly Microsoft.Extensions.Logging.ILogger<IKahuna> kahunaLogger;

    public TestRangeMoveKeepsHeldHistory(ITestOutputHelper outputHelper)
    {
        Microsoft.Extensions.Logging.ILoggerFactory loggerFactory = TestLogFactory.Create(outputHelper);
        raftLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IRaft>(loggerFactory);
        kahunaLogger = Microsoft.Extensions.Logging.LoggerFactoryExtensions.CreateLogger<IKahuna>(loggerFactory);
    }

    private sealed record Cluster(IRaft[] Rafts, KahunaManager[] Kahunas, CancellationTokenSource Cts);

    private async Task<Cluster> AssembleRf1Cluster()
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
            int partition = partitionId;
            await WaitUntilAsync(async () =>
            {
                foreach (IRaft raft in rafts)
                    if (await raft.AmILeaderIfHosted(partition, ct))
                        return true;
                return false;
            }, timeoutMs: 60_000);
        }

        foreach (IRaft raft in rafts)
        {
            for (int partitionId = 1; partitionId <= Partitions; partitionId++)
            {
                int partition = partitionId;
                await WaitUntilAsync(() => raft.GetPartitionReplicas(partition).Count > 0, timeoutMs: 60_000);
            }
        }

        foreach (KahunaManager kahuna in kahunas)
            kahuna.RegisterKeyRange(Space);

        return new(rafts, kahunas, cts);
    }

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> MetaLeader(Cluster cluster, CancellationToken ct)
    {
        while (true)
        {
            for (int i = 0; i < cluster.Rafts.Length; i++)
                if (await cluster.Rafts[i].AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct))
                    return (cluster.Rafts[i], cluster.Kahunas[i]);

            await Task.Delay(50, ct);
        }
    }

    /// <summary>
    /// Creates the partition the split will move to, then seeds the range on a partition hosted elsewhere, so the destination
    /// is a replica that never hosted the source.
    /// </summary>
    private static async Task<(int Source, int Destination)> SeedRangeAwayFromNewPartition(Cluster cluster, CancellationToken ct)
    {
        (IRaft raft, KahunaManager driver) = await MetaLeader(cluster, ct);

        int destination = RangeSplitter.ComputeNextPartitionId(raft, driver.RangeMapStore.Current);
        RaftPartitionLifecycleResult created = await raft.CreatePartitionAsync(destination, RaftRoutingMode.Unrouted, null, ct);
        Assert.True(created.Success);

        await WaitUntilAsync(() => cluster.Rafts.Any(r => r.HostsPartition(destination)), timeoutMs: 30_000);
        IRaft destinationHost = cluster.Rafts.First(r => r.HostsPartition(destination));

        int source = 0;
        for (int partitionId = 1; partitionId <= Partitions && source == 0; partitionId++)
        {
            if (!destinationHost.HostsPartition(partitionId))
                source = partitionId;
        }

        Assert.NotEqual(0, source);

        Assert.True(await driver.RangeMapStore.MutateAsync(_ => [new RangeDescriptor { KeySpace = Space, PartitionId = source, Generation = 1 }], ct));

        foreach (KahunaManager kahuna in cluster.Kahunas)
            await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, Space + "/x")?.Generation == 1, timeoutMs: 30_000);

        return (source, destination);
    }

    private static async Task<T> RetryWhileNotReady<T>(Func<Task<T>> operation, Func<T, KeyValueResponseType> typeOf, CancellationToken ct)
    {
        T result = await operation();

        for (int attempt = 0; attempt < 100 && typeOf(result) is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication; attempt++)
        {
            await Task.Delay(Math.Min(10 * (attempt + 1), 100), ct);
            result = await operation();
        }

        return result;
    }

    private static async Task<HLCTimestamp> Set(KahunaManager node, string key, string value, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryWhileNotReady(
            () => node.LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
            static r => r.Item1, ct);

        Assert.Equal(KeyValueResponseType.Set, type);

        (_, ReadOnlyKeyValueEntry? written) = await ReadUntilReady(node, key, HLCTimestamp.Zero, ct);
        return written!.LastModified;
    }

    private static async Task Delete(KahunaManager node, string key, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryWhileNotReady(
            () => node.LocateAndTryDeleteKeyValue(HLCTimestamp.Zero, key, KeyValueDurability.Persistent, ct),
            static r => r.Item1, ct);

        Assert.Equal(KeyValueResponseType.Deleted, type);
    }

    private static async Task<(KeyValueResponseType Type, ReadOnlyKeyValueEntry? Entry)> ReadUntilReady(KahunaManager node, string key, HLCTimestamp at, CancellationToken ct)
    {
        for (int attempt = 0; ; attempt++)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await node.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, at, KeyValueDurability.Persistent, ct);

            if (attempt >= 100 || type is not (KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication))
                return (type, entry);

            await Task.Delay(Math.Min(10 * (attempt + 1), 100), ct);
        }
    }

    private static async Task AssertValue(KahunaManager node, string key, HLCTimestamp at, string expected, CancellationToken ct)
    {
        (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await ReadUntilReady(node, key, at, ct);

        Assert.Equal(KeyValueResponseType.Get, type);
        Assert.Equal(expected, Encoding.UTF8.GetString(entry!.Value!));
    }

    private static async Task AssertAbsent(KahunaManager node, string key, HLCTimestamp at, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await ReadUntilReady(node, key, at, ct);

        Assert.Equal(KeyValueResponseType.DoesNotExist, type);
    }

    // No amount of retrying makes the node able to answer this timestamp.
    private static async Task AssertRefused(KahunaManager node, string key, HLCTimestamp at, CancellationToken ct)
    {
        (KeyValueResponseType type, _) = await node.LocateAndTryGetValue(HLCTimestamp.Zero, key, -1, at, KeyValueDurability.Persistent, ct);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
    }

    private static async Task<HLCTimestamp> Hold(Cluster cluster, KahunaManager node, string holder, HLCTimestamp at, CancellationToken ct)
    {
        (KeyValueResponseType type, _, _) = await RetryWhileNotReady(
            () => node.LocateAndAcquireSnapshotHold(holder, at, leaseMs: 600_000, ct), static r => r.Item1, ct);

        Assert.Equal(KeyValueResponseType.Set, type);

        await WaitUntilAsync(
            () => cluster.Kahunas.All(k => k.KeyValues.SnapshotFloorStore.Holds.Values.Any(h => h.Timestamp == at)),
            timeoutMs: 30_000);

        return at;
    }

    private static async Task<SplitOutcome> Split(KahunaManager driver, int destination, CancellationToken ct)
    {
        SplitOutcome outcome = default;

        for (int attempt = 0; attempt < 5; attempt++)
        {
            outcome = await driver.RangeSplitter.SplitAsync(Space, SplitKey, destination, ct);

            if (outcome.Status != SplitStatus.CutoverFailed)
                break;

            await Task.Delay(100, ct);
        }

        return outcome;
    }

    [Fact]
    public async Task ASplit_CarriesEveryHoldsRevisions_ToAReplicaThatNeverHostedTheSource()
    {
        Cluster cluster = await AssembleRf1Cluster();
        using CancellationTokenSource guard = cluster.Cts;
        CancellationToken ct = cluster.Cts.Token;

        (int source, int destination) = await SeedRangeAwayFromNewPartition(cluster, ct);
        (_, KahunaManager driver) = await MetaLeader(cluster, ct);

        await Set(driver, Below, "below", ct);
        await Set(driver, Untouched, "held", ct);
        await Set(driver, Updated, "held", ct);
        HLCTimestamp held = await Hold(cluster, driver, "split-test", await Set(driver, Deleted, "held", ct), ct);

        HLCTimestamp notHeld = await Set(driver, Updated, "between", ct);

        for (int i = 0; i < 8; i++)
            await Set(driver, Updated, $"after-{i}", ct);

        await Delete(driver, Deleted, ct);
        await Set(driver, Inserted, "new", ct);

        SplitOutcome outcome = await Split(driver, destination, ct);
        Assert.Equal(SplitStatus.Succeeded, outcome.Status);

        // The destination's only replica never held the source, so nothing but the carried rows can answer.
        int host = Array.FindIndex(cluster.Rafts, r => r.HostsPartition(destination));
        Assert.False(cluster.Rafts[host].HostsPartition(source));
        KahunaManager node = cluster.Kahunas[host];

        await WaitUntilAsync(() => node.RangeMapStore.Current.Find(Space, Updated)?.PartitionId == destination, timeoutMs: 30_000);

        await AssertValue(node, Untouched, held, "held", ct);
        await AssertValue(node, Updated, held, "held", ct);
        await AssertValue(node, Deleted, held, "held", ct);
        await AssertAbsent(node, Inserted, held, ct);

        // The current state, including a deleted key that must stay deleted.
        await AssertValue(node, Updated, HLCTimestamp.Zero, "after-7", ct);
        await AssertAbsent(node, Deleted, HLCTimestamp.Zero, ct);
        await AssertValue(node, Inserted, HLCTimestamp.Zero, "new", ct);

        // A timestamp nothing holds was not carried, and is refused rather than answered with an older revision.
        await AssertRefused(node, Updated, notHeld, ct);
        await AssertRefused(driver, Updated, notHeld, ct);

        // The half that stayed keeps its whole history.
        RangeDescriptor stayed = node.RangeMapStore.Current.Find(Space, Below)!;
        Assert.Equal(HLCTimestamp.Zero, stayed.HistoryCompleteFrom);
        await AssertValue(driver, Below, notHeld, "below", ct);

        // Every node holds the same bound, because it rides the range map.
        foreach (KahunaManager kahuna in cluster.Kahunas)
        {
            await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(Space, Updated)?.PartitionId == destination, timeoutMs: 30_000);
            RangeDescriptor moved = kahuna.RangeMapStore.Current.Find(Space, Updated)!;
            Assert.Equal([held], moved.HistoryPins);
            Assert.True(moved.HistoryCompleteFrom.CompareTo(held) > 0);
        }
    }

    [Fact]
    public async Task AKeyDeletedBetweenTheCopies_StaysDeleted()
    {
        Cluster cluster = await AssembleRf1Cluster();
        using CancellationTokenSource guard = cluster.Cts;
        CancellationToken ct = cluster.Cts.Token;

        (_, int destination) = await SeedRangeAwayFromNewPartition(cluster, ct);
        (_, KahunaManager driver) = await MetaLeader(cluster, ct);

        await Set(driver, Below, "below", ct);
        await Set(driver, Untouched, "untouched", ct);

        await Set(driver, Deleted, "copied-by-the-bulk-copy", ct);
        await Set(driver, Updated, "copied-by-the-bulk-copy", ct);

        driver.RangeSplitter.AfterBulkCopy = async () =>
        {
            driver.RangeSplitter.AfterBulkCopy = null;
            await Delete(driver, Deleted, ct);
            await Set(driver, Updated, "after-the-bulk-copy", ct);
        };

        SplitOutcome outcome = await Split(driver, destination, ct);
        Assert.Equal(SplitStatus.Succeeded, outcome.Status);

        await AssertAbsent(driver, Deleted, HLCTimestamp.Zero, ct);
        await AssertValue(driver, Updated, HLCTimestamp.Zero, "after-the-bulk-copy", ct);
    }

    [Fact]
    public async Task AHoldRegisteredDuringTheSplit_IsCarriedToo()
    {
        Cluster cluster = await AssembleRf1Cluster();
        using CancellationTokenSource guard = cluster.Cts;
        CancellationToken ct = cluster.Cts.Token;

        (int source, int destination) = await SeedRangeAwayFromNewPartition(cluster, ct);
        (_, KahunaManager driver) = await MetaLeader(cluster, ct);

        await Set(driver, Below, "below", ct);
        await Set(driver, Untouched, "untouched", ct);

        HLCTimestamp early = await Hold(cluster, driver, "early", await Set(driver, Updated, "at-early", ct), ct);
        HLCTimestamp? late = null;

        driver.RangeSplitter.AfterBulkCopy = async () =>
        {
            driver.RangeSplitter.AfterBulkCopy = null;
            late = await Hold(cluster, driver, "late", await Set(driver, Updated, "at-late", ct), ct);
            await Set(driver, Updated, "now", ct);
        };

        SplitOutcome outcome = await Split(driver, destination, ct);
        Assert.Equal(SplitStatus.Succeeded, outcome.Status);

        int host = Array.FindIndex(cluster.Rafts, r => r.HostsPartition(destination));
        Assert.False(cluster.Rafts[host].HostsPartition(source));
        KahunaManager node = cluster.Kahunas[host];

        await AssertValue(node, Updated, early, "at-early", ct);
        await AssertValue(node, Updated, late!.Value, "at-late", ct);
        await AssertValue(node, Updated, HLCTimestamp.Zero, "now", ct);
    }

    [Fact]
    public async Task AMerge_CarriesHoldsToTheSurvivor_AndBoundsTheWholeMergedRange()
    {
        Cluster cluster = await AssembleRf1Cluster();
        using CancellationTokenSource guard = cluster.Cts;
        CancellationToken ct = cluster.Cts.Token;

        (int source, int destination) = await SeedRangeAwayFromNewPartition(cluster, ct);
        (_, KahunaManager driver) = await MetaLeader(cluster, ct);

        HLCTimestamp beforeSplit = await Set(driver, Below, "below", ct);
        await Set(driver, Untouched, "before-split", ct);
        await Set(driver, Deleted, "before-split", ct);

        SplitOutcome split = await Split(driver, destination, ct);
        Assert.Equal(SplitStatus.Succeeded, split.Status);

        // These revisions only ever exist on the destination: the survivor stopped seeing the range at the split.
        await Set(driver, Updated, "held", ct);
        HLCTimestamp held = await Hold(cluster, driver, "merge-test", await Set(driver, Inserted, "held", ct), ct);
        HLCTimestamp notHeld = await Set(driver, Updated, "between", ct);
        await Set(driver, Updated, "now", ct);
        await Delete(driver, Inserted, ct);

        RangeDescriptor left = driver.RangeMapStore.Current.Find(Space, Below)!;
        RangeDescriptor right = driver.RangeMapStore.Current.Find(Space, Updated)!;

        MergeOutcome merge = await driver.RangeMerger.MergeAsync(Space, left, right, ct);
        Assert.Equal(MergeStatus.Succeeded, merge.Status);

        KahunaManager node = cluster.Kahunas[Array.FindIndex(cluster.Rafts, r => r.HostsPartition(source))];
        await WaitUntilAsync(() => node.RangeMapStore.Current.Find(Space, Updated)?.PartitionId == source, timeoutMs: 30_000);

        await AssertValue(node, Updated, held, "held", ct);
        await AssertValue(node, Inserted, held, "held", ct);
        await AssertValue(node, Below, held, "below", ct);

        await AssertValue(node, Updated, HLCTimestamp.Zero, "now", ct);
        await AssertAbsent(node, Inserted, HLCTimestamp.Zero, ct);

        // The map has one bound per range, so a key that never moved is bounded with the rest of the merged range.
        await AssertRefused(node, Updated, notHeld, ct);
        await AssertRefused(node, Below, beforeSplit, ct);
    }

    [Fact]
    public void AnUnboundedRange_AnswersEverything_AndABoundedOneOnlyItsPinsBelowTheBound()
    {
        HLCTimestamp At(long physical) => new(0, physical, 0);

        RangeDescriptor unbounded = new() { KeySpace = Space, PartitionId = 1 };
        Assert.True(unbounded.IsHistoryServable(At(1)));

        RangeDescriptor bounded = unbounded with { HistoryCompleteFrom = At(100), HistoryPins = [At(10), At(20)] };
        Assert.True(bounded.IsHistoryServable(At(10)));
        Assert.True(bounded.IsHistoryServable(At(20)));
        Assert.False(bounded.IsHistoryServable(At(15)));
        Assert.False(bounded.IsHistoryServable(At(99)));
        Assert.True(bounded.IsHistoryServable(At(100)));
        Assert.True(bounded.IsHistoryServable(At(101)));

        Assert.Equal(At(100), new RangeMap([unbounded, bounded with { KeySpace = "other" }]).HighestHistoryBound);
        Assert.Equal(HLCTimestamp.Zero, new RangeMap([unbounded]).HighestHistoryBound);
    }
}

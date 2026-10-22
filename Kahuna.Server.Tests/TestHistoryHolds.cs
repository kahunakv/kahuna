using System.Text;
using Kahuna;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The replicated history hold registry and the pins each replica takes from it. A hold acquired through any node is
/// committed by the meta-partition leader and reaches every replica, and each replica pins its own WAL for the hosted
/// partition at the first entry a resume from the hold's timestamp can need. Renewing forward moves every pin forward;
/// a release, or the reaper's purge of a lapsed hold, removes every pin; the bounds refuse an acquire; and after a
/// restart the registry comes back from disk and the partition is pinned again as its restore finishes.
/// </summary>
public sealed class TestHistoryHolds : BaseCluster
{
    private readonly ILoggerFactory loggerFactory;

    private readonly ILogger<IRaft> raftLogger;

    private readonly ILogger<IKahuna> kahunaLogger;

    public TestHistoryHolds(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper, quietKommander: true);
        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    [Fact]
    public async Task Acquire_ThroughAnyNode_PinsThePartitionOnEveryReplica()
    {
        await WithCluster(async (nodes, ct) =>
        {
            int partitionId = await WriteSome(nodes, "hh-pin", ct);
            HLCTimestamp from = new(1, 1, 0);

            KahunaManager sender = await NotMetaLeader(nodes, ct);
            HistoryHoldAnswer acquired = await Hold(sender, new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-a", [partitionId], from, 60_000), ct);

            Assert.Equal(KeyValueResponseType.Set, acquired.Type);
            HistoryHoldInfo hold = Assert.Single(acquired.Holds);
            Assert.Equal(partitionId, hold.PartitionId);
            Assert.Equal(from, hold.Timestamp);

            foreach (KahunaManager node in nodes)
            {
                await WaitUntilAsync(() => node.KeyValues.HistoryHoldStoreForTests.Holds.ContainsKey(hold.HoldId));
                await WaitPinnedAt(node, partitionId, from);
            }

            HistoryHoldAnswer listed = await Hold(nodes[2], new HistoryHoldRequest(HistoryHoldOperation.List, "cdc-a"), ct);
            Assert.Equal(KeyValueResponseType.Get, listed.Type);
            Assert.Equal(hold.HoldId, Assert.Single(listed.Holds).HoldId);
        });
    }

    [Fact]
    public async Task Acquire_IsIdempotentPerPartition_AndNeverMovesBack()
    {
        await WithCluster(async (nodes, ct) =>
        {
            int partitionId = await WriteSome(nodes, "hh-idem", ct);

            HistoryHoldAnswer first = await Hold(nodes[0], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-b", [partitionId], new HLCTimestamp(1, 1_000, 0), 60_000), ct);
            HistoryHoldAnswer forward = await Hold(nodes[1], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-b", [partitionId], new HLCTimestamp(1, 2_000, 0), 60_000), ct);

            Assert.Equal(KeyValueResponseType.Set, forward.Type);
            Assert.Equal(Assert.Single(first.Holds).HoldId, Assert.Single(forward.Holds).HoldId);
            Assert.Equal(new HLCTimestamp(1, 2_000, 0), forward.Holds[0].Timestamp);

            HistoryHoldAnswer back = await Hold(nodes[2], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-b", [partitionId], new HLCTimestamp(1, 1_500, 0), 60_000), ct);
            Assert.Equal(KeyValueResponseType.InvalidInput, back.Type);

            HistoryHoldAnswer listed = await Hold(nodes[0], new HistoryHoldRequest(HistoryHoldOperation.List, "cdc-b"), ct);
            Assert.Equal(new HLCTimestamp(1, 2_000, 0), Assert.Single(listed.Holds).Timestamp);
        });
    }

    [Fact]
    public async Task Renew_MovesThePinForward_OnEveryReplica_AndRefusesABackwardTimestamp()
    {
        await WithCluster(async (nodes, ct) =>
        {
            int partitionId = await WriteSome(nodes, "hh-renew", ct);
            HLCTimestamp early = new(1, 1, 0);

            HistoryHoldInfo hold = Assert.Single((await Hold(nodes[0], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-c", [partitionId], early, 60_000), ct)).Holds);

            foreach (KahunaManager node in nodes)
                await WaitPinnedAt(node, partitionId, early);

            // Past every entry the partition holds: the pin moves to the next index.
            HLCTimestamp later = Now(nodes[0]) + 10_000;
            HistoryHoldAnswer renewed = await Hold(nodes[1], new HistoryHoldRequest(HistoryHoldOperation.Renew, Timestamp: later, LeaseMs: 60_000, HoldIds: [hold.HoldId]), ct);

            Assert.Equal(KeyValueResponseType.Set, renewed.Type);
            Assert.Equal(later, Assert.Single(renewed.Holds).Timestamp);

            foreach (KahunaManager node in nodes)
            {
                await WaitPinnedAt(node, partitionId, later);
                Assert.True(node.KeyValues.HistoryHoldPinnerForTests!.PinnedIndexes()[partitionId] > 1, "the renewed pin must move forward");
            }

            HistoryHoldAnswer back = await Hold(nodes[2], new HistoryHoldRequest(HistoryHoldOperation.Renew, Timestamp: early, LeaseMs: 60_000, HoldIds: [hold.HoldId]), ct);
            Assert.Equal(KeyValueResponseType.InvalidInput, back.Type);
        });
    }

    [Fact]
    public async Task Release_DropsThePinOnEveryReplica()
    {
        await WithCluster(async (nodes, ct) =>
        {
            int partitionId = await WriteSome(nodes, "hh-release", ct);
            HistoryHoldInfo hold = Assert.Single((await Hold(nodes[0], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-d", [partitionId], new HLCTimestamp(1, 1, 0), 60_000), ct)).Holds);

            foreach (KahunaManager node in nodes)
                await WaitUntilAsync(() => node.KeyValues.HistoryHoldPinnerForTests!.PinnedIndexes().ContainsKey(partitionId));

            HistoryHoldAnswer released = await Hold(nodes[2], new HistoryHoldRequest(HistoryHoldOperation.Release, HoldIds: [hold.HoldId]), ct);
            Assert.Equal(KeyValueResponseType.Deleted, released.Type);

            foreach (KahunaManager node in nodes)
            {
                await WaitUntilAsync(() => !node.KeyValues.HistoryHoldPinnerForTests!.PinnedIndexes().ContainsKey(partitionId));
                Assert.Empty(node.KeyValues.HistoryHoldStoreForTests.Holds);
            }

            HistoryHoldAnswer renewGone = await Hold(nodes[1], new HistoryHoldRequest(HistoryHoldOperation.Renew, LeaseMs: 60_000, HoldIds: [hold.HoldId]), ct);
            Assert.Equal(KeyValueResponseType.DoesNotExist, renewGone.Type);
        });
    }

    [Fact]
    public async Task LapsedHold_KeepsItsPin_UntilThePurgeCommits()
    {
        await WithCluster(async (nodes, ct) =>
        {
            int partitionId = await WriteSome(nodes, "hh-lapse", ct);
            HistoryHoldInfo hold = Assert.Single((await Hold(nodes[0], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-e", [partitionId], new HLCTimestamp(1, 1, 0), 200), ct)).Holds);

            await Task.Delay(800, ct);

            // The lease lapsed, but nothing removed the hold: every replica keeps its pin.
            foreach (KahunaManager node in nodes)
            {
                await WaitUntilAsync(() => node.KeyValues.HistoryHoldPinnerForTests!.PinnedIndexes().ContainsKey(partitionId));
                Assert.True(node.KeyValues.HistoryHoldStoreForTests.Holds.ContainsKey(hold.HoldId));
            }

            KahunaManager metaLeader = await MetaLeader(nodes, ct);
            // The node's reaper purges on its own timer, so it can take the hold first; either purge removes it.
            int purged = await metaLeader.KeyValues.PurgeHistoryHoldsAsync(ct);
            Assert.True(purged == 1 || !metaLeader.KeyValues.HistoryHoldStoreForTests.Holds.ContainsKey(hold.HoldId), $"the purge removed {purged} holds and left the retired partition's hold");

            foreach (KahunaManager node in nodes)
            {
                await WaitUntilAsync(() => node.KeyValues.HistoryHoldStoreForTests.Holds.Count == 0);
                await WaitUntilAsync(() => !node.KeyValues.HistoryHoldPinnerForTests!.PinnedIndexes().ContainsKey(partitionId));
            }
        });
    }

    [Fact]
    public async Task LapsedHold_IsRevivedByARenew_BeforeThePurge()
    {
        await WithCluster(async (nodes, ct) =>
        {
            int partitionId = await WriteSome(nodes, "hh-revive", ct);
            HistoryHoldInfo hold = Assert.Single((await Hold(nodes[0], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-f", [partitionId], new HLCTimestamp(1, 1, 0), 200), ct)).Holds);

            await Task.Delay(800, ct);

            HistoryHoldAnswer revived = await Hold(nodes[1], new HistoryHoldRequest(HistoryHoldOperation.Renew, LeaseMs: 60_000, HoldIds: [hold.HoldId]), ct);
            Assert.Equal(KeyValueResponseType.Set, revived.Type);

            KahunaManager metaLeader = await MetaLeader(nodes, ct);
            Assert.Equal(0, await metaLeader.KeyValues.PurgeHistoryHoldsAsync(ct));
            Assert.True(metaLeader.KeyValues.HistoryHoldStoreForTests.Holds.ContainsKey(hold.HoldId));
        });
    }

    [Fact]
    public async Task Bounds_RefuseAnAcquire()
    {
        await WithCluster(configure: c =>
        {
            c.HistoryHoldMaxPerHolder = 2;
            c.HistoryHoldMaxLeaseMs = 10_000;
        }, body: async (nodes, ct) =>
        {
            HistoryHoldAnswer tooLong = await Hold(nodes[0], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-g", [1], new HLCTimestamp(1, 1, 0), 20_000), ct);
            Assert.Equal(KeyValueResponseType.InvalidInput, tooLong.Type);

            HistoryHoldAnswer tooMany = await Hold(nodes[0], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-g", [1, 2, 3], new HLCTimestamp(1, 1, 0), 5_000), ct);
            Assert.Equal(KeyValueResponseType.HistoryHoldLimitReached, tooMany.Type);

            HistoryHoldAnswer two = await Hold(nodes[1], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-g", [1, 2], new HLCTimestamp(1, 1, 0), 5_000), ct);
            Assert.Equal(KeyValueResponseType.Set, two.Type);
            Assert.Equal(2, two.Holds.Count);

            // Re-acquiring partitions the holder already holds is not a new hold.
            HistoryHoldAnswer again = await Hold(nodes[2], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-g", [2, 1], new HLCTimestamp(1, 1, 0), 5_000), ct);
            Assert.Equal(KeyValueResponseType.Set, again.Type);

            HistoryHoldAnswer third = await Hold(nodes[2], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-g", [3], new HLCTimestamp(1, 1, 0), 5_000), ct);
            Assert.Equal(KeyValueResponseType.HistoryHoldLimitReached, third.Type);
        });
    }

    [Fact]
    public async Task HoldsOfAPartitionThatIsGone_ArePurged_AndAreNeverAcquired()
    {
        await WithCluster(async (nodes, ct) =>
        {
            HistoryHoldAnswer unknown = await Hold(nodes[0], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-h", [999], new HLCTimestamp(1, 1, 0), 60_000), ct);
            Assert.Equal(KeyValueResponseType.InvalidInput, unknown.Type);

            HistoryHoldAnswer meta = await Hold(nodes[0], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-h", [0], new HLCTimestamp(1, 1, 0), 60_000), ct);
            Assert.Equal(KeyValueResponseType.InvalidInput, meta.Type);

            HistoryHoldInfo hold = Assert.Single((await Hold(nodes[0], new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-h", [2], new HLCTimestamp(1, 1, 0), 60_000), ct)).Holds);

            // The partition is retired (as after a merge): the next purge removes the live hold.
            KahunaManager metaLeader = await MetaLeader(nodes, ct);
            metaLeader.KeyValues.HistoryHoldStoreForTests.AttachPartitionLiveness(p => p != 2);

            Assert.Equal(1, await metaLeader.KeyValues.PurgeHistoryHoldsAsync(ct));

            foreach (KahunaManager node in nodes)
                await WaitUntilAsync(() => !node.KeyValues.HistoryHoldStoreForTests.Holds.ContainsKey(hold.HoldId));
        });
    }

    [Fact]
    public async Task Hold_SurvivesARestart_AndIsPinnedAgainAsTheRestoreFinishes()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        string storagePath = CreateTempDir("kahuna-history-store-");
        string walPath = CreateTempDir("kahuna-history-wal-");

        try
        {
            string holdId;
            int partitionId;

            {
                await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
                await node.StartAsync(ct);
                await node.WaitForLeaderForKeyAsync("restart/k", ct);

                KahunaManager kahuna = (KahunaManager)node.Kahuna;
                partitionId = kahuna.KeyValues.LocateDurablePartition("restart/k").PartitionId;

                for (int i = 0; i < 5; i++)
                {
                    (KeyValueResponseType set, _, _) = await RetryOnMustRetryAsync(
                        () => node.Kahuna.LocateAndTrySetKeyValue(HLCTimestamp.Zero, "restart/k", Encoding.UTF8.GetBytes($"v{i}"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
                        r => r.Item1);
                    Assert.Equal(KeyValueResponseType.Set, set);
                }

                HistoryHoldAnswer acquired = await Hold(kahuna, new HistoryHoldRequest(HistoryHoldOperation.Acquire, "cdc-restart", [partitionId], new HLCTimestamp(1, 1, 0), 600_000), ct);
                Assert.Equal(KeyValueResponseType.Set, acquired.Type);
                holdId = acquired.Holds[0].HoldId;

                await WaitUntilAsync(() => kahuna.KeyValues.HistoryHoldPinnerForTests!.PinnedIndexes().ContainsKey(partitionId));
            }

            {
                await using EmbeddedKahunaNode node = new(PersistentOptions(storagePath, walPath), loggerFactory);
                await node.StartAsync(ct);

                KahunaManager kahuna = (KahunaManager)node.Kahuna;

                // Back from the durable copy, and pinned once the partition's restore finished.
                Assert.True(kahuna.KeyValues.HistoryHoldStoreForTests.Holds.ContainsKey(holdId));
                await WaitUntilAsync(() => kahuna.KeyValues.HistoryHoldPinnerForTests!.PinnedIndexes().ContainsKey(partitionId));
            }
        }
        finally
        {
            TryDeleteDir(storagePath);
            TryDeleteDir(walPath);
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private async Task WithCluster(Func<KahunaManager[], CancellationToken, Task> body, Action<Configuration.KahunaConfiguration>? configure = null)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger, configure);

        try
        {
            await body([(KahunaManager)k1, (KahunaManager)k2, (KahunaManager)k3], ct);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    private static async Task<HistoryHoldAnswer> Hold(KahunaManager node, HistoryHoldRequest request, CancellationToken ct) =>
        await RetryOnMustRetryAsync(() => node.HistoryHold(request, ct), a => a.Type);

    // Writes a few values under one key and answers the key's partition, so its WAL holds entries to pin.
    private static async Task<int> WriteSome(KahunaManager[] nodes, string prefix, CancellationToken ct)
    {
        string key = $"{prefix}/{Guid.NewGuid():N}";

        for (int i = 0; i < 5; i++)
        {
            (KeyValueResponseType type, _, _) = await RetryOnMustRetryAsync(
                () => nodes[i % nodes.Length].LocateAndTrySetKeyValue(HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes($"v{i}"), null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent, ct),
                r => r.Item1);
            Assert.Equal(KeyValueResponseType.Set, type);
        }

        return nodes[0].KeyValues.LocateDurablePartition(key).PartitionId;
    }

    // Waits until the node pins the partition at the index a resume from the timestamp needs on its own WAL.
    private static Task WaitPinnedAt(KahunaManager node, int partitionId, HLCTimestamp timestamp)
    {
        HistoryHoldPinner pinner = node.KeyValues.HistoryHoldPinnerForTests!;
        return WaitUntilAsync(() => pinner.PinnedIndexes().TryGetValue(partitionId, out long index) && index == pinner.IndexFor(partitionId, timestamp));
    }

    private static HLCTimestamp Now(KahunaManager node) =>
        node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());

    private static async Task<KahunaManager> MetaLeader(KahunaManager[] nodes, CancellationToken ct)
    {
        while (true)
        {
            foreach (KahunaManager node in nodes)
            {
                if (await node.Raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
                    return node;
            }

            await Task.Delay(50, ct);
        }
    }

    private static async Task<KahunaManager> NotMetaLeader(KahunaManager[] nodes, CancellationToken ct)
    {
        KahunaManager leader = await MetaLeader(nodes, ct);
        return nodes.First(n => !ReferenceEquals(n, leader));
    }

    private static EmbeddedKahunaOptions PersistentOptions(string storagePath, string walPath) => new()
    {
        InitialPartitions = 1,
        Storage = "sqlite",
        StoragePath = storagePath,
        StorageRevision = "history-holds",
        WalStorage = "sqlite",
        WalPath = walPath,
        WalRevision = "history-holds-wal",
        WalSyncWrites = true,
        CollectionInterval = TimeSpan.FromMinutes(10)
    };

    private static string CreateTempDir(string prefix)
    {
        string path = Path.Combine(Path.GetTempPath(), prefix + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDir(string path)
    {
        try { if (Directory.Exists(path)) Directory.Delete(path, recursive: true); }
        catch { /* best-effort cleanup */ }
    }
}

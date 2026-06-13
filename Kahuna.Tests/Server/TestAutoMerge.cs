
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Acceptance tests for the automatic merge trigger (RangeMergeTrigger / TriggerAutoMergeAsync).
///
/// <para>
/// <b>Leader requirement.</b> RangeMergeTrigger.TriggerAsync guards itself with
/// <c>AmILeader(MetaPartitionId)</c>; since the meta map shares the system partition (P0,
/// Kommander 0.11.0+), a single P0 leadership grant covers both partition lifecycle and the
/// descriptor cutover. A node that is not the P0 leader returns 0 immediately. To avoid vacuous
/// passes the tests use <c>ForceLeaderForTestingAsync</c> to put P0 leadership on nodes[0] before
/// exercising the trigger.
/// </para>
/// </summary>
public sealed class TestAutoMerge : BaseCluster
{
    private readonly ILogger<IRaft>   raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestAutoMerge(ITestOutputHelper outputHelper)
    {
        ILoggerFactory lf = LoggerFactory.Create(b =>
            b.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));
        raftLogger   = lf.CreateLogger<IRaft>();
        kahunaLogger = lf.CreateLogger<IKahuna>();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(
        int partition, (IRaft, KahunaManager)[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeader(partition, ct))
                    return (raft, kahuna);
            await Task.Delay(50, ct);
        }
    }

    private static async Task WaitFor(Func<bool> predicate, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate()) return;
            await Task.Delay(25, ct);
        }
        Assert.Fail("Timed out waiting for condition.");
    }

    /// <summary>
    /// Forces <paramref name="target"/> to become leader of the system/meta partition (P0), then
    /// waits until the role is confirmed. Since Kommander 0.11.0 the meta map shares P0, so a single
    /// P0 leadership grant satisfies both <c>CreatePartitionAsync</c>/<c>RemovePartitionAsync</c>
    /// (system-leader) and the descriptor cutover (meta-leader) — no dual-partition colocation.
    /// Uses <c>ForceLeaderForTestingAsync</c> — deterministic, not probabilistic.
    /// </summary>
    private static async Task<KahunaManager> ForceMetaLeaderAsync(
        (IRaft Raft, KahunaManager Kahuna) target, CancellationToken ct)
    {
        await target.Raft.ForceLeaderForTestingAsync(RangeMapStore.MetaPartitionId, ct);

        long deadline = Environment.TickCount64 + 10_000;
        while (Environment.TickCount64 < deadline)
        {
            if (await target.Raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
                return target.Kahuna;
            await Task.Delay(25, ct);
        }
        Assert.Fail("ForceMetaLeaderAsync: nodes[0] did not become P0 leader within 10 s.");
        return null!; // unreachable
    }

    /// <summary>
    /// Assembles a 4-partition 3-node cluster, registers <paramref name="space"/> as KeyRange,
    /// seeds two adjacent descriptors on P2 and P3, each holding <paramref name="keysPerRange"/>
    /// keys, and waits for all keys to be visible on every node.
    /// </summary>
    private async Task<((IRaft, KahunaManager)[] Nodes, List<string> Keys)> SetupTwoRanges(
        string space, int keysPerRange)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            kahuna.RegisterKeyRange(space);

        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

        // Split key sits between the two halves.
        string splitKey = $"{space}/{keysPerRange:D4}";

        // Two adjacent descriptors: [null, splitKey) on P2 and [splitKey, null) on P3.
        bool committed = await metaLeader.RangeMapStore.MutateAsync(
            _ => [
                new RangeDescriptor
                {
                    KeySpace    = space,
                    StartKey    = null,
                    EndKey      = splitKey,
                    PartitionId = RangeMapStore.FirstDataPartitionId,
                    Generation  = 1
                },
                new RangeDescriptor
                {
                    KeySpace    = space,
                    StartKey    = splitKey,
                    EndKey      = null,
                    PartitionId = RangeMapStore.FirstDataPartitionId + 1,
                    Generation  = 1
                }
            ], ct);
        Assert.True(committed);

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            await WaitFor(() => kahuna.RangeMapStore.Current.FindAll(space).Count == 2);

        (IRaft _, KahunaManager leftLeader) =
            await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);
        (IRaft _, KahunaManager rightLeader) =
            await LeaderOf(RangeMapStore.FirstDataPartitionId + 1, nodes);

        var keys = new List<string>(keysPerRange * 2);

        for (int i = 0; i < keysPerRange; i++)
        {
            string key = $"{space}/{i:D4}";
            (KeyValueResponseType t, _, _) = await leftLeader.TrySetKeyValue(
                HLCTimestamp.Zero, key,
                Encoding.UTF8.GetBytes("v" + i),
                null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, t);
            keys.Add(key);
        }

        for (int i = keysPerRange; i < keysPerRange * 2; i++)
        {
            string key = $"{space}/{i:D4}";
            (KeyValueResponseType t, _, _) = await rightLeader.TrySetKeyValue(
                HLCTimestamp.Zero, key,
                Encoding.UTF8.GetBytes("v" + i),
                null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, t);
            keys.Add(key);
        }

        keys.Sort(StringComparer.Ordinal);

        // Wait for all keys to be readable on every node (replication lag).
        foreach (string key in keys)
        {
            string k = key;
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                KahunaManager km = kahuna;
                long deadline = Environment.TickCount64 + 10_000;
                while (Environment.TickCount64 < deadline)
                {
                    (KeyValueResponseType rt, _) =
                        await km.TryGetValue(HLCTimestamp.Zero, k, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                    if (rt == KeyValueResponseType.Get) break;
                    await Task.Delay(25, ct);
                }
            }
        }

        return (nodes, keys);
    }

    // ── UnderMin_TriggersExactlyOneMerge ─────────────────────────────────────

    /// <summary>
    /// Two adjacent under-min ranges are merged into one by <c>TriggerAutoMergeAsync</c>.
    /// After the trigger the map contains a single full-range descriptor (null start, null end).
    /// </summary>
    [Fact]
    public async Task UnderMin_TriggersExactlyOneMerge()
    {
        const string space        = "am:a";
        const int    keysPerRange = 3;   // well below minMergeSize
        const int    minMergeSize = 10;

        ((IRaft, KahunaManager)[] nodes, _) = await SetupTwoRanges(space, keysPerRange);
        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            // Deterministically put system/meta (P0) leadership on nodes[0] so the trigger runs.
            KahunaManager dualLeader = await ForceMetaLeaderAsync(nodes[0], ct);

            int merges = await dualLeader.TriggerAutoMergeAsync(minMergeSize, ct);
            Assert.Equal(1, merges);

            // The map must collapse to a single full-range descriptor.
            await WaitFor(() => dualLeader.RangeMapStore.Current.FindAll(space).Count == 1);

            IReadOnlyList<RangeDescriptor> descriptors = dualLeader.RangeMapStore.Current.FindAll(space);
            Assert.Single(descriptors);
            Assert.Null(descriptors[0].StartKey);
            Assert.Null(descriptors[0].EndKey);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── AboveMin_NoMerge ──────────────────────────────────────────────────────

    /// <summary>
    /// When both ranges have at least <c>minMergeSize</c> keys, <c>TriggerAutoMergeAsync</c>
    /// returns 0 and the map retains both descriptors unchanged.
    /// </summary>
    [Fact]
    public async Task AboveMin_NoMerge()
    {
        const string space        = "am:b";
        const int    keysPerRange = 15;  // above minMergeSize
        const int    minMergeSize = 10;

        ((IRaft, KahunaManager)[] nodes, _) = await SetupTwoRanges(space, keysPerRange);
        (IRaft r1, IRaft r2, IRaft r3) =
            (nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            // Force P0 (system/meta) leadership so the trigger actually runs (not silently skipped).
            KahunaManager dualLeader = await ForceMetaLeaderAsync(nodes[0], ct);

            int merges = await dualLeader.TriggerAutoMergeAsync(minMergeSize, ct);
            Assert.Equal(0, merges);

            // Both descriptors must still be present on every node.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                Assert.Equal(2, kahuna.RangeMapStore.Current.FindAll(space).Count);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}

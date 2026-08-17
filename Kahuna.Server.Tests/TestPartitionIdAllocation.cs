
using System.Text;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance tests for how a split picks the partition ID it creates.
///
/// <para>
/// The rule under test: an ID is spent the moment a partition is created with it, and stays spent
/// even after that partition is retired — Kommander keeps a tombstone and refuses to recreate the
/// ID. A range descriptor, by contrast, disappears when its range is merged away or when a split is
/// rolled back. Deriving the next ID from the descriptor set therefore hands out an ID whose
/// creation is refused forever, wedging every later split of that key space; deriving it from the
/// partition map does not.
/// </para>
/// </summary>
public sealed class TestPartitionIdAllocation : BaseCluster
{
    private readonly ILogger<IRaft>   raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestPartitionIdAllocation(ITestOutputHelper outputHelper)
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

    /// <summary>
    /// Assembles a 3-node cluster, registers <paramref name="space"/> as a key range, seeds one
    /// full-range descriptor and writes <paramref name="count"/> keys, waiting until every key is
    /// readable from every node.
    /// </summary>
    private async Task<(IRaft, KahunaManager)[]> SetupWithKeys(string space, int count)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 3, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            kahuna.RegisterKeyRange(space);

        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

        bool committed = await metaLeader.RangeMapStore.MutateAsync(
            _ => [new RangeDescriptor
            {
                KeySpace    = space,
                StartKey    = null,
                EndKey      = null,
                PartitionId = RangeMapStore.FirstDataPartitionId,
                Generation  = 1
            }], ct);
        Assert.True(committed);

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            await WaitUntilAsync(() => kahuna.RangeMapStore.Current.Find(space, space + "/x") is not null);

        (IRaft _, KahunaManager dataLeader) = await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);

        for (int i = 0; i < count; i++)
        {
            (KeyValueResponseType t, _, _) = await dataLeader.TrySetKeyValue(
                HLCTimestamp.Zero, $"{space}/{i:D4}",
                Encoding.UTF8.GetBytes("v" + i),
                null, -1, KeyValueFlags.Set, 0, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Set, t);
        }

        for (int i = 0; i < count; i++)
        {
            string key = $"{space}/{i:D4}";
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
            {
                KahunaManager km = kahuna;
                await WaitUntilAsync(async () =>
                {
                    (KeyValueResponseType rt, _) = await km.TryGetValue(
                        HLCTimestamp.Zero, key, 0, HLCTimestamp.Zero, KeyValueDurability.Persistent);
                    return rt == KeyValueResponseType.Get;
                });
            }
        }

        return nodes;
    }

    /// <summary>
    /// Splits through the manual (operator) path, re-resolving the P0 leader between attempts.
    /// Retries only paper over a leadership change — an ID that is genuinely blocked fails every
    /// attempt, which is exactly what these tests assert on.
    /// </summary>
    private static async Task<SplitOutcome> ForceSplitAsync(
        (IRaft, KahunaManager)[] nodes, string space, string splitKey, CancellationToken ct)
    {
        SplitOutcome outcome = SplitOutcome.PartitionCreationFailed;

        for (int attempt = 0; attempt < 5; attempt++)
        {
            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            outcome = await metaLeader.ForceSplitAtKeyAsync(space, splitKey, ct: ct);

            if (outcome.IsSuccess)
                return outcome;

            await Task.Delay(100, ct);
        }

        return outcome;
    }

    /// <summary>Merges the space back into one range, returning the retired partition ID.</summary>
    private static async Task<int> MergeBackAsync(
        (IRaft, KahunaManager)[] nodes, string space, int expectedRetiredId, CancellationToken ct)
    {
        int merges = 0;

        for (int attempt = 0; attempt < 10 && merges == 0; attempt++)
        {
            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            // A min size far above the key count makes both halves merge candidates.
            merges = await metaLeader.TriggerAutoMergeAsync(minMergeSize: 1_000, ct);
            if (merges == 0) await Task.Delay(100, ct);
        }

        Assert.Equal(1, merges);

        (IRaft sysRaft, KahunaManager leader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
        await WaitUntilAsync(() => leader.RangeMapStore.Current.FindAll(space).Count == 1);

        // The merged-away partition is retired: its entry survives as a tombstone, which reads back
        // as generation 0 and can never be recreated.
        await WaitUntilAsync(() => sysRaft.GetPartitionGeneration(expectedRetiredId) == 0);

        return expectedRetiredId;
    }

    // ── manual split after a merge ───────────────────────────────────────────

    /// <summary>
    /// Split, let the merge checker fold the halves back, split again. The second split must land on
    /// a partition ID that was never used — reusing the retired one is refused outright.
    /// </summary>
    [Fact]
    public async Task SplitThenMergeThenSplit_LandsOnAFreshPartitionId()
    {
        const string space = "pid:a";

        (IRaft, KahunaManager)[] nodes = await SetupWithKeys(space, 6);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            SplitOutcome first = await ForceSplitAsync(nodes, space, space + "/0003", ct);
            Assert.True(first.IsSuccess, $"first split failed: {first.Status}");

            int retiredId = first.NewPartitionId;

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.FindAll(space).Count == 2);

            await MergeBackAsync(nodes, space, retiredId, ct);

            SplitOutcome second = await ForceSplitAsync(nodes, space, space + "/0003", ct);

            Assert.True(second.IsSuccess, $"split after the merge failed: {second.Status}");
            Assert.NotEqual(retiredId, second.NewPartitionId);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── automatic split after a merge ────────────────────────────────────────

    /// <summary>
    /// The same situation reached through the size-based trigger rather than the operator surface:
    /// auto-split shares the allocation path, and a key space that has been split and merged back
    /// must still split automatically.
    /// </summary>
    [Fact]
    public async Task AutoSplitAfterAMerge_StillSplits()
    {
        const string space = "pid:b";

        (IRaft, KahunaManager)[] nodes = await SetupWithKeys(space, 12);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            SplitOutcome first = await ForceSplitAsync(nodes, space, space + "/0006", ct);
            Assert.True(first.IsSuccess, $"first split failed: {first.Status}");

            int retiredId = first.NewPartitionId;

            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntilAsync(() => kahuna.RangeMapStore.Current.FindAll(space).Count == 2);

            await MergeBackAsync(nodes, space, retiredId, ct);

            int splits = 0;
            for (int attempt = 0; attempt < 20 && splits == 0; attempt++)
            {
                (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
                splits = await metaLeader.TriggerAutoSplitAsync(threshold: 4, minRangeSize: 2, ct);
                if (splits == 0) await Task.Delay(100, ct);
            }

            Assert.Equal(1, splits);

            (IRaft sysRaft, KahunaManager leader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            await WaitUntilAsync(() => leader.RangeMapStore.Current.FindAll(space).Count == 2);

            // Whatever partition the new range landed on, it is live — not the retired one.
            foreach (RangeDescriptor descriptor in leader.RangeMapStore.Current.FindAll(space))
            {
                Assert.NotEqual(retiredId, descriptor.PartitionId);
                Assert.NotEqual(0, sysRaft.GetPartitionGeneration(descriptor.PartitionId));
            }
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── split rolled back after the partition was created ────────────────────

    /// <summary>
    /// A split that fails after creating its partition retires that ID through the orphan cleanup,
    /// without ever committing a descriptor for it. The next split must move on to a new ID instead
    /// of recomputing the retired one — otherwise a single failed split wedges the splitter for good.
    /// </summary>
    [Fact]
    public async Task SplitRolledBackAfterCreate_DoesNotBurnThePartitionId()
    {
        const string space = "pid:c";

        (IRaft, KahunaManager)[] nodes = await SetupWithKeys(space, 10);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            (IRaft sysRaft, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            Assert.True(await sysRaft.AmILeader(RangeMapStore.MetaPartitionId, ct));

            RangeDescriptor current = metaLeader.RangeMapStore.Current.Find(space, space + "/0000")!;
            int abandonedId = RangeSplitter.ComputeNextPartitionId(sysRaft, metaLeader.RangeMapStore.Current);

            // "/zzzz" is past every key, so the right half is empty: SplitAsync fails with
            // BelowMinRangeSize *after* the partition was created, and the cleanup retires it.
            bool didSplit = await metaLeader.RangeSplitTrigger.ExecuteSplitAsync(current, space + "/zzzz", ct);
            Assert.False(didSplit, "a split with an empty half must fail");

            await WaitUntilAsync(() => sysRaft.GetPartitionGeneration(abandonedId) == 0);

            // No descriptor ever referenced the abandoned ID, so a descriptor-derived allocation
            // would offer it again — and be refused.
            SplitOutcome next = await ForceSplitAsync(nodes, space, space + "/0005", ct);

            Assert.True(next.IsSuccess, $"split after the rollback failed: {next.Status}");
            Assert.NotEqual(abandonedId, next.NewPartitionId);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── manual and automatic splits share one gate ───────────────────────────

    /// <summary>
    /// The operator split and the size-based trigger allocate under the same lock. Without it both
    /// can pick the same partition ID, and the branch that loses the cutover runs its orphan cleanup
    /// against the ID the winner just put into service — retiring a partition that holds live data.
    /// </summary>
    [Fact]
    public async Task ManualAndAutomaticSplits_AllocateUnderOneGate()
    {
        const string space = "pid:d";

        (IRaft, KahunaManager)[] nodes = await SetupWithKeys(space, 12);

        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            (IRaft sysRaft, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            RangeDescriptor descriptor = metaLeader.RangeMapStore.Current.Find(space, space + "/0000")!;

            Task<bool>? automatic = null;
            bool automaticWasBlocked = false;

            // Start the automatic split from inside the manual split's quiesce window: it must not
            // get past the gate while the manual split still holds it.
            SplitOutcome manual = await metaLeader.ForceSplitAtKeyAsync(
                space,
                space + "/0006",
                duringQuiesce: async () =>
                {
                    automatic = metaLeader.RangeSplitTrigger.ExecuteSplitAsync(descriptor, space + "/0003", ct);
                    await Task.Delay(300, ct);
                    automaticWasBlocked = !automatic.IsCompleted;
                },
                ct: ct);

            Assert.True(manual.IsSuccess, $"manual split failed: {manual.Status}");
            Assert.NotNull(automatic);
            Assert.True(automaticWasBlocked, "the automatic split ran while the manual split held the gate");

            // It runs once the manual split releases the gate; it then finds its descriptor stale.
            await automatic!;

            // The decisive invariant: nothing removed a partition a descriptor still points at.
            (IRaft _, KahunaManager leader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

            foreach (RangeDescriptor live in leader.RangeMapStore.Current.FindAll(space))
                Assert.NotEqual(0, sysRaft.GetPartitionGeneration(live.PartitionId));

            Assert.True(leader.RangeMapStore.Current.IsValid);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }
}

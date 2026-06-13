using Kahuna.Server.KeyValues.Ranges;
using Kommander;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Tests for the key-order router <c>LocateRange</c> + the per-key-space registry. The
/// router resolves a key to <c>(partitionId, generation)</c> through the range-descriptor map for
/// key-range spaces and falls back to the hash router (<c>GetPartitionKey</c>) for hash spaces — the
/// two coexist. No caller is switched yet; these tests exercise the function and the
/// single-source-of-truth that keeps the locator and the proposal actor from drifting.
/// </summary>
public sealed class TestLocateRange : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestLocateRange(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    /// <summary>Finds the node that currently leads the meta partition (where the map is mutated).</summary>
    private static async Task<(IRaft Raft, KahunaManager Kahuna)> MetaLeader(
        (IRaft, KahunaManager)[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (await raft.AmILeader(RangeMapStore.MetaPartitionId, ct))
                    return (raft, kahuna);

            await Task.Delay(50, ct);
        }
    }

    private static RangeDescriptor WholeSpace(string keySpace, int partitionId, long generation) => new()
    {
        KeySpace = keySpace,
        StartKey = null,
        EndKey = null,
        PartitionId = partitionId,
        Generation = generation
    };

    // ── KeyRangeSpace_ReturnsDescriptorPartitionAndGeneration ─────────────────────

    [Fact]
    public async Task KeyRangeSpace_ReturnsDescriptorPartitionAndGeneration()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 2, raftLogger, kahunaLogger);
        try
        {
            (IRaft _, KahunaManager leader) = await MetaLeader(
                [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)]);

            leader.RegisterKeyRange("t:r");

            bool committed = await leader.RangeMapStore.MutateAsync(
                _ => [WholeSpace("t:r", partitionId: 2, generation: 7)],
                TestContext.Current.CancellationToken);
            Assert.True(committed);

            (int partitionId, long generation) = leader.LocateRange("t:r/0001");

            Assert.Equal(2, partitionId);
            Assert.Equal(7L, generation);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── RegisterKeyRangeAsync_AutoSeedsInitialDescriptor ─────────────────────────

    [Fact]
    public async Task RegisterKeyRangeAsync_AutoSeedsInitialDescriptor()
    {
        const int partitions = 4;
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", partitions, raftLogger, kahunaLogger);
        try
        {
            (IRaft, KahunaManager)[] nodes =
                [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];
            (IRaft _, KahunaManager leader) = await MetaLeader(nodes);

            // No manual MutateAsync: registration alone seeds the whole-space descriptor.
            bool seeded = await leader.RegisterKeyRangeAsync("t:r", TestContext.Current.CancellationToken);
            Assert.True(seeded);

            (int partitionId, long generation) = leader.LocateRange("t:r/0001");
            Assert.True(partitionId >= RangeMapStore.FirstDataPartitionId, "seed must land on a data partition (>= 1)");
            Assert.True(generation >= 1, "seed generation must be a real fence (>= 1)");

            // The same key always resolves to the single seeded range.
            Assert.Equal((partitionId, generation), leader.LocateRange("t:r/9999"));

            // Idempotent: a second call commits nothing new.
            Assert.False(await leader.RegisterKeyRangeAsync("t:r", TestContext.Current.CancellationToken));
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── RegisterKeyRangeAsync_SinglePartition_SeedsOnP1 ──────────────────────────

    [Fact]
    public async Task RegisterKeyRangeAsync_SinglePartition_SeedsOnP1()
    {
        // Since the meta map shares the system partition (P0), a single user partition (P1) can host
        // ranged data: InitialPartitions = 1 is now fully supported and registration seeds on P1
        // rather than degrading to a hash-routed no-op.
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 1, raftLogger, kahunaLogger);
        try
        {
            (IRaft _, KahunaManager leader) = await MetaLeader(
                [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)]);

            Assert.True(await leader.RegisterKeyRangeAsync("t:r", TestContext.Current.CancellationToken));

            // Key-range routed onto the single data partition (P1) with a real fence.
            (int partitionId, long generation) = leader.LocateRange("t:r/0001");
            Assert.Equal(RangeMapStore.FirstDataPartitionId, partitionId); // P1
            Assert.True(generation >= 1);
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── HashSpace_RoutesAcrossUserPartitionPool ──────────────────────────────────

    [Fact]
    public async Task HashSpace_RoutesAcrossUserPartitionPool()
    {
        // 4 partitions → user partitions {1,2,3,4}. Partition 0 is Kommander's system partition.
        const int partitions = 4;
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", partitions, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await MetaLeader(
                [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)]);

            // "h/x" belongs to key space "h", which is never registered → Hash routing. Kahuna owns
            // hash assignment (not GetPartitionKey), mapping onto the user partitions [1, N]. P0 hosts
            // the meta map + system coordinator and is never in the data pool.
            (int partitionId, long generation) = leader.LocateRange("h/x");

            Assert.InRange(partitionId, RangeMapStore.FirstDataPartitionId, partitions); // 1..4
            Assert.Equal(0L, generation);                                                // hash carries no fence

            // Deterministic and within the pool across many key spaces.
            for (int i = 0; i < 50; i++)
            {
                (int p, _) = leader.LocateRange($"space{i}/row");
                Assert.InRange(p, RangeMapStore.FirstDataPartitionId, partitions);
                (int again, _) = leader.LocateRange($"space{i}/row");
                Assert.Equal(p, again);
            }
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }

    // ── LocateRange_AndProposalActor_AgreeOnPartition ────────────────────────────

    [Fact]
    public async Task LocateRange_AndProposalActor_AgreeOnPartition()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 2, raftLogger, kahunaLogger);
        try
        {
            (IRaft leaderRaft, KahunaManager leader) = await MetaLeader(
                [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)]);

            leader.RegisterKeyRange("t:r");
            Assert.True(await leader.RangeMapStore.MutateAsync(
                _ => [WholeSpace("t:r", partitionId: 2, generation: 3)],
                TestContext.Current.CancellationToken));

            const string rangedKey = "t:r/0001";

            // The locator's request-routing path.
            (int PartitionId, long Generation) viaLocator = leader.LocateRange(rangedKey);

            // The proposal actor's replicate-into path will call the SAME shared resolver.
            // Resolving through it must yield the identical partition+generation — no drift.
            (int PartitionId, long Generation) viaProposalPath = RangeRouting.Locate(
                leader.KeySpaceRegistry, leader.RangeMapStore.Current,
                new DataPartitionRouter(leaderRaft), rangedKey);

            Assert.Equal(viaLocator, viaProposalPath);
            Assert.Equal(2, viaProposalPath.PartitionId); // the descriptor's partition, not the hash partition
        }
        finally
        {
            await LeaveCluster(r1, r2, r3);
        }
    }
}

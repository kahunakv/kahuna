using System.Text;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Tests for the key-range generation fence (Task 4). The fence uses Kahuna's replicated descriptor
/// map (not Kommander's partition generation): a persistent key-range write carries the
/// <see cref="RangeDescriptor.Generation"/> it routed on, and the proposal actor on the leader rejects
/// it with <c>MustRetry</c> if the current descriptor generation no longer matches — the range
/// moved/split since routing. Hash spaces are unaffected.
/// </summary>
public sealed class TestGenerationFence : BaseCluster
{
    private const string Space = "t:r";
    private const int DataPartition = 2;   // descriptor partition (>= FirstDataPartitionId)
    private const long SeedGeneration = 5;

    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestGenerationFence(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

    private static async Task<(IRaft Raft, KahunaManager Kahuna)> LeaderOf(int partition, (IRaft, KahunaManager)[] nodes)
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

    /// <summary>Assembles a 4-partition cluster, registers the ranged space on every node, seeds one descriptor.</summary>
    private async Task<((IRaft, KahunaManager)[] Nodes, KahunaManager DataLeader)> Setup()
    {
        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            kahuna.RegisterKeyRange(Space);

        (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);

        bool committed = await metaLeader.RangeMapStore.MutateAsync(
            _ => [new RangeDescriptor { KeySpace = Space, StartKey = null, EndKey = null, PartitionId = DataPartition, Generation = SeedGeneration }],
            TestContext.Current.CancellationToken);
        Assert.True(committed);

        // Wait for the descriptor to replicate to every node's map.
        foreach ((IRaft _, KahunaManager kahuna) in nodes)
            await WaitUntil(() => kahuna.RangeMapStore.Current.Find(Space, Space + "/k")?.Generation == SeedGeneration);

        (IRaft _, KahunaManager dataLeader) = await LeaderOf(DataPartition, nodes);
        return (nodes, dataLeader);
    }

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 5000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (predicate())
                return;
            await Task.Delay(25, ct);
        }
        Assert.Fail("Timed out waiting for the range map to converge.");
    }

    private static byte[] V(string s) => Encoding.UTF8.GetBytes(s);

    // ── StaleGeneration_WriteRejectedWithMustRetry ───────────────────────────────

    [Fact]
    public async Task StaleGeneration_WriteRejectedWithMustRetry()
    {
        ((IRaft, KahunaManager)[] nodes, KahunaManager dataLeader) = await Setup();
        try
        {
            // A write carrying a stale generation (the range bumped to SeedGeneration since) is fenced.
            (KeyValueResponseType type, _, _) = await dataLeader.TrySetKeyValueRanged(
                HLCTimestamp.Zero, Space + "/0001", V("v"), routedGeneration: SeedGeneration - 1);

            Assert.Equal(KeyValueResponseType.MustRetry, type);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── ProposalActor_RoutesRangedKeyViaLocateRange_WithGeneration ────────────────

    [Fact]
    public async Task ProposalActor_RoutesRangedKeyViaLocateRange_WithGeneration()
    {
        ((IRaft, KahunaManager)[] nodes, KahunaManager dataLeader) = await Setup();
        try
        {
            // The ranged key resolves to the descriptor's partition + generation, not GetPartitionKey's.
            (int partitionId, long generation) = dataLeader.LocateRange(Space + "/0001");
            Assert.Equal(DataPartition, partitionId);
            Assert.Equal(SeedGeneration, generation);

            // A write carrying that generation passes the fence and replicates into the descriptor partition.
            (KeyValueResponseType type, _, _) = await dataLeader.TrySetKeyValueRanged(
                HLCTimestamp.Zero, Space + "/0001", V("v"), routedGeneration: SeedGeneration);
            Assert.Equal(KeyValueResponseType.Set, type);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── MustRetry_RefreshesCacheAndLandsOnCorrectPartition ───────────────────────

    [Fact]
    public async Task MustRetry_RefreshesCacheAndLandsOnCorrectPartition()
    {
        ((IRaft, KahunaManager)[] nodes, KahunaManager dataLeader) = await Setup();
        try
        {
            // Stale route → MustRetry.
            (KeyValueResponseType stale, _, _) = await dataLeader.TrySetKeyValueRanged(
                HLCTimestamp.Zero, Space + "/0001", V("v"), routedGeneration: SeedGeneration - 1);
            Assert.Equal(KeyValueResponseType.MustRetry, stale);

            // Re-resolve LocateRange (fresh generation) and retry → commits on the descriptor partition.
            (int _, long fresh) = dataLeader.LocateRange(Space + "/0001");
            (KeyValueResponseType retried, _, _) = await dataLeader.TrySetKeyValueRanged(
                HLCTimestamp.Zero, Space + "/0001", V("v"), routedGeneration: fresh);

            Assert.Equal(KeyValueResponseType.Set, retried);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── Fence_NeverDoubleApplies ─────────────────────────────────────────────────

    [Fact]
    public async Task Fence_NeverDoubleApplies()
    {
        ((IRaft, KahunaManager)[] nodes, KahunaManager dataLeader) = await Setup();
        try
        {
            const string key = Space + "/0001";

            // Rejected (stale) write applies nothing.
            (KeyValueResponseType stale, _, _) = await dataLeader.TrySetKeyValueRanged(
                HLCTimestamp.Zero, key, V("a"), routedGeneration: SeedGeneration - 1);
            Assert.Equal(KeyValueResponseType.MustRetry, stale);

            // First accepted write creates the key at revision 0 — proof the rejected one didn't apply.
            (KeyValueResponseType set0, long rev0, _) = await dataLeader.TrySetKeyValueRanged(
                HLCTimestamp.Zero, key, V("b"), routedGeneration: SeedGeneration);
            Assert.Equal(KeyValueResponseType.Set, set0);
            Assert.Equal(0, rev0);

            // Next accepted write increments to revision 1 (writes apply exactly once each).
            (KeyValueResponseType set1, long rev1, _) = await dataLeader.TrySetKeyValueRanged(
                HLCTimestamp.Zero, key, V("c"), routedGeneration: SeedGeneration);
            Assert.Equal(KeyValueResponseType.Set, set1);
            Assert.Equal(1, rev1);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }
}

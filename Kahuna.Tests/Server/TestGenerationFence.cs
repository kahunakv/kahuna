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

    private static async Task<KahunaManager> NonLeaderOf(int partition, (IRaft, KahunaManager)[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach ((IRaft raft, KahunaManager kahuna) in nodes)
                if (!await raft.AmILeader(partition, ct))
                    return kahuna;

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

    // ── BatchSet_ZeroGeneration_LocatorStampsLiveGeneration ──────────────────────

    /// <summary>
    /// A client that sends RoutedGeneration = 0 (the default) relies on the locator to resolve
    /// and stamp the current generation. The item should be Set, confirming the
    /// populate-when-0 branch fires.
    /// </summary>
    [Fact]
    public async Task BatchSet_ZeroGeneration_LocatorStampsLiveGeneration()
    {
        ((IRaft, KahunaManager)[] nodes, KahunaManager dataLeader) = await Setup();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            // RoutedGeneration left at 0 — the locator must stamp freshGeneration from LocateRange.
            List<KahunaSetKeyValueResponseItem> responses = await dataLeader.LocateAndTrySetManyKeyValue(
                [new() { Key = Space + "/zero1", Value = V("v"), RoutedGeneration = 0, Durability = KeyValueDurability.Persistent }],
                ct);

            Assert.Equal(KeyValueResponseType.Set, responses.Single().Type);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── BatchSet_StaleGeneration_ItemFenced ──────────────────────────────────────

    /// <summary>
    /// A batched set whose ranged item carries a pre-split generation is rejected with MustRetry
    /// for that item; the item with the current generation is applied successfully.
    /// </summary>
    [Fact]
    public async Task BatchSet_StaleGeneration_ItemFenced()
    {
        ((IRaft, KahunaManager)[] nodes, KahunaManager dataLeader) = await Setup();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            List<KahunaSetKeyValueRequestItem> items =
            [
                new() { Key = Space + "/batch1", Value = V("v1"), RoutedGeneration = SeedGeneration - 1, Durability = KeyValueDurability.Persistent },
                new() { Key = Space + "/batch2", Value = V("v2"), RoutedGeneration = SeedGeneration,     Durability = KeyValueDurability.Persistent },
            ];

            List<KahunaSetKeyValueResponseItem> responses = await dataLeader.LocateAndTrySetManyKeyValue(items, ct);

            KahunaSetKeyValueResponseItem staleResp = responses.Single(r => r.Key == Space + "/batch1");
            KahunaSetKeyValueResponseItem freshResp = responses.Single(r => r.Key == Space + "/batch2");

            Assert.Equal(KeyValueResponseType.MustRetry, staleResp.Type);
            Assert.Equal(KeyValueResponseType.Set,       freshResp.Type);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── BatchSet_CrossNode_FencedLikeSingleKey ───────────────────────────────────

    /// <summary>
    /// A batch submitted from a non-leader node is redirected to the leader; each item's
    /// RoutedGeneration is preserved across the redirect and the fence behaves identically
    /// to the single-key path.
    /// </summary>
    [Fact]
    public async Task BatchSet_CrossNode_FencedLikeSingleKey()
    {
        ((IRaft, KahunaManager)[] nodes, _) = await Setup();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            // Find a node that is not the data-partition leader so the locator will redirect.
            KahunaManager nonLeader = await NonLeaderOf(DataPartition, nodes);

            // Stale generation via cross-node redirect → MustRetry.
            List<KahunaSetKeyValueResponseItem> staleResponses = await nonLeader.LocateAndTrySetManyKeyValue(
                [new() { Key = Space + "/cross1", Value = V("v"), RoutedGeneration = SeedGeneration - 1, Durability = KeyValueDurability.Persistent }],
                ct);
            Assert.Equal(KeyValueResponseType.MustRetry, staleResponses.Single().Type);

            // Fresh generation via cross-node redirect → Set (same as single-key TrySetKeyValue).
            List<KahunaSetKeyValueResponseItem> freshResponses = await nonLeader.LocateAndTrySetManyKeyValue(
                [new() { Key = Space + "/cross1", Value = V("v"), RoutedGeneration = SeedGeneration, Durability = KeyValueDurability.Persistent }],
                ct);
            Assert.Equal(KeyValueResponseType.Set, freshResponses.Single().Type);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── BatchSet_ZeroGeneration_LocatorStampsCurrent ─────────────────────────────

    /// <summary>
    /// A batched item left with the default <c>RoutedGeneration = 0</c> (what a real client sends)
    /// must be stamped by the locator with the live descriptor generation and applied — proving the
    /// populate-when-0 path, not just the pre-stamped fence path.
    /// </summary>
    [Fact]
    public async Task BatchSet_ZeroGeneration_LocatorStampsCurrent()
    {
        ((IRaft, KahunaManager)[] nodes, KahunaManager dataLeader) = await Setup();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            // RoutedGeneration not set → defaults to 0; the locator resolves the current generation.
            List<KahunaSetKeyValueResponseItem> responses = await dataLeader.LocateAndTrySetManyKeyValue(
                [new() { Key = Space + "/zerogen", Value = V("v"), Durability = KeyValueDurability.Persistent }],
                ct);

            Assert.Equal(KeyValueResponseType.Set, responses.Single().Type);
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

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;
using Microsoft.Extensions.Logging;

namespace Kahuna.Tests.Server;

/// <summary>
/// Tests for Task 9 — the routing mass-switch.
/// Verifies that key-range spaces route through the descriptor map at both the locator and the
/// proposal actor, that schema-log spaces stay hash-routed, and that the registry guard prevents
/// accidental KeyRange registration of schema-log key spaces.
/// </summary>
public sealed class TestKeySpaceRouting : BaseCluster
{
    private readonly ILogger<IRaft> raftLogger;
    private readonly ILogger<IKahuna> kahunaLogger;

    public TestKeySpaceRouting(ITestOutputHelper outputHelper)
    {
        ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            builder.AddXUnit(outputHelper).SetMinimumLevel(LogLevel.Warning));

        raftLogger = loggerFactory.CreateLogger<IRaft>();
        kahunaLogger = loggerFactory.CreateLogger<IKahuna>();
    }

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

    private static async Task WaitUntil(Func<bool> predicate, int timeoutMs = 8000)
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

    // ── Registry_RejectsRangingSchemaLog ─────────────────────────────────────────

    /// <summary>
    /// Attempting to register a schema-log key space as KeyRange must throw. Schema-log spaces
    /// (those whose key space ends in "/meta") must stay hash-routed per the §8 invariant.
    /// This test needs no cluster — the guard is in <see cref="KeySpaceRegistry"/>.
    /// </summary>
    [Fact]
    public void Registry_RejectsRangingSchemaLog()
    {
        KeySpaceRegistry registry = new();
        ArgumentException ex = Assert.Throws<ArgumentException>(
            () => registry.RegisterKeyRange("mydb/meta"));
        Assert.Contains("schema-log", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ── SchemaLog_StaysSinglePartition ───────────────────────────────────────────

    /// <summary>
    /// Keys in a <c>{db}/meta</c> schema-log space must stay hash-routed. The space must not
    /// be in the registry and keys must commit successfully via the normal hash path.
    /// </summary>
    [Fact]
    public async Task SchemaLog_StaysSinglePartition()
    {
        const string MetaKey = "mydb/meta/orders";

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            // The schema-log space must never appear in the registry.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                Assert.Equal(RoutingMode.Hash,
                    kahuna.KeySpaceRegistry.GetMode(KeySpaceRegistry.ExtractKeySpace(MetaKey)));

            // A write to the schema-log key should commit (hash-routed to any data-bearing partition).
            (IRaft _, KahunaManager anyLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            (KeyValueResponseType wt, _, _) = await anyLeader.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, MetaKey, System.Text.Encoding.UTF8.GetBytes("v"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, wt);

            // Verify: LocateRange reports Hash routing (generation == 0 for hash spaces).
            (int _, long gen) = anyLeader.LocateRange(MetaKey);
            Assert.Equal(0L, gen);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }

    // ── RowAndIndexKeys_RouteViaLocateRange ──────────────────────────────────────

    /// <summary>
    /// Keys in a registered KeyRange space must resolve through the descriptor map at both the
    /// locator (<see cref="KahunaManager.LocateRange"/>) and the proposal actor
    /// (verified by the write committing on the descriptor's partition).
    /// </summary>
    [Fact]
    public async Task RowAndIndexKeys_RouteViaLocateRange()
    {
        const string Space = "t:r";
        const string Key = "t:r/0001";
        const long SeedGen = 7;

        (IRaft r1, IRaft r2, IRaft r3, IKahuna k1, IKahuna k2, IKahuna k3) =
            await AssembleThreNodeCluster("memory", 4, raftLogger, kahunaLogger);

        (IRaft, KahunaManager)[] nodes =
            [(r1, (KahunaManager)k1), (r2, (KahunaManager)k2), (r3, (KahunaManager)k3)];

        try
        {
            // Register the key space as KeyRange on all nodes.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                kahuna.RegisterKeyRange(Space);

            // Seed a whole-space descriptor [−∞,+∞) @P2 gen=SeedGen.
            (IRaft _, KahunaManager metaLeader) = await LeaderOf(RangeMapStore.MetaPartitionId, nodes);
            bool committed = await metaLeader.RangeMapStore.MutateAsync(
                _ => [new RangeDescriptor
                {
                    KeySpace = Space,
                    StartKey = null,
                    EndKey = null,
                    PartitionId = RangeMapStore.FirstDataPartitionId,
                    Generation = SeedGen
                }],
                TestContext.Current.CancellationToken);
            Assert.True(committed);

            // Wait for the descriptor to reach every node.
            foreach ((IRaft _, KahunaManager kahuna) in nodes)
                await WaitUntil(() =>
                    kahuna.RangeMapStore.Current.Find(Space, Key)?.Generation == SeedGen);

            // Verify LocateRange returns the descriptor's (partitionId, generation).
            (int partitionId, long gen) = metaLeader.LocateRange(Key);
            Assert.Equal(RangeMapStore.FirstDataPartitionId, partitionId);
            Assert.Equal(SeedGen, gen);

            // Write via LocateAndTrySetKeyValue — must commit (routed to descriptor partition).
            (KeyValueResponseType wt, _, _) = await metaLeader.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, Key, System.Text.Encoding.UTF8.GetBytes("v"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent,
                TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, wt);

            // Read back from the P2 leader to confirm the write landed on the descriptor's partition.
            (IRaft _, KahunaManager dataLeader) = await LeaderOf(RangeMapStore.FirstDataPartitionId, nodes);
            (KeyValueResponseType rt, _) = await dataLeader.TryGetValue(
                HLCTimestamp.Zero, Key, 0, KeyValueDurability.Persistent);
            Assert.Equal(KeyValueResponseType.Get, rt);
        }
        finally
        {
            await LeaveCluster(nodes[0].Item1, nodes[1].Item1, nodes[2].Item1);
        }
    }
}


using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna.Server.Tests;

/// <summary>
/// Verifies cache-coherence invariants for key-value actors across follower apply and leader
/// promotion. When a Raft follower applies a committed log entry via KeyValueReplicator.Replicate,
/// the owning actor's in-memory cache must be updated to the new revision so that a node promoted
/// to leader immediately serves the latest value without a disk round-trip.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestKeyValueFailoverCoherence
{
    private readonly ILogger<IRaft>    raftLogger    = NullLogger<IRaft>.Instance;
    private readonly ILogger<IKahuna>  kahunaLogger  = NullLogger<IKahuna>.Instance;

    private static readonly double TimingScale = GetTimingScale();
    private static double GetTimingScale()
    {
        string? val = Environment.GetEnvironmentVariable("KAHUNA_TEST_TIMING_SCALE");
        return val is not null && double.TryParse(val, out double s) && s >= 1.0 ? s : 1.0;
    }

    private sealed record Node(RaftManager Raft, KahunaManager Kahuna);

    private const int ElectionTimeoutSeedBase = 97000;

    private (RaftManager, KahunaManager) BuildNode(
        int nodeId, int port, string[] peers,
        MemoryInterNodeCommmunication interNode, InMemoryCommunication comm)
    {
        ActorSystem actorSystem = new(logger: raftLogger);

        RaftConfiguration raftCfg = new()
        {
            NodeName              = "coherence" + nodeId,
            NodeId                = nodeId,
            Host                  = "localhost",
            Port                  = port,
            InitialPartitions     = 2,
            StartElectionTimeout  = (int)(50  * TimingScale),
            EndElectionTimeout    = (int)(150 * TimingScale),
            ElectionTimeoutSeed   = ElectionTimeoutSeedBase + nodeId,
            CompactEveryOperations = 1000,
            CompactNumberEntries   = 50,
            EnableQuiescence       = false
        };

        RaftManager raft = new(
            raftCfg,
            new StaticDiscovery([new(peers[0]), new(peers[1])]),
            new InMemoryWAL(raftLogger),
            comm,
            new HybridLogicalClock(),
            raftLogger);

        KahunaConfiguration kahunaConfig = new()
        {
            HttpsCertificate          = "",
            HttpsCertificatePassword  = "",
            LocksWorkers              = 8,
            KeyValueWorkers           = 8,
            BackgroundWriterWorkers   = 1,
            Storage                   = "memory",
            StoragePath               = "/tmp",
            StorageRevision           = Guid.NewGuid().ToString(),
            DefaultTransactionTimeout = 5000,
            ScriptCacheExpiration     = TimeSpan.FromMinutes(1),
        };

        KahunaManager kahuna = new(actorSystem, raft, kahunaConfig, interNode, kahunaLogger);
        raft.OnLogRestored         += kahuna.OnLogRestored;
        raft.OnReplicationReceived += kahuna.OnReplicationReceived;
        raft.OnReplicationError    += kahuna.OnReplicationError;
        raft.OnLeaderChanged       += kahuna.OnLeaderChanged;

        return (raft, kahuna);
    }

    private async Task<Node[]> Assemble()
    {
        MemoryInterNodeCommmunication interNode = new();
        InMemoryCommunication comm = new();

        string[] p1 = ["localhost:9301", "localhost:9302"];
        string[] p2 = ["localhost:9300", "localhost:9302"];
        string[] p3 = ["localhost:9300", "localhost:9301"];

        (RaftManager r1, KahunaManager k1) = BuildNode(1, 9300, p1, interNode, comm);
        (RaftManager r2, KahunaManager k2) = BuildNode(2, 9301, p2, interNode, comm);
        (RaftManager r3, KahunaManager k3) = BuildNode(3, 9302, p3, interNode, comm);

        interNode.SetNodes(new() { { "localhost:9300", k1 }, { "localhost:9301", k2 }, { "localhost:9302", k3 } });
        comm.SetNodes(new() { { "localhost:9300", r1 }, { "localhost:9301", r2 }, { "localhost:9302", r3 } });

        await Task.WhenAll(r1.JoinCluster(), r2.JoinCluster(), r3.JoinCluster());

        // InitialPartitions = 2 → partitions 0 (meta), 1 and 2 (data): wait for all.
        for (int partition = 0; partition <= 2; partition++)
            await WaitForAnyLeader(partition, r1, r2, r3);

        return [new(r1, k1), new(r2, k2), new(r3, k3)];
    }

    /// <summary>Retries the operation until the response is no longer MustRetry, up to a deadline.</summary>
    private static async Task<(KeyValueResponseType, long, HLCTimestamp)> RetrySet(
        Func<Task<(KeyValueResponseType, long, HLCTimestamp)>> op, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (true)
        {
            (KeyValueResponseType type, long rev, HLCTimestamp ts) = await op();
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, rev, ts);
            await Task.Delay(50, ct);
        }
    }

    private static async Task WaitForAnyLeader(int partition, params RaftManager[] rafts)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach (RaftManager raft in rafts)
                if (await raft.AmILeader(partition, ct))
                    return;
            await Task.Delay(50, ct);
        }
    }

    private static async Task<Node> LeaderOf(int partition, Node[] nodes)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        while (true)
        {
            foreach (Node node in nodes)
                if (await node.Raft.AmILeader(partition, ct))
                    return node;
            await Task.Delay(50, ct);
        }
    }

    private static async Task LeaveAll(Node[] nodes)
    {
        foreach (Node node in nodes)
        {
            try { await node.Raft.LeaveCluster(dispose: true); }
            catch (ObjectDisposedException) { }
        }
    }

    /// <summary>
    /// Drain BackgroundWriterActor on <paramref name="node"/> and poll the local actor until
    /// TryGetValue returns the key at <paramref name="expectedRevision"/>. This replaces the
    /// fixed-sleep + flush + manual warm-up: it naturally waits for replication to arrive (the
    /// flush is retried if the backend doesn't have the key yet), warms the actor cache, and
    /// completes as soon as the condition is met rather than burning a fixed wall-clock budget.
    /// </summary>
    private static async Task WarmNodeToRevision(Node node, string key, long expectedRevision,
        KeyValueDurability durability, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (true)
        {
            await node.Kahuna.FlushPersistenceAsync();
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) =
                await node.Kahuna.TryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, durability);
            if (type == KeyValueResponseType.Get && entry?.Revision == expectedRevision)
                return;
            if (Environment.TickCount64 >= deadline)
                Assert.Fail($"Timed out waiting for '{key}' to reach revision {expectedRevision} on node");
            await Task.Delay(20, ct);
        }
    }

    /// <summary>
    /// Poll the local actor (no flush) until TryGetValue returns the key at
    /// <paramref name="expectedRevision"/>. Used after a write to wait for
    /// <c>InvalidateOrApply</c> to update the already-resident cache entry.
    /// </summary>
    private static async Task<ReadOnlyKeyValueEntry> PollNodeToRevision(Node node, string key,
        long expectedRevision, KeyValueDurability durability, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (true)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) =
                await node.Kahuna.TryGetValue(HLCTimestamp.Zero, key, -1, HLCTimestamp.Zero, durability);
            if (type == KeyValueResponseType.Get && entry?.Revision == expectedRevision)
                return entry;
            if (Environment.TickCount64 >= deadline)
                Assert.Fail($"Timed out waiting for '{key}' to reach revision {expectedRevision} on node");
            await Task.Delay(20, ct);
        }
    }

    /// <summary>
    /// A follower's in-memory cache must be updated when it applies a committed TrySet log entry
    /// via <c>CompleteFollowerAppend → OnReplicationReceived → InvalidateOrApply</c>. The test
    /// proves this by: (1) writing a key so all nodes cache it at rev1, (2) updating the key to
    /// rev2, (3) waiting for the <c>InvalidateOrApply</c> messages to propagate to every
    /// follower's actor, and (4) verifying that a local <c>TryGetValue</c> on every node returns
    /// rev2 from the in-memory cache — not the stale rev1 it would return if the cache update
    /// were missing.
    /// </summary>
    [Fact]
    public async Task FollowerApply_InvalidatesResidentEntry()
    {
        Node[] nodes = await Assemble();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            // ── phase 1: write rev1, warm all nodes' caches ────────────────────────────────
            byte[] value1 = "v1"u8.ToArray();
            (KeyValueResponseType t1, long rev1, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, "follower-cache-key", value1,
                    null, 0, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, t1);
            Assert.True(rev1 >= 0, $"rev1 set failed (type={t1})");

            // Drain + warm each node: flush until the backend has the key, then read it into the
            // actor's in-memory Store. The loop retries the flush if replication hasn't arrived yet.
            foreach (Node node in nodes)
                await WarmNodeToRevision(node, "follower-cache-key", rev1, KeyValueDurability.Persistent);

            // ── phase 2: overwrite to rev2 ────────────────────────────────────────────────
            byte[] value2 = "v2"u8.ToArray();
            (KeyValueResponseType t2, long rev2, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, "follower-cache-key", value2,
                    null, 0, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, t2);
            Assert.True(rev2 > rev1, $"rev2={rev2} must exceed rev1={rev1}");

            // ── phase 3: all nodes must see rev2, not the stale rev1 ──────────────────────
            // Poll (no flush) until every actor's cache reflects rev2, which proves InvalidateOrApply
            // updated the resident entry rather than a disk re-read delivering the correct value.
            foreach (Node node in nodes)
            {
                ReadOnlyKeyValueEntry entry =
                    await PollNodeToRevision(node, "follower-cache-key", rev2, KeyValueDurability.Persistent);
                Assert.Equal(value2, entry.Value);
            }
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    /// <summary>
    /// When a node that was a follower at the time of a write is promoted to leader, it must
    /// serve the latest committed revision from its cache — provided the follower had already
    /// applied the entry before becoming leader.
    ///
    /// <para><b>Coverage scope:</b> this test uses <c>PollNodeToRevision</c> to confirm every
    /// follower's cache is at rev2 before <c>StepDownAsync</c> is called, ensuring the
    /// applied-before-promotion path is tested. It therefore covers the
    /// "applied-before-promotion" path only.</para>
    ///
    /// <para><b>Unverified window:</b> a follower that wins a leader election while entries are
    /// committed-but-not-yet-applied (pending WAL completions in the replication queue) can serve
    /// a stale revision during the brief interval before those completions drain. Reproducing that
    /// race requires pausing Kommander's replication queue at a specific point, which is not
    /// available in the current test API. The correct fix is for Kommander's
    /// <c>InvokeLeaderChanged</c> to fire only after all pending follower appends for the
    /// partition have been applied — see <c>KeyValuesManager.OnLeaderChanged</c> for details.</para>
    /// </summary>
    [Fact]
    public async Task PromotedLeader_AfterApply_ServesLatestRevision()
    {
        Node[] nodes = await Assemble();
        try
        {
            CancellationToken ct = TestContext.Current.CancellationToken;

            // ── phase 1: write rev1, warm all caches ────────────────────────────────────
            byte[] value1 = "hello"u8.ToArray();
            (KeyValueResponseType t1, long rev1, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, "coherence-key", value1,
                    null, 0, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, t1);
            Assert.True(rev1 >= 0, $"rev1 set failed (type={t1})");

            foreach (Node node in nodes)
                await WarmNodeToRevision(node, "coherence-key", rev1, KeyValueDurability.Persistent);

            // ── phase 2: overwrite to rev2 — InvalidateOrApply updates all follower caches ─
            byte[] value2 = "world"u8.ToArray();
            (KeyValueResponseType t2, long rev2, _) = await RetrySet(() =>
                nodes[0].Kahuna.LocateAndTrySetKeyValue(
                    HLCTimestamp.Zero, "coherence-key", value2,
                    null, 0, KeyValueFlags.None, 0, KeyValueDurability.Persistent, ct));
            Assert.Equal(KeyValueResponseType.Set, t2);
            Assert.True(rev2 > rev1, $"rev2={rev2} must exceed rev1={rev1}");

            // Wait deterministically for every node's cache to reach rev2 before stepping down.
            // This ensures the "applied-before-promotion" precondition is satisfied, which is
            // what this test covers (see doc-comment for the unverified promotion-window case).
            foreach (Node node in nodes)
                await PollNodeToRevision(node, "coherence-key", rev2, KeyValueDurability.Persistent);

            // ── phase 3: step down all data-partition leaders ────────────────────────────
            // This forces former followers to become leaders. Partition 0 is the meta partition
            // and not involved in key-value routing; step down 1 and 2 (the data partitions).
            for (int p = 1; p <= 2; p++)
            {
                Node oldLeader = await LeaderOf(p, nodes);
                await oldLeader.Raft.StepDownAsync(p, ct);
            }

            // Wait for new leaders to stabilise on both data partitions.
            for (int p = 1; p <= 2; p++)
                await WaitForAnyLeader(p, nodes[0].Raft, nodes[1].Raft, nodes[2].Raft);

            // ── phase 4: read from the cluster — new leaders must serve rev2 ─────────────
            // RetryGet handles any transient MustRetry from the leadership transition.
            (KeyValueResponseType getType, ReadOnlyKeyValueEntry? entry) = await RetryGet(
                () => nodes[0].Kahuna.LocateAndTryGetValue(
                    HLCTimestamp.Zero, "coherence-key", -1, HLCTimestamp.Zero,
                    KeyValueDurability.Persistent, ct));

            Assert.Equal(KeyValueResponseType.Get, getType);
            Assert.NotNull(entry);
            Assert.Equal(rev2, entry!.Revision);
            Assert.Equal(value2, entry.Value);
        }
        finally
        {
            await LeaveAll(nodes);
        }
    }

    private static async Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)> RetryGet(
        Func<Task<(KeyValueResponseType, ReadOnlyKeyValueEntry?)>> op, int timeoutMs = 10_000)
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        long deadline = Environment.TickCount64 + (long)(timeoutMs * TimingScale);
        while (true)
        {
            (KeyValueResponseType type, ReadOnlyKeyValueEntry? entry) = await op();
            if (type != KeyValueResponseType.MustRetry || Environment.TickCount64 >= deadline)
                return (type, entry);
            await Task.Delay(50, ct);
        }
    }
}

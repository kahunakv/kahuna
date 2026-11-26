using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Replication;
using Kommander.System;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The peer guard of the one-phase bundle entry with a stub Raft and in-memory replicas: when it confirms, when it asks
/// again, which replicas it asks, and that a confirmed verdict costs a commit no call.
/// </summary>
public sealed class TestOnePhaseBundlePeerGuard
{
    private const int Partition = 1;

    /// <summary>A replica that answers the question of the guard with <see cref="Answer"/>.</summary>
    private sealed class AnsweringNode : FakeKahunaBase, IKahuna
    {
        private int calls;

        public IReadOnlyList<string>? Answer { get; set; } = OnePhaseBundlePeerGuard.AppliedLogTypes;

        /// <summary>When set, an answer waits until the gate opens.</summary>
        public ManualResetEventSlim? Gate { get; set; }

        public int Calls => Volatile.Read(ref calls);

        public IReadOnlyList<string>? GetAppliedLogTypes()
        {
            Interlocked.Increment(ref calls);
            Gate?.Wait(TimeSpan.FromSeconds(10));
            return Answer;
        }
    }

    private static (PeerGuardStubRaft Raft, MemoryInterNodeCommmunication Transport, Dictionary<string, AnsweringNode> Nodes) Group(params string[] replicas)
    {
        PeerGuardStubRaft raft = new() { LocalEndpoint = "self:1" };
        raft.Replicas = [new RaftReplica { Endpoint = "self:1" }, .. replicas.Select(e => new RaftReplica { Endpoint = e })];

        Dictionary<string, AnsweringNode> nodes = replicas.ToDictionary(e => e, _ => new AnsweringNode());
        MemoryInterNodeCommmunication transport = new();
        transport.SetNodes(nodes.ToDictionary(p => p.Key, p => (IKahuna)p.Value));
        return (raft, transport, nodes);
    }

    private static OnePhaseBundlePeerGuard Guard(PeerGuardStubRaft raft, MemoryInterNodeCommmunication transport, bool enabled = true, bool singleProcess = false, int retryMs = 50, int refreshMs = 60_000) =>
        new(raft, transport, NullLogger<IKahuna>.Instance, enabled, singleProcess, TimeSpan.FromMilliseconds(retryMs), TimeSpan.FromMilliseconds(refreshMs));

    private static async Task WaitUntilAsync(Func<bool> condition, string what, int timeoutMs = 5_000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (!condition())
        {
            if (Environment.TickCount64 > deadline)
                Assert.Fail("Timed out waiting until " + what);

            await Task.Delay(10, TestContext.Current.CancellationToken);
        }
    }

    [Fact]
    public void TurnedOff_NeverProposesTheBundle_AndAsksNobody()
    {
        (PeerGuardStubRaft raft, MemoryInterNodeCommmunication transport, _) = Group("a:1", "b:1");
        using OnePhaseBundlePeerGuard guard = Guard(raft, transport, enabled: false);

        Assert.False(guard.ShouldProposeBundle(Partition, 1));
        guard.OnLeaderChanged(Partition, "self:1");
        Assert.Equal(0, guard.Asks);
        Assert.Equal(0, guard.Fallbacks);
    }

    [Fact]
    public void APartitionWithNoOtherReplica_AndASingleProcessGroup_AreConfirmedAtOnce()
    {
        PeerGuardStubRaft alone = new() { LocalEndpoint = "self:1", Replicas = [new RaftReplica { Endpoint = "self:1" }] };
        using (OnePhaseBundlePeerGuard guard = Guard(alone, new MemoryInterNodeCommmunication()))
        {
            Assert.True(guard.ShouldProposeBundle(Partition, 1));
            Assert.Equal(0, guard.Asks);
        }

        (PeerGuardStubRaft raft, MemoryInterNodeCommmunication transport, _) = Group("witness-1:0", "witness-2:0");
        using (OnePhaseBundlePeerGuard guard = Guard(raft, transport, singleProcess: true))
        {
            Assert.True(guard.ShouldProposeBundle(Partition, 1));
            Assert.Equal(0, guard.Asks);
        }
    }

    [Fact]
    public async Task ThreeEntriesUntilEveryReplicaConfirms_ThenNoCallForACommit()
    {
        (PeerGuardStubRaft raft, MemoryInterNodeCommmunication transport, Dictionary<string, AnsweringNode> nodes) = Group("a:1", "b:1");
        using OnePhaseBundlePeerGuard guard = Guard(raft, transport);

        // No verdict yet: three entries, and an ask of each replica.
        Assert.False(guard.ShouldProposeBundle(Partition, 1));
        Assert.Equal(1, guard.Fallbacks);
        await WaitUntilAsync(() => guard.ShouldProposeBundle(Partition, 1), "the replicas confirm");
        Assert.All(nodes.Values, node => Assert.Equal(1, node.Calls));

        long asks = guard.Asks;
        for (int i = 0; i < 100; i++)
            Assert.True(guard.ShouldProposeBundle(Partition, 1));

        Assert.Equal(asks, guard.Asks);
    }

    public static TheoryData<string> Refusals => ["unanswered", "no_bundle_type"];

    [Theory]
    [MemberData(nameof(Refusals))]
    public async Task AReplicaThatRefuses_KeepsThreeEntries_UntilItConfirmsAtTheNextAsk(string refusal)
    {
        (PeerGuardStubRaft raft, MemoryInterNodeCommmunication transport, Dictionary<string, AnsweringNode> nodes) = Group("a:1", "b:1");
        nodes["b:1"].Answer = refusal == "unanswered" ? null : [ReplicationTypes.KeyValues, ReplicationTypes.PreparedIntent];
        using OnePhaseBundlePeerGuard guard = Guard(raft, transport, retryMs: 50);

        Assert.False(guard.ShouldProposeBundle(Partition, 1));
        await WaitUntilAsync(() => nodes["b:1"].Calls >= 1 && guard.Asks >= 2, "the first ask");
        await Task.Delay(20, TestContext.Current.CancellationToken);

        for (int i = 0; i < 5; i++)
            Assert.False(guard.ShouldProposeBundle(Partition, 1));
        Assert.True(guard.Fallbacks >= 6);

        // The replica runs a build that applies the type now: the next ask after the retry interval confirms it.
        nodes["b:1"].Answer = OnePhaseBundlePeerGuard.AppliedLogTypes;
        await WaitUntilAsync(() => guard.ShouldProposeBundle(Partition, 1), "the replica confirms at a later ask");
        Assert.True(nodes["b:1"].Calls >= 2);
    }

    [Fact]
    public async Task AVerdictOfAnotherTerm_IsNotAConfirmation()
    {
        (PeerGuardStubRaft raft, MemoryInterNodeCommmunication transport, Dictionary<string, AnsweringNode> nodes) = Group("a:1");
        using OnePhaseBundlePeerGuard guard = Guard(raft, transport);

        guard.ShouldProposeBundle(Partition, 1);
        await WaitUntilAsync(() => guard.ShouldProposeBundle(Partition, 1), "confirmed in term 1");

        // A new term with a replica that refuses: the confirmation of term 1 does not hold.
        nodes["a:1"].Answer = null;
        Assert.False(guard.ShouldProposeBundle(Partition, 2));
        await WaitUntilAsync(() => nodes["a:1"].Calls >= 2, "the ask of term 2");
        await Task.Delay(20, TestContext.Current.CancellationToken);
        Assert.False(guard.ShouldProposeBundle(Partition, 2));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AChangeOfTheReplicasOrTheMembers_IsAskedAgain(bool partitionMap)
    {
        (PeerGuardStubRaft raft, MemoryInterNodeCommmunication transport, Dictionary<string, AnsweringNode> nodes) = Group("a:1", "b:1");
        using OnePhaseBundlePeerGuard guard = Guard(raft, transport);

        guard.ShouldProposeBundle(Partition, 1);
        await WaitUntilAsync(() => guard.ShouldProposeBundle(Partition, 1), "confirmed");

        // A new learner of an older build joins the partition.
        AnsweringNode learner = new() { Answer = null };
        transport.SetNodes(new Dictionary<string, IKahuna> { ["a:1"] = nodes["a:1"], ["b:1"] = nodes["b:1"], ["c:1"] = learner });
        raft.Replicas = [.. raft.Replicas, new RaftReplica { Endpoint = "c:1", Role = RaftReplicaRole.Learner }];

        if (partitionMap)
            raft.RaisePartitionMapChanged();
        else
            raft.RaiseMembershipChanged();

        Assert.False(guard.ShouldProposeBundle(Partition, 1));
        await WaitUntilAsync(() => learner.Calls >= 1, "the learner is asked");
        await Task.Delay(20, TestContext.Current.CancellationToken);
        Assert.False(guard.ShouldProposeBundle(Partition, 1));
    }

    [Fact]
    public async Task AConfirmationIsAskedAgainAfterTheRefreshInterval_AndFallsWhenAReplicaRefuses()
    {
        (PeerGuardStubRaft raft, MemoryInterNodeCommmunication transport, Dictionary<string, AnsweringNode> nodes) = Group("a:1");
        using OnePhaseBundlePeerGuard guard = Guard(raft, transport, refreshMs: 100);

        guard.ShouldProposeBundle(Partition, 1);
        await WaitUntilAsync(() => guard.ShouldProposeBundle(Partition, 1), "confirmed");

        // The replica started again on a build that does not know the type.
        nodes["a:1"].Answer = null;
        await Task.Delay(150, TestContext.Current.CancellationToken);

        // The confirmation still holds for this call, which starts the ask in the background.
        Assert.True(guard.ShouldProposeBundle(Partition, 1));
        await WaitUntilAsync(() => !guard.ShouldProposeBundle(Partition, 1), "the refresh finds the refusal");
    }

    [Fact]
    public async Task TakingTheLeadership_AsksAtOnce_AndAnotherLeaderAsksNothing()
    {
        (PeerGuardStubRaft raft, MemoryInterNodeCommmunication transport, Dictionary<string, AnsweringNode> nodes) = Group("a:1");
        using OnePhaseBundlePeerGuard guard = Guard(raft, transport);

        guard.OnLeaderChanged(Partition, "a:1");
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.Equal(0, nodes["a:1"].Calls);

        guard.OnLeaderChanged(Partition, "self:1");
        await WaitUntilAsync(() => guard.ShouldProposeBundle(Partition, raft.Term), "confirmed with no commit before it");
        Assert.Equal(0, guard.Fallbacks);
    }

    /// <summary>A partition with no replica set of its own is on every node: the guard asks the nodes this node knows,
    /// a configured node that is not in the roster yet included, and the members of the roster.</summary>
    [Fact]
    public async Task APartitionOnEveryNode_AsksTheNodesAndTheMembers_ButNotItself()
    {
        PeerGuardStubRaft raft = new() { LocalEndpoint = "self:1", Replicas = [], Nodes = ["self:1", "a:1", "c:1"], Members = ["self:1", "a:1", "b:1"] };
        AnsweringNode a = new(), b = new(), c = new();
        MemoryInterNodeCommmunication transport = new();
        transport.SetNodes(new Dictionary<string, IKahuna> { ["a:1"] = a, ["b:1"] = b, ["c:1"] = c });
        using OnePhaseBundlePeerGuard guard = Guard(raft, transport);

        Assert.False(guard.ShouldProposeBundle(Partition, 1));
        await WaitUntilAsync(() => guard.ShouldProposeBundle(Partition, 1), "confirmed");
        Assert.Equal(1, a.Calls);
        Assert.Equal(1, b.Calls);
        Assert.Equal(1, c.Calls);
        Assert.Equal(3, guard.Asks);
    }

    /// <summary>While an ask runs, the commits of the partition propose three entries and start no second ask.</summary>
    [Fact]
    public async Task OneAskAtATime()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        (PeerGuardStubRaft raft, MemoryInterNodeCommmunication transport, Dictionary<string, AnsweringNode> nodes) = Group("a:1");
        using ManualResetEventSlim gate = new(false);
        nodes["a:1"].Gate = gate;
        using OnePhaseBundlePeerGuard guard = Guard(raft, transport, retryMs: 0);

        // The first call starts the ask, which waits at the replica.
        Task<bool> first = Task.Run(() => guard.ShouldProposeBundle(Partition, 1), ct);
        await WaitUntilAsync(() => nodes["a:1"].Calls == 1, "the first ask reaches the replica");

        Task<bool>[] others = [.. Enumerable.Range(0, 5).Select(_ => Task.Run(() => guard.ShouldProposeBundle(Partition, 1), ct))];
        bool[] answers = await Task.WhenAll(others).WaitAsync(TimeSpan.FromSeconds(5), ct);

        Assert.All(answers, Assert.False);
        Assert.Equal(1, nodes["a:1"].Calls);

        gate.Set();
        Assert.False(await first.WaitAsync(TimeSpan.FromSeconds(5), ct));
        await WaitUntilAsync(() => guard.ShouldProposeBundle(Partition, 1), "confirmed");
    }
}

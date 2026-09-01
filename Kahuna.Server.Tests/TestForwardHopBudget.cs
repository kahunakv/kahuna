using Kahuna;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the inter-node forward budget. Every node resolves a forward target from its own
/// local belief, and local beliefs disagree during an election window: node A believes B leads a
/// partition while B believes A leads it. Unbudgeted, each forwards to the other for as long as the
/// disagreement lasts — an unbounded request loop on the gRPC transport, and mutual recursion that
/// ends the process with a stack overflow on the in-memory transport, whose forward is a direct
/// in-process call.
/// </summary>
public sealed class TestForwardHopBudget
{
    // ── the ambient marker ──────────────────────────────────────────────────────────────────────

    [Fact]
    public void NestedScopes_CountUpAndRestoreOnDispose()
    {
        Assert.False(ForwardedRequestScope.IsActive);
        Assert.Equal(0, ForwardedRequestScope.ChainedHops);

        using (ForwardedRequestScope.Enter())
        {
            Assert.True(ForwardedRequestScope.IsActive);
            Assert.Equal(1, ForwardedRequestScope.ChainedHops);
            Assert.True(ForwardedRequestScope.CanForward);

            using (ForwardedRequestScope.Enter())
            {
                Assert.Equal(2, ForwardedRequestScope.ChainedHops);
                Assert.False(ForwardedRequestScope.CanForward);
            }

            // Disposing the inner scope restores the outer count rather than clearing the marker.
            Assert.Equal(1, ForwardedRequestScope.ChainedHops);
        }

        Assert.Equal(0, ForwardedRequestScope.ChainedHops);
    }

    [Fact]
    public void EnterAt_AdoptsTheWireCountAndNeverDropsBelowThisHop()
    {
        using (ForwardedRequestScope.EnterAt(2))
            Assert.Equal(2, ForwardedRequestScope.ChainedHops);

        // A peer that does not stamp the field (0) still leaves the request marked as forwarded.
        using (ForwardedRequestScope.EnterAt(0))
        {
            Assert.True(ForwardedRequestScope.IsActive);
            Assert.Equal(1, ForwardedRequestScope.ChainedHops);
        }

        Assert.Equal(0, ForwardedRequestScope.ChainedHops);
    }

    [Fact]
    public void Suppress_StartsAFreshChainAndRestoresIt()
    {
        using (ForwardedRequestScope.EnterAt(ForwardedRequestScope.MaxForwardHops))
        {
            Assert.False(ForwardedRequestScope.CanForward);

            // A sub-operation the serving node initiates on its own behalf is a new operation with
            // its own target, so it gets the full budget again.
            using (ForwardedRequestScope.Suppress())
            {
                Assert.False(ForwardedRequestScope.IsActive);
                Assert.True(ForwardedRequestScope.CanForward);
            }

            Assert.False(ForwardedRequestScope.CanForward);
        }
    }

    [Fact]
    public void NestingPastTheCeiling_FailsTheOperationInsteadOfExhaustingTheStack()
    {
        // Suppression restarts the hop chain but not the nesting depth, so a loop that runs through
        // a suppressing component still hits the ceiling.
        List<IDisposable> scopes = [];

        try
        {
            ForwardLoopException thrown = Assert.Throws<ForwardLoopException>(() =>
            {
                for (int i = 0; i < 1_000; i++)
                {
                    scopes.Add(ForwardedRequestScope.Enter());
                    scopes.Add(ForwardedRequestScope.Suppress());
                }
            });

            Assert.True(thrown.NestedForwards > 0);
        }
        finally
        {
            for (int i = scopes.Count - 1; i >= 0; i--)
                scopes[i].Dispose();
        }

        Assert.Equal(0, ForwardedRequestScope.ChainedHops);
    }

    // ── the resolver, on a partition this node hosts ────────────────────────────────────────────

    [Fact]
    public async Task HostedPartition_SpendsTheBudgetThenRefusesToForwardAgain()
    {
        LeadershipStubRaft raft = new() { Hosts = true, Answer = false, LeaderEndpoint = "node2:2070" };

        // The originator forwards, and so does the first receiver: that receiver hosts the range,
        // so its own resolution can still correct a sender that routed on a replica guess.
        Assert.Equal("node2:2070", await raft.TryResolveLeader(1, CancellationToken.None));

        using (ForwardedRequestScope.EnterAt(1))
            Assert.Equal("node2:2070", await raft.TryResolveLeader(1, CancellationToken.None));

        // Past the budget the answer is null, which every call site reports as MustRetry.
        using (ForwardedRequestScope.EnterAt(ForwardedRequestScope.MaxForwardHops))
            Assert.Null(await raft.TryResolveLeader(1, CancellationToken.None));
    }

    [Fact]
    public async Task HostedPartition_ALocalAnswerIsNotAForwardAndSurvivesTheBudget()
    {
        LeadershipStubRaft raft = new() { Hosts = true, Answer = false, LeaderEndpoint = "node1:2070" };

        // "node1:2070" is this stub's own endpoint: the caller serves the operation locally, so
        // exhausting the forward budget must not turn a servable request into MustRetry.
        using (ForwardedRequestScope.EnterAt(ForwardedRequestScope.MaxForwardHops))
            Assert.Equal("node1:2070", await raft.TryResolveLeader(1, CancellationToken.None));
    }

    // ── the in-memory transport ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task MutuallyStaleLeaderViews_TerminateInsteadOfRecursingForever()
    {
        // Both nodes host the partition (full replication) and each believes the other leads it —
        // the election-window disagreement that made a working-set release recurse until the test
        // host died of stack exhaustion.
        MemoryInterNodeCommmunication transport = new();

        PingPongNode node1 = new("node1:2070", "node2:2070", transport);
        PingPongNode node2 = new("node2:2070", "node1:2070", transport);

        transport.SetNodes(new() { { "node1:2070", node1 }, { "node2:2070", node2 } });

        (KeyValueResponseType type, string key) = await node1.LocateAndTryReleaseExclusiveLock(
            HLCTimestamp.Zero,
            "k1",
            KeyValueDurability.Persistent,
            CancellationToken.None
        );

        Assert.Equal(KeyValueResponseType.MustRetry, type);
        Assert.Equal("k1", key);

        // The budget is what stops it: the originator forwards, the receiver may correct it once,
        // and the hop after that is refused.
        Assert.Equal(ForwardedRequestScope.MaxForwardHops + 1, node1.Visits + node2.Visits);
    }

    /// <summary>
    /// A node that resolves the release path exactly as <c>KeyValueLocator</c> does — believe the
    /// peer leads the partition, forward there, and answer <c>MustRetry</c> when no target
    /// resolves — over the real in-memory transport and the real leadership resolver.
    /// </summary>
    private sealed class PingPongNode(string endpoint, string peer, MemoryInterNodeCommmunication transport) : FakeKahunaBase
    {
        private int visits;

        /// <summary>How many times this node was asked to serve the operation.</summary>
        public int Visits => Volatile.Read(ref visits);

        private readonly LeadershipStubRaft raft = new()
        {
            Hosts = true,
            Answer = false,
            LeaderEndpoint = peer,
            LocalEndpoint = endpoint
        };

        public override async Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(
            HLCTimestamp transactionId,
            string key,
            KeyValueDurability durability,
            CancellationToken cancellationToken,
            string coordinatorKey = "",
            TransactionOperationId operationId = default
        )
        {
            Interlocked.Increment(ref visits);

            string? leader = await raft.TryResolveLeader(0, cancellationToken);
            if (leader is null || leader == raft.GetLocalEndpoint())
                return (KeyValueResponseType.MustRetry, key);

            return await transport.TryReleaseExclusiveLock(leader, transactionId, key, durability, cancellationToken);
        }
    }
}

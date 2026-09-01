
using Kahuna.Server;

using Kommander;
using Kommander.Communication;
using Kommander.Data;
using Kommander.Diagnostics;
using Kommander.Discovery;
using Kommander.Scheduling;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;

namespace Kahuna.Server.Tests;

/// <summary>
/// Coverage for the placement-safe leadership wrappers. A partition this node does not host must
/// answer false/null (a retryable routing condition) instead of throwing; the advisory hosted
/// check must not race incorrectly (a typed throw after the guard says yes is still answered
/// retryably); and an unknown partition id must keep throwing — that is a caller error, and
/// swallowing it would turn a routing bug into an infinite retry loop.
/// </summary>
public sealed class TestPartitionLeaderResolver
{
    [Fact]
    public async Task HostedPartition_DelegatesToTheRawApis()
    {
        LeadershipStubRaft raft = new() { Hosts = true, Answer = true, LeaderEndpoint = "node2:2070" };

        Assert.True(await raft.AmILeaderIfHosted(1, CancellationToken.None));
        Assert.True(await raft.AmILeaderQuickIfHosted(1));
        Assert.True(await raft.ConfirmLeadershipIfHosted(1, CancellationToken.None));
        Assert.Equal("node2:2070", await raft.TryResolveLeader(1, CancellationToken.None));

        Assert.Equal(4, raft.RawCalls);

        raft.Answer = false;

        Assert.False(await raft.AmILeaderIfHosted(1, CancellationToken.None));
        Assert.False(await raft.AmILeaderQuickIfHosted(1));
        Assert.False(await raft.ConfirmLeadershipIfHosted(1, CancellationToken.None));
    }

    [Fact]
    public async Task NotHostedPartition_AnswersRetryablyWithoutCallingKommander()
    {
        LeadershipStubRaft raft = new() { Hosts = false, Failure = new InvalidOperationException("the raw API must not be reached") };

        Assert.False(await raft.AmILeaderIfHosted(1, CancellationToken.None));
        Assert.False(await raft.AmILeaderQuickIfHosted(1));
        Assert.False(await raft.ConfirmLeadershipIfHosted(1, CancellationToken.None));

        // No hint and no replicas known: nothing to forward to, so the resolver answers null
        // (MustRetry) without touching the leadership APIs.
        Assert.Null(await raft.TryResolveLeader(1, CancellationToken.None));

        Assert.Equal(0, raft.RawCalls);
    }

    [Fact]
    public async Task NotHostedPartition_PrefersTheGossipedLeaderHint()
    {
        LeadershipStubRaft raft = new()
        {
            Hosts = false,
            Failure = new InvalidOperationException("the raw API must not be reached"),
            LeaderHint = "node4:2070",
            Replicas = [Voter("node2:2070"), Voter("node3:2070")]
        };

        Assert.Equal("node4:2070", await raft.TryResolveLeader(1, CancellationToken.None));
        Assert.Equal(0, raft.RawCalls);
    }

    [Fact]
    public async Task NotHostedPartition_FallsBackToTheFirstRemoteVoter()
    {
        // A hint naming the local node is useless for forwarding (the partition is not hosted
        // here) and must be skipped; transitional replicas come after voters.
        LeadershipStubRaft raft = new()
        {
            Hosts = false,
            LeaderHint = "node1:2070",
            Replicas =
            [
                new RaftReplica { Endpoint = "node5:2070", Role = RaftReplicaRole.Learner },
                new RaftReplica { Endpoint = "node1:2070", Role = RaftReplicaRole.Voter },
                Voter("node3:2070"),
                Voter("node4:2070")
            ]
        };

        Assert.Equal("node3:2070", await raft.TryResolveLeader(1, CancellationToken.None));
    }

    [Fact]
    public async Task NotHostedPartition_TransitionalReplicaIsTheLastResort()
    {
        LeadershipStubRaft raft = new()
        {
            Hosts = false,
            Replicas = [new RaftReplica { Endpoint = "node5:2070", Role = RaftReplicaRole.Learner }]
        };

        Assert.Equal("node5:2070", await raft.TryResolveLeader(1, CancellationToken.None));
    }

    [Fact]
    public async Task ForwardedRequest_NeverForwardsOnwardFromANonHostingReceiver()
    {
        LeadershipStubRaft raft = new()
        {
            Hosts = false,
            LeaderHint = "node4:2070",
            Replicas = [Voter("node2:2070"), Voter("node3:2070")]
        };

        using (ForwardedRequestScope.Enter())
            Assert.Null(await raft.TryResolveLeader(1, CancellationToken.None));

        // The suppression is scoped to the forwarded flow; an originator resolves normally again.
        Assert.Equal("node4:2070", await raft.TryResolveLeader(1, CancellationToken.None));
    }

    [Fact]
    public async Task MaterializationRace_TypedThrowAfterTheGuard_IsAnsweredRetryably()
    {
        // The committed map can list this node as a replica before the partition materializes
        // locally, so the hosted guard can say yes while the raw API still throws the typed
        // exception. The wrappers must treat the throw as the authoritative retryable answer.
        LeadershipStubRaft raft = new() { Hosts = true, Failure = new PartitionNotHostedException(7) };

        Assert.False(await raft.AmILeaderIfHosted(7, CancellationToken.None));
        Assert.False(await raft.AmILeaderQuickIfHosted(7));
        Assert.False(await raft.ConfirmLeadershipIfHosted(7, CancellationToken.None));
        Assert.Null(await raft.TryResolveLeader(7, CancellationToken.None));

        Assert.Equal(4, raft.RawCalls);
    }

    [Fact]
    public async Task UnknownPartition_PlainRaftExceptionPropagates()
    {
        LeadershipStubRaft raft = new() { Hosts = true, Failure = new RaftException("Invalid partition: 99") };

        await Assert.ThrowsAsync<RaftException>(async () => await raft.AmILeaderIfHosted(99, CancellationToken.None));
        await Assert.ThrowsAsync<RaftException>(async () => await raft.AmILeaderQuickIfHosted(99));
        await Assert.ThrowsAsync<RaftException>(async () => await raft.ConfirmLeadershipIfHosted(99, CancellationToken.None));
        await Assert.ThrowsAsync<RaftException>(async () => await raft.TryResolveLeader(99, CancellationToken.None));
    }

    private static RaftReplica Voter(string endpoint) => new() { Endpoint = endpoint, Role = RaftReplicaRole.Voter };
}

using Kommander;
using Kommander.Gossip;
using Kommander.System;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.Tests;

/// <summary>
/// The embedded node boots as a 3-voter Raft cluster: the real node plus two synthetic
/// witnesses. The witnesses must answer SWIM liveness probes as Alive, otherwise the failure
/// detector marks them Dead and Kommander evicts them, collapsing the roster to a lone voter
/// that then spams "No other nodes availables to send hearthbeat" every heartbeat.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestEmbeddedWitnessLiveness
{
    private readonly ILoggerFactory loggerFactory;

    public TestEmbeddedWitnessLiveness(ITestOutputHelper outputHelper)
    {
        loggerFactory = TestLogFactory.Create(outputHelper);
    }

    [Fact]
    public async Task TestWitnessTransportAnswersSwimProbesAsAlive()
    {
        EmbeddedRaftCommunication communication = new();
        RaftNode witness = EmbeddedRaftCommunication.Witnesses[0];

        PingResponse pingResponse = await communication.SendPing(
            null!, witness, new PingRequest("localhost:0"), TestContext.Current.CancellationToken);

        Assert.True(pingResponse.Alive);

        PingReqResponse pingReqResponse = await communication.SendPingReq(
            null!, witness, new PingReqRequest("localhost:0", witness.Endpoint), TestContext.Current.CancellationToken);

        Assert.True(pingReqResponse.Reached);
    }

    [Fact]
    public async Task TestWitnessesSurviveSuspicionAndEvictionGrace()
    {
        await using EmbeddedKahunaNode node = new(new()
        {
            Storage = "memory",
            WalStorage = "memory",
            InitialPartitions = 1
        }, loggerFactory);

        await node.StartAsync(TestContext.Current.CancellationToken);
        await node.WaitForLeaderForKeyAsync("tenant/table/key-a", TestContext.Current.CancellationToken);

        // Run past the point where, absent SendPing/SendPingReq answers, the witnesses would be
        // marked Suspect → Dead and then evicted from the committed roster. A couple of extra
        // ping rounds of margin keeps the assertion clear of the eviction boundary.
        RaftConfiguration config = node.Raft.Configuration;
        TimeSpan window = config.SuspicionTimeout
                          + config.DeadMemberEvictionGrace
                          + TimeSpan.FromTicks(config.PingInterval.Ticks * 3);

        DateTime deadline = DateTime.UtcNow + window;

        // Poll throughout the window: the roster must never drop below the three seeded voters,
        // not merely be intact at the end.
        while (DateTime.UtcNow < deadline)
        {
            AssertRosterStillThreeVoters(node);
            await Task.Delay(TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);
        }

        AssertRosterStillThreeVoters(node);

        // The peer set surfaced by UpdateNodes must be non-empty so SendHeartbeat has targets and
        // stops logging the "No other nodes availables" warning.
        await node.Raft.UpdateNodes();
        Assert.NotEmpty(node.Raft.GetNodes());
    }

    private static void AssertRosterStillThreeVoters(EmbeddedKahunaNode node)
    {
        ClusterMembership membership = node.Raft.GetMembership();
        List<ClusterMember> voters = membership.Members
            .Where(member => member.Role == ClusterMemberRole.Voter)
            .ToList();

        Assert.Equal(3, voters.Count);

        foreach (RaftNode witness in EmbeddedRaftCommunication.Witnesses)
            Assert.Contains(voters, member => member.Endpoint == witness.Endpoint);
    }
}

using System.Diagnostics.Metrics;
using System.Text.Json;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.Communication.Internode;
using Kahuna.Shared.Communication.Rest;
using Kommander;
using Kommander.Communication.Memory;
using Kommander.System;
using Kommander.WAL;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Tests for the placement observability/control surface: the placement table and health
/// hosted-count builders against real clusters (placed and legacy), the per-partition
/// replication-factor override (leader commits, follower refuses, zero clears), the
/// forward-resolution metrics, and the REST wire shapes.
/// </summary>
public sealed class TestClusterPlacementSurface : BaseCluster
{
    private const int Partitions = 6;

    private static async Task<(IRaft[] Rafts, CancellationToken Ct)> AssembleRf1Cluster()
    {
        InMemoryCommunication raftComm = new();
        MemoryInterNodeCommmunication interComm = new();

        (IRaft raft1, IKahuna kahuna1) = BuildNode(interComm, raftComm, "memory", 1, 8001,
            ["localhost:8002", "localhost:8003"], NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, Partitions, replicationFactor: 1);
        (IRaft raft2, IKahuna kahuna2) = BuildNode(interComm, raftComm, "memory", 2, 8002,
            ["localhost:8001", "localhost:8003"], NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, Partitions, replicationFactor: 1);
        (IRaft raft3, IKahuna kahuna3) = BuildNode(interComm, raftComm, "memory", 3, 8003,
            ["localhost:8001", "localhost:8002"], NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance, Partitions, replicationFactor: 1);

        interComm.SetNodes(new()
        {
            { "localhost:8001", kahuna1 },
            { "localhost:8002", kahuna2 },
            { "localhost:8003", kahuna3 }
        });

        raftComm.SetNodes(new()
        {
            { "localhost:8001", raft1 },
            { "localhost:8002", raft2 },
            { "localhost:8003", raft3 }
        });

        CancellationToken ct = TestContext.Current.CancellationToken;
        await Task.WhenAll(raft1.JoinCluster(ct), raft2.JoinCluster(ct), raft3.JoinCluster(ct));

        IRaft[] rafts = [raft1, raft2, raft3];

        // Placement is committed with the bootstrap map; wait until every node's applied map shows
        // the replica assignments so the assertions below read a settled view.
        foreach (IRaft raft in rafts)
            for (int partitionId = 1; partitionId <= Partitions; partitionId++)
                await WaitUntilAsync(() => raft.GetPartitionReplicas(partitionId).Count > 0);

        return (rafts, ct);
    }

    [Fact]
    public async Task Placement_Rf1Cluster_ReflectsCommittedPlacement()
    {
        (IRaft[] rafts, _) = await AssembleRf1Cluster();

        int hostedTotal = 0;

        foreach (IRaft raft in rafts)
        {
            KahunaClusterPlacementResponse placement = ClusterHandlers.BuildPlacementResponse(raft);

            Assert.Equal(1, placement.ReplicationFactor);
            Assert.True(placement.Initialized);
            Assert.Equal(raft.GetLocalEndpoint(), placement.LocalEndpoint);
            Assert.Equal(Partitions, placement.Partitions.Count);

            foreach (KahunaPartitionPlacementResponse partition in placement.Partitions)
            {
                Assert.Equal(1, partition.EffectiveReplicationFactor);
                KahunaPartitionReplicaResponse replica = Assert.Single(partition.Replicas);
                Assert.Equal("Voter", replica.Role);
                // The hosted flag is the local materialization of the committed replica set.
                Assert.Equal(replica.Endpoint == placement.LocalEndpoint, partition.HostedLocally);
            }

            hostedTotal += placement.HostedPartitionCount;

            // The health probe reports the same count, informationally, while staying ready.
            KahunaClusterHealthResponse health = ClusterHandlers.BuildHealthResponse(raft);
            Assert.True(health.Ready);
            Assert.Equal(placement.HostedPartitionCount, health.HostedPartitions);
        }

        // Replication factor 1 partitions the set: each partition hosted by exactly one node.
        Assert.Equal(Partitions, hostedTotal);

        await LeaveCluster(rafts[0], rafts[1], rafts[2]);
    }

    [Fact]
    public async Task Placement_LegacyCluster_EverythingHostedWithEmptyReplicas()
    {
        (IRaft raft1, IRaft raft2, IRaft raft3, _, _, _) = await AssembleThreNodeCluster(
            "memory", 4, NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance);

        foreach (IRaft raft in (IRaft[])[raft1, raft2, raft3])
        {
            KahunaClusterPlacementResponse placement = ClusterHandlers.BuildPlacementResponse(raft);

            Assert.Equal(0, placement.ReplicationFactor);
            Assert.Equal(placement.Partitions.Count, placement.HostedPartitionCount);
            foreach (KahunaPartitionPlacementResponse partition in placement.Partitions)
            {
                Assert.True(partition.HostedLocally);
                Assert.Empty(partition.Replicas);
                Assert.Equal(0, partition.EffectiveReplicationFactor);
            }

            KahunaClusterHealthResponse health = ClusterHandlers.BuildHealthResponse(raft);
            Assert.True(health.Ready);
            Assert.Equal(placement.Partitions.Count, health.HostedPartitions);
        }

        await LeaveCluster(raft1, raft2, raft3);
    }

    [Fact]
    public async Task SetReplicationFactor_LeaderCommits_FollowerRefuses_ZeroClears()
    {
        (IRaft[] rafts, CancellationToken ct) = await AssembleRf1Cluster();

        IRaft leader = rafts[0];
        IRaft follower = rafts[0];
        foreach (IRaft raft in rafts)
        {
            if (await raft.AmILeaderIfHosted(0, ct))
                leader = raft;
            else
                follower = raft;
        }

        // A follower refuses with the reason instead of committing (or leaking a 500).
        KahunaSetReplicationFactorResponse refused = await ClusterHandlers.SetReplicationFactorAsync(
            follower, new KahunaSetReplicationFactorRequest { PartitionId = 1, ReplicationFactor = 2 }, ct);
        Assert.False(refused.Success);
        Assert.Equal("Refused", refused.Status);
        Assert.False(string.IsNullOrEmpty(refused.Reason));

        // The meta-partition leader commits; the effective factor reflects the override.
        KahunaSetReplicationFactorResponse committed = await ClusterHandlers.SetReplicationFactorAsync(
            leader, new KahunaSetReplicationFactorRequest { PartitionId = 1, ReplicationFactor = 2 }, ct);
        Assert.True(committed.Success);
        IRaft committedLeader = leader;
        await WaitUntilAsync(() => committedLeader.GetEffectiveReplicationFactor(1) == 2);

        // Zero clears the override: the partition inherits the global factor again.
        KahunaSetReplicationFactorResponse cleared = await ClusterHandlers.SetReplicationFactorAsync(
            leader, new KahunaSetReplicationFactorRequest { PartitionId = 1, ReplicationFactor = 0 }, ct);
        Assert.True(cleared.Success);
        await WaitUntilAsync(() => committedLeader.GetEffectiveReplicationFactor(1) == 1);

        // Invalid input never reaches the map: partition 0 is the meta partition.
        KahunaSetReplicationFactorResponse invalid = await ClusterHandlers.SetReplicationFactorAsync(
            leader, new KahunaSetReplicationFactorRequest { PartitionId = 0, ReplicationFactor = 2 }, ct);
        Assert.False(invalid.Success);
        Assert.Equal("InvalidInput", invalid.Status);

        await LeaveCluster(rafts[0], rafts[1], rafts[2]);
    }

    [Fact]
    public async Task ResolveLeader_NonHostedPartition_CountsForwardResolutionAndHintMiss()
    {
        long hintHits = 0, hintMisses = 0, resolved = 0, unresolved = 0;
        using MeterListener listener = new();
        listener.InstrumentPublished = (inst, l) =>
        {
            if (inst.Meter.Name == "Kahuna" && inst.Name.StartsWith("kahuna.placement.", StringComparison.Ordinal))
                l.EnableMeasurementEvents(inst);
        };
        listener.SetMeasurementEventCallback<long>((inst, val, _, _) =>
        {
            switch (inst.Name)
            {
                case "kahuna.placement.leader_hint_hits": Interlocked.Add(ref hintHits, val); break;
                case "kahuna.placement.leader_hint_misses": Interlocked.Add(ref hintMisses, val); break;
                case "kahuna.placement.forwards_resolved": Interlocked.Add(ref resolved, val); break;
                case "kahuna.placement.forwards_unresolved": Interlocked.Add(ref unresolved, val); break;
            }
        });
        listener.Start();

        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        TestBackupService.StubRaft raft = new(wal, [new RaftPartitionRange { PartitionId = 7, State = RaftPartitionState.Active }])
        {
            HostsPartitionOverride = _ => false
        };

        // No hint, no replicas: unresolved (the caller answers MustRetry), counted as a hint miss.
        Assert.Null(await raft.TryResolveLeader(7, TestContext.Current.CancellationToken));

        // No hint, but the committed replica set names a remote voter: resolved via the fallback.
        raft.PartitionReplicasOverride = _ =>
            [new RaftReplica { Endpoint = "otherhost:20000", Role = RaftReplicaRole.Voter }];
        Assert.Equal("otherhost:20000", await raft.TryResolveLeader(7, TestContext.Current.CancellationToken));

        // A gossiped hint answers directly: hit, no fallback.
        raft.PartitionLeaderHintOverride = _ => "hinted:20001";
        Assert.Equal("hinted:20001", await raft.TryResolveLeader(7, TestContext.Current.CancellationToken));

        listener.Dispose();
        Assert.Equal(1, hintHits);
        Assert.Equal(2, hintMisses);
        Assert.Equal(2, resolved);
        Assert.Equal(1, unresolved);
    }

    [Fact]
    public void PlacementResponses_SurviveJsonRoundTrip()
    {
        KahunaClusterPlacementResponse placement = new()
        {
            ReplicationFactor = 3,
            RebalancerEnabled = true,
            Initialized = true,
            LocalEndpoint = "localhost:8001",
            HostedPartitionCount = 2,
            Partitions =
            [
                new KahunaPartitionPlacementResponse
                {
                    PartitionId = 1,
                    State = "Active",
                    Generation = 5,
                    EffectiveReplicationFactor = 3,
                    HostedLocally = true,
                    Replicas = [new KahunaPartitionReplicaResponse { Endpoint = "localhost:8001", Role = "Voter" }]
                }
            ]
        };

        string json = JsonSerializer.Serialize(placement, KahunaJsonContext.Default.KahunaClusterPlacementResponse);
        KahunaClusterPlacementResponse? back =
            JsonSerializer.Deserialize(json, KahunaJsonContext.Default.KahunaClusterPlacementResponse);

        Assert.NotNull(back);
        Assert.Equal(3, back!.ReplicationFactor);
        Assert.True(back.RebalancerEnabled);
        Assert.Equal("localhost:8001", back.LocalEndpoint);
        KahunaPartitionPlacementResponse p = Assert.Single(back.Partitions);
        Assert.Equal(5, p.Generation);
        Assert.Equal("Voter", Assert.Single(p.Replicas).Role);

        KahunaSetReplicationFactorResponse outcome = new()
        {
            Success = false, Status = "Refused", Generation = 0, Reason = "not the leader"
        };
        string outcomeJson = JsonSerializer.Serialize(outcome, KahunaJsonContext.Default.KahunaSetReplicationFactorResponse);
        KahunaSetReplicationFactorResponse? outcomeBack =
            JsonSerializer.Deserialize(outcomeJson, KahunaJsonContext.Default.KahunaSetReplicationFactorResponse);
        Assert.NotNull(outcomeBack);
        Assert.False(outcomeBack!.Success);
        Assert.Equal("not the leader", outcomeBack.Reason);
    }
}

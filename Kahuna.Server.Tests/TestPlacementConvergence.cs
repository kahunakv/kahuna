using System.Collections.Concurrent;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Shared.Communication.Rest;
using Kommander;
using Kommander.System;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// The placement controller has to actually act on a committed replication-factor target, not just
/// store it. These tests drive the override through the public cluster surface and assert the
/// committed replica set converges — with the leader balancer switched off, which is the
/// configuration a placed cluster runs by default and the one under which placement passes used to
/// never be scheduled at all.
/// </summary>
public sealed class TestPlacementConvergence : BaseCluster
{
    private const int Nodes = 6;
    private const int Partitions = 4;
    private const int PlacedRf = 3;

    /// <summary>Voter replicas of <paramref name="partitionId"/> in the node's applied map.</summary>
    private static int VoterCount(IRaft raft, int partitionId) =>
        raft.GetPartitionReplicas(partitionId).Count(r => r.Role == RaftReplicaRole.Voter);

    private static long GenerationOf(IRaft raft, int partitionId) =>
        raft.GetPartitionMap().FirstOrDefault(r => r.PartitionId == partitionId)?.Generation ?? 0;

    private static async Task<IRaft> MetaLeaderOf(IRaft[] rafts, CancellationToken ct)
    {
        foreach (IRaft raft in rafts)
            if (await raft.AmILeaderIfHosted(RangeMapStore.MetaPartitionId, ct))
                return raft;

        throw new InvalidOperationException("No node leads the meta partition.");
    }

    /// <summary>
    /// Records the Information lines every node emits so the test can assert on the hosted-set
    /// transition, which is the node-side evidence that a replica really moved rather than the map
    /// merely being rewritten.
    /// </summary>
    private sealed class CapturingKahunaLogger : ILogger<IKahuna>
    {
        public readonly ConcurrentQueue<string> Lines = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
                Lines.Enqueue(formatter(state, exception));
        }
    }

    /// <summary>
    /// Trimming one range from the cluster factor down to a single voter must reach the committed
    /// map: the range loses voters until it holds exactly one, its generation advances (the fence
    /// consumers watch), and the node that lost it stops hosting it. Every other range keeps its
    /// full replica set — an override is scoped to its own range.
    /// </summary>
    [Fact]
    public async Task ReplicationFactorOverride_TrimsTheReplicaSet_WithTheLeaderBalancerOff()
    {
        CapturingKahunaLogger capture = new();

        (IRaft[] rafts, _) = await AssembleCluster(
            Nodes, "memory", Partitions,
            NullLogger<IRaft>.Instance, capture,
            PlacedRf, enablePlacementRebalancer: true);

        CancellationToken ct = TestContext.Current.CancellationToken;

        // The bug this covers was that placement rode the leader balancer's timer, so a cluster
        // with the balancer off never ran a single pass. Pin the fixture's intent: turning the
        // balancer on here would mask the regression rather than test it.
        Assert.All(rafts, raft => Assert.False(raft.Configuration.EnableLeaderBalancer));
        Assert.All(rafts, raft => Assert.Equal(PlacedRf, VoterCount(raft, 1)));

        IRaft leader = await MetaLeaderOf(rafts, ct);
        long generationBefore = GenerationOf(leader, 1);

        KahunaSetReplicationFactorResponse committed = await ClusterHandlers.SetReplicationFactorAsync(
            leader, new KahunaSetReplicationFactorRequest { PartitionId = 1, ReplicationFactor = 1 }, ct);

        Assert.True(committed.Success);

        // The target alone proves nothing — the failure mode is a stored target nothing acts on.
        await WaitUntilAsync(() => rafts.All(raft => VoterCount(raft, 1) == 1), timeoutMs: 30_000);

        Assert.True(
            GenerationOf(leader, 1) > generationBefore,
            "trimming the replica set must advance the range generation");

        // Untouched ranges keep their replica sets: the plan is scoped to the overridden range.
        for (int partitionId = 2; partitionId <= Partitions; partitionId++)
        {
            int probed = partitionId;
            Assert.All(rafts, raft => Assert.Equal(PlacedRf, VoterCount(raft, probed)));
        }

        // Node-side evidence: two of the three replicas stopped hosting the range. Asserting only
        // on the map would pass even if nothing below Kommander reacted.
        await WaitUntilAsync(
            () => capture.Lines.Count(l => l.StartsWith("Stopped hosting", StringComparison.Ordinal)) >= 2);

        await Task.WhenAll(rafts.Select(LeaveCluster));
    }

    /// <summary>
    /// Clearing the override returns the range to the cluster factor, and the controller re-replicates
    /// it — the add-then-promote half of a move, which only converges if learner promotion is driven
    /// (the same pass owns both halves).
    /// </summary>
    [Fact]
    public async Task ClearingTheOverride_ReReplicatesTheRangeBackToTheClusterFactor()
    {
        (IRaft[] rafts, _) = await AssembleCluster(
            Nodes, "memory", Partitions,
            NullLogger<IRaft>.Instance, NullLogger<IKahuna>.Instance,
            PlacedRf, enablePlacementRebalancer: true);

        CancellationToken ct = TestContext.Current.CancellationToken;
        IRaft leader = await MetaLeaderOf(rafts, ct);

        // One voter below the cluster factor, so clearing the override costs exactly one
        // add-then-promote relocation. Trimming further would prove nothing extra here (the trim
        // direction has its own test) while doubling a wait already gated on the promotion window.
        KahunaSetReplicationFactorResponse trimmed = await ClusterHandlers.SetReplicationFactorAsync(
            leader, new KahunaSetReplicationFactorRequest { PartitionId = 1, ReplicationFactor = PlacedRf - 1 }, ct);
        Assert.True(trimmed.Success);

        await WaitUntilAsync(() => rafts.All(raft => VoterCount(raft, 1) == PlacedRf - 1), timeoutMs: 30_000);

        // Zero clears the override; the range inherits the cluster factor and is under-replicated,
        // which is the planner's highest priority.
        KahunaSetReplicationFactorResponse cleared = await ClusterHandlers.SetReplicationFactorAsync(
            leader, new KahunaSetReplicationFactorRequest { PartitionId = 1, ReplicationFactor = 0 }, ct);
        Assert.True(cleared.Success);

        // A relocation costs several passes plus the learner promotion stable window, and these
        // fixtures run alongside other clusters — allow for a slow machine rather than a slow bug.
        await WaitUntilAsync(() => rafts.All(raft => VoterCount(raft, 1) == PlacedRf), timeoutMs: 90_000);

        await Task.WhenAll(rafts.Select(LeaveCluster));
    }
}

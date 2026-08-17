
using Grpc.Core;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kahuna.Shared.Communication.Rest;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Provides gRPC endpoints exposing cluster membership topology and graceful decommission of the
/// local node. Mirrors the REST <c>/v1/cluster/membership</c> and <c>/v1/cluster/leave</c>
/// endpoints 1:1.
/// </summary>
public sealed class ClusterService : Cluster.ClusterBase
{
    private readonly IRaft raft;

    private readonly IKahuna keyValues;

    public ClusterService(IRaft raft, IKahuna keyValues)
    {
        this.raft = raft;
        this.keyValues = keyValues;
    }

    public override Task<GrpcGetMembershipResponse> GetMembership(GrpcGetMembershipRequest request, ServerCallContext context)
    {
        ClusterMembership membership = raft.GetMembership();
        string localEndpoint = raft.GetLocalEndpoint();

        GrpcClusterMemberRole localRole = GrpcClusterMemberRole.ClusterMemberRoleNotMember;
        GrpcGetMembershipResponse response = new()
        {
            MembershipVersion = membership.MembershipVersion,
            Initialized = raft.IsInitialized
        };

        foreach (ClusterMember m in membership.Members)
        {
            GrpcClusterMemberRole role = ToGrpcRole(m.Role);
            response.Members.Add(new GrpcClusterMember
            {
                Endpoint = m.Endpoint,
                NodeId = m.NodeId,
                Role = role,
                JoinedVersion = m.JoinedVersion
            });

            if (m.Endpoint == localEndpoint)
                localRole = role;
        }

        response.LocalRole = localRole;
        return Task.FromResult(response);
    }

    /// <summary>
    /// Commits this node's removal from the cluster roster and reports the outcome. The node keeps
    /// serving afterwards so the caller can read the result before stopping the process.
    /// </summary>
    public override async Task<GrpcClusterLeaveResponse> Leave(GrpcClusterLeaveRequest request, ServerCallContext context)
    {
        LeaveClusterResult result = await ClusterLeave.ExecuteAsync(raft, context.CancellationToken)
            .ConfigureAwait(false);

        return new()
        {
            Left = result.Left,
            Drained = result.Drained,
            Outcome = ToGrpcOutcome(result.Outcome),
            MembershipVersion = result.MembershipVersion,
            Retryable = ClusterLeave.IsRetryable(result.Outcome),
            Reason = ClusterLeave.ToReason(result.Outcome)
        };
    }

    /// <summary>
    /// Returns the per-partition placement table. Mirrors REST <c>GET /v1/cluster/placement</c> 1:1;
    /// the hosted flags describe the answering node.
    /// </summary>
    public override Task<GrpcGetPlacementResponse> GetPlacement(GrpcGetPlacementRequest request, ServerCallContext context)
    {
        GrpcGetPlacementResponse response = new()
        {
            ReplicationFactor = raft.Configuration.ReplicationFactor,
            RebalancerEnabled = raft.Configuration.EnablePlacementRebalancer,
            Initialized = raft.IsInitialized,
            LocalEndpoint = raft.GetLocalEndpoint()
        };

        foreach (RaftPartitionRange range in raft.GetPartitionMap())
        {
            bool hosted = raft.HostsPartition(range.PartitionId);

            GrpcPartitionPlacement partition = new()
            {
                PartitionId = range.PartitionId,
                State = range.State.ToString(),
                Generation = range.Generation,
                EffectiveReplicationFactor = raft.GetEffectiveReplicationFactor(range.PartitionId),
                HostedLocally = hosted
            };

            foreach (RaftReplica replica in range.Replicas)
                partition.Replicas.Add(new GrpcPartitionReplica
                {
                    Endpoint = replica.Endpoint,
                    Role = ToGrpcReplicaRole(replica.Role)
                });

            if (hosted && range.State != RaftPartitionState.Removed)
                response.HostedPartitionCount++;

            response.Partitions.Add(partition);
        }

        return Task.FromResult(response);
    }

    /// <summary>
    /// Commits a per-partition replication-factor override (0 clears it). Leader-only: a follower
    /// refuses with the reason so the caller retries against the meta-partition leader. Mirrors
    /// REST <c>POST /v1/cluster/replication-factor</c> 1:1.
    /// </summary>
    public override async Task<GrpcSetReplicationFactorResponse> SetReplicationFactor(
        GrpcSetReplicationFactorRequest request, ServerCallContext context)
    {
        if (request.PartitionId <= 0 || request.ReplicationFactor < 0)
            return new()
            {
                Success = false,
                Status = "InvalidInput",
                Reason = "PartitionId must be a data partition (> 0) and ReplicationFactor must be >= 0 (0 clears the override)."
            };

        try
        {
            RaftPartitionLifecycleResult result = await raft.SetReplicationFactorAsync(
                request.PartitionId, request.ReplicationFactor, context.CancellationToken).ConfigureAwait(false);

            return new()
            {
                Success = result.Success,
                Status = result.Status.ToString(),
                Generation = result.Generation,
                Reason = result.Success ? "" : "The override was not committed; see status."
            };
        }
        catch (RaftException ex)
        {
            return new()
            {
                Success = false,
                Status = "Refused",
                Reason = ex.Message
            };
        }
    }

    /// <summary>
    /// Returns the range-descriptor map as this node has applied it. Mirrors REST
    /// <c>GET /v1/ranges</c> 1:1 — same projection, same node-local routing modes, no leadership
    /// gate. An empty <c>KeySpace</c> asks for every space.
    /// </summary>
    public override Task<GrpcGetRangesResponse> GetRanges(GrpcGetRangesRequest request, ServerCallContext context)
    {
        KahunaRangeMapResponse map = keyValues.GetRangeMap(
            string.IsNullOrEmpty(request.KeySpace) ? null : request.KeySpace);

        GrpcGetRangesResponse response = new()
        {
            Initialized = map.Initialized,
            LocalEndpoint = map.LocalEndpoint
        };

        foreach (KahunaKeySpaceRangesResponse space in map.KeySpaces)
        {
            GrpcKeySpaceRanges entry = new()
            {
                KeySpace = space.KeySpace,
                RoutingMode = space.RoutingMode
            };

            foreach (KahunaRangeDescriptorResponse descriptor in space.Descriptors)
            {
                // Leaving a bound unset is what carries ±infinity; assigning null to an `optional`
                // field clears presence, so the two cases stay distinguishable on the wire and an
                // open end never arrives as an empty-string bound.
                GrpcRangeDescriptor range = new()
                {
                    PartitionId = descriptor.PartitionId,
                    Generation = descriptor.Generation
                };

                if (descriptor.StartKey is not null)
                    range.StartKey = descriptor.StartKey;

                if (descriptor.EndKey is not null)
                    range.EndKey = descriptor.EndKey;

                entry.Descriptors.Add(range);
            }

            response.KeySpaces.Add(entry);
        }

        return Task.FromResult(response);
    }

    /// <summary>
    /// Splits the range covering the given key. Mirrors REST <c>POST /v1/ranges/split</c> 1:1,
    /// including the leadership gate and the determinate/indeterminate split of the outcomes — both
    /// transports call the same manager method, so the classification cannot drift between them.
    /// </summary>
    public override async Task<GrpcSplitRangeResponse> SplitRange(
        GrpcSplitRangeRequest request, ServerCallContext context)
    {
        KahunaSplitRangeResponse outcome = await keyValues
            .SplitRangeAtKeyWithOutcomeAsync(request.KeySpace, request.SplitKey, context.CancellationToken)
            .ConfigureAwait(false);

        return new()
        {
            Success = outcome.Success,
            Status = outcome.Status,
            Determinate = outcome.Determinate,
            NewPartitionId = outcome.NewPartitionId,
            NewGeneration = outcome.NewGeneration,
            LeaderHint = outcome.LeaderHint ?? "",
            Reason = outcome.Reason ?? ""
        };
    }

    /// <summary>
    /// Runs the merge pass on demand. Mirrors REST <c>POST /v1/ranges/merge</c> 1:1: leader-only, so
    /// a merge count is never reported by a node that did not run the pass.
    /// </summary>
    public override async Task<GrpcMergeRangesResponse> MergeRanges(
        GrpcMergeRangesRequest request, ServerCallContext context)
    {
        KahunaMergeRangesResponse outcome = await keyValues
            .MergeRangesWithOutcomeAsync(context.CancellationToken)
            .ConfigureAwait(false);

        return new()
        {
            Success = outcome.Success,
            Status = outcome.Status,
            Determinate = outcome.Determinate,
            Merges = outcome.Merges,
            LeaderHint = outcome.LeaderHint ?? "",
            Reason = outcome.Reason ?? ""
        };
    }

    private static GrpcPartitionReplicaRole ToGrpcReplicaRole(RaftReplicaRole role) => role switch
    {
        RaftReplicaRole.Voter    => GrpcPartitionReplicaRole.PartitionReplicaRoleVoter,
        RaftReplicaRole.Learner  => GrpcPartitionReplicaRole.PartitionReplicaRoleLearner,
        RaftReplicaRole.Removing => GrpcPartitionReplicaRole.PartitionReplicaRoleRemoving,
        _                        => GrpcPartitionReplicaRole.PartitionReplicaRoleRemoving
    };

    private static GrpcLeaveClusterOutcome ToGrpcOutcome(LeaveClusterOutcome outcome) => outcome switch
    {
        LeaveClusterOutcome.Committed                 => GrpcLeaveClusterOutcome.LeaveClusterOutcomeCommitted,
        LeaveClusterOutcome.NotAMember                => GrpcLeaveClusterOutcome.LeaveClusterOutcomeNotAMember,
        LeaveClusterOutcome.RefusedInsufficientVoters => GrpcLeaveClusterOutcome.LeaveClusterOutcomeRefusedInsufficientVoters,
        LeaveClusterOutcome.NotInitialized            => GrpcLeaveClusterOutcome.LeaveClusterOutcomeNotInitialized,
        LeaveClusterOutcome.NoLeader                  => GrpcLeaveClusterOutcome.LeaveClusterOutcomeNoLeader,
        LeaveClusterOutcome.RefusedDrainInProgress    => GrpcLeaveClusterOutcome.LeaveClusterOutcomeRefusedDrainInProgress,
        LeaveClusterOutcome.DrainTimedOut             => GrpcLeaveClusterOutcome.LeaveClusterOutcomeDrainTimedOut,
        // Anything unrecognised is treated as unresolved rather than as a success: the caller must
        // re-read the roster instead of concluding the node left.
        _                                             => GrpcLeaveClusterOutcome.LeaveClusterOutcomeTimeout
    };

    private static GrpcClusterMemberRole ToGrpcRole(ClusterMemberRole role) => role switch
    {
        ClusterMemberRole.Learner   => GrpcClusterMemberRole.ClusterMemberRoleLearner,
        ClusterMemberRole.Voter     => GrpcClusterMemberRole.ClusterMemberRoleVoter,
        ClusterMemberRole.Leaving   => GrpcClusterMemberRole.ClusterMemberRoleLeaving,
        ClusterMemberRole.NotMember => GrpcClusterMemberRole.ClusterMemberRoleNotMember,
        _                           => GrpcClusterMemberRole.ClusterMemberRoleNotMember
    };
}


using Grpc.Core;
using Kommander;
using Kommander.Data;
using Kommander.System;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Provides gRPC endpoints exposing cluster membership topology and graceful decommission of the
/// local node. Mirrors the REST <c>/v1/cluster/membership</c> and <c>/v1/cluster/leave</c>
/// endpoints 1:1.
/// </summary>
public sealed class ClusterService : Cluster.ClusterBase
{
    private readonly IRaft raft;

    public ClusterService(IRaft raft)
    {
        this.raft = raft;
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
            Outcome = ToGrpcOutcome(result.Outcome),
            MembershipVersion = result.MembershipVersion,
            Retryable = ClusterLeave.IsRetryable(result.Outcome),
            Reason = ClusterLeave.ToReason(result.Outcome)
        };
    }

    private static GrpcLeaveClusterOutcome ToGrpcOutcome(LeaveClusterOutcome outcome) => outcome switch
    {
        LeaveClusterOutcome.Committed                 => GrpcLeaveClusterOutcome.LeaveClusterOutcomeCommitted,
        LeaveClusterOutcome.NotAMember                => GrpcLeaveClusterOutcome.LeaveClusterOutcomeNotAMember,
        LeaveClusterOutcome.RefusedInsufficientVoters => GrpcLeaveClusterOutcome.LeaveClusterOutcomeRefusedInsufficientVoters,
        LeaveClusterOutcome.NotInitialized            => GrpcLeaveClusterOutcome.LeaveClusterOutcomeNotInitialized,
        LeaveClusterOutcome.NoLeader                  => GrpcLeaveClusterOutcome.LeaveClusterOutcomeNoLeader,
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

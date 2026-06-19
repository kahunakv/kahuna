
using Grpc.Core;
using Kommander;
using Kommander.System;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Provides a read-only gRPC endpoint exposing cluster membership topology.
/// Mirrors the REST <c>GET /v1/cluster/membership</c> endpoint 1:1.
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
            MembershipVersion = membership.MembershipVersion
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

    private static GrpcClusterMemberRole ToGrpcRole(ClusterMemberRole role) => role switch
    {
        ClusterMemberRole.Learner   => GrpcClusterMemberRole.ClusterMemberRoleLearner,
        ClusterMemberRole.Voter     => GrpcClusterMemberRole.ClusterMemberRoleVoter,
        ClusterMemberRole.Leaving   => GrpcClusterMemberRole.ClusterMemberRoleLeaving,
        ClusterMemberRole.NotMember => GrpcClusterMemberRole.ClusterMemberRoleNotMember,
        _                           => GrpcClusterMemberRole.ClusterMemberRoleNotMember
    };
}

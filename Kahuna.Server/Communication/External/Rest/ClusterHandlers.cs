
using Kommander;
using Kommander.System;
using Kahuna.Shared.Communication.Rest;

namespace Kahuna.Communication.External.Rest;

/// <summary>
/// Provides read-only REST endpoints exposing cluster membership topology.
/// </summary>
public static class ClusterHandlers
{
    public static void MapClusterRoutes(WebApplication app)
    {
        app.MapGet("/v1/cluster/membership", (IRaft raft) =>
        {
            ClusterMembership membership = raft.GetMembership();
            string localEndpoint = raft.GetLocalEndpoint();

            List<KahunaClusterMemberResponse> members = new(membership.Members.Count);
            string localRole = ClusterMemberRole.NotMember.ToString();

            foreach (ClusterMember m in membership.Members)
            {
                members.Add(new KahunaClusterMemberResponse
                {
                    Endpoint = m.Endpoint,
                    NodeId = m.NodeId,
                    Role = m.Role.ToString(),
                    JoinedVersion = m.JoinedVersion
                });

                if (m.Endpoint == localEndpoint)
                    localRole = m.Role.ToString();
            }

            return new KahunaClusterMembershipResponse
            {
                MembershipVersion = membership.MembershipVersion,
                Members = members,
                LocalRole = localRole
            };
        });
    }
}

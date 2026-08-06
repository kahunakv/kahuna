
using System.Text.Json;
using Kommander;
using Kommander.System;
using Kahuna.Shared.Communication.Rest;

namespace Kahuna.Communication.External.Rest;

/// <summary>
/// Provides read-only REST endpoints exposing cluster membership topology and node readiness.
/// </summary>
public static class ClusterHandlers
{
    public static void MapClusterRoutes(WebApplication app)
    {
        app.MapGet("/v1/cluster/membership", (IRaft raft) => BuildMembershipResponse(raft));

        // Readiness probe: a node opens its port and answers membership queries roughly a second
        // after launch, but refuses every key/value request until cluster initialization completes
        // (partition map received and applied) — a window that can last tens of seconds after a
        // restart. This route makes the two states distinguishable: 200 when the node can serve,
        // 503 while it cannot, so load balancers / Kubernetes probes / client node pickers can
        // route traffic elsewhere without sending a real request just to be refused.
        app.MapGet("/v1/cluster/health", (IRaft raft) =>
        {
            KahunaClusterHealthResponse health = BuildHealthResponse(raft);

            return Results.Text(
                JsonSerializer.Serialize(health, KahunaJsonContext.Default.KahunaClusterHealthResponse),
                "application/json",
                statusCode: health.Ready ? StatusCodes.Status200OK : StatusCodes.Status503ServiceUnavailable);
        });
    }

    public static KahunaClusterMembershipResponse BuildMembershipResponse(IRaft raft)
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
            LocalRole = localRole,
            Initialized = raft.IsInitialized
        };
    }

    /// <summary>
    /// Ready requires both conditions: initialization complete (otherwise no partition leader can
    /// be resolved and every request returns MustRetry) and a serving role in the roster (a node
    /// evicted while down reports NotMember and likewise cannot serve). The role alone is
    /// misleading — during initialization a node can report Voter while unable to serve a single
    /// read — which is why the flag gates first.
    /// </summary>
    public static KahunaClusterHealthResponse BuildHealthResponse(IRaft raft)
    {
        bool initialized;
        string localRole;

        try
        {
            initialized = raft.IsInitialized;
            localRole = raft.LocalRole.ToString();
        }
        catch (Exception)
        {
            // A node so early in boot that membership state is not constructed yet cannot serve;
            // fail closed (503) rather than surface an unclassifiable 500 to orchestrator probes.
            return new()
            {
                Ready = false,
                Initialized = false,
                LocalRole = ClusterMemberRole.NotMember.ToString()
            };
        }

        return new()
        {
            Ready = initialized && localRole != nameof(ClusterMemberRole.NotMember),
            Initialized = initialized,
            LocalRole = localRole
        };
    }
}

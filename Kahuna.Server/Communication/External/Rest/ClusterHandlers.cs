
using System.Text.Json;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kahuna.Shared.Communication.Rest;
using Microsoft.AspNetCore.Mvc;

namespace Kahuna.Communication.External.Rest;

/// <summary>
/// Provides REST endpoints exposing cluster membership topology, node readiness, and graceful
/// decommission of the local node.
/// </summary>
public static class ClusterHandlers
{
    public static void MapClusterRoutes(WebApplication app)
    {
        app.MapGet("/v1/cluster/membership", (IRaft raft) => BuildMembershipResponse(raft));

        // Decommission: commit this node's removal from the roster now, instead of stopping the
        // process and making the cluster infer the departure from silence — which costs the
        // suspicion timeout plus the eviction grace per node, and logs a planned removal as a
        // failure. The node deliberately keeps serving its port afterwards so the caller can read
        // the committed result before stopping the process; stopping is the caller's next step.
        app.MapPost("/v1/cluster/leave", async (IRaft raft, HttpContext httpContext) =>
        {
            LeaveClusterResult result = await ClusterLeave.ExecuteAsync(raft, httpContext.RequestAborted)
                .ConfigureAwait(false);

            return Results.Text(
                JsonSerializer.Serialize(
                    ClusterLeave.ToResponse(result), KahunaJsonContext.Default.KahunaClusterLeaveResponse),
                "application/json",
                statusCode: ClusterLeave.ToStatusCode(result.Outcome));
        });

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

        // Placement table: which nodes host each partition and in what role, the effective
        // replication factor, and which partitions the answering node hosts locally. Under full
        // replication every replica set is empty and everything is hosted locally.
        app.MapGet("/v1/cluster/placement", (IRaft raft) => Results.Text(
            JsonSerializer.Serialize(
                BuildPlacementResponse(raft), KahunaJsonContext.Default.KahunaClusterPlacementResponse),
            "application/json"));

        // Per-partition replication-factor override (0 clears it). Leader-only like every map
        // mutation: a follower refuses with the reason and the caller retries against the
        // meta-partition leader. The change adjusts the placement target only — the rebalancer
        // moves replicas toward it on later passes.
        app.MapPost("/v1/cluster/replication-factor", async (
            [FromBody] KahunaSetReplicationFactorRequest req, IRaft raft, HttpContext httpContext) =>
        {
            KahunaSetReplicationFactorResponse response =
                await SetReplicationFactorAsync(raft, req, httpContext.RequestAborted).ConfigureAwait(false);

            return Results.Text(
                JsonSerializer.Serialize(response, KahunaJsonContext.Default.KahunaSetReplicationFactorResponse),
                "application/json",
                statusCode: response.Success ? StatusCodes.Status200OK : StatusCodes.Status409Conflict);
        });
    }

    /// <summary>
    /// Commits a per-partition replication-factor override and maps the outcome to the wire shape.
    /// Kommander refuses by throwing (not initialized, system partition, follower); those become a
    /// non-success response carrying the reason, so the caller can retry against the leader instead
    /// of receiving an opaque 500.
    /// </summary>
    public static async Task<KahunaSetReplicationFactorResponse> SetReplicationFactorAsync(
        IRaft raft, KahunaSetReplicationFactorRequest req, CancellationToken cancellationToken)
    {
        if (req.PartitionId <= 0 || req.ReplicationFactor < 0)
            return new()
            {
                Success = false,
                Status = "InvalidInput",
                Reason = "partitionId must be a data partition (> 0) and replicationFactor must be >= 0 (0 clears the override)."
            };

        try
        {
            RaftPartitionLifecycleResult result = await raft.SetReplicationFactorAsync(
                req.PartitionId, req.ReplicationFactor, cancellationToken).ConfigureAwait(false);

            return new()
            {
                Success = result.Success,
                Status = result.Status.ToString(),
                Generation = result.Generation,
                Reason = result.Success ? null : "The override was not committed; see status."
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
    /// Builds the placement table from the committed partition map. The hosted flags describe the
    /// answering node; every node returns the same map (it is committed on the meta partition), so
    /// asking a second node only changes the local perspective fields.
    /// </summary>
    public static KahunaClusterPlacementResponse BuildPlacementResponse(IRaft raft)
    {
        KahunaClusterPlacementResponse response = new()
        {
            ReplicationFactor = raft.Configuration.ReplicationFactor,
            RebalancerEnabled = raft.Configuration.EnablePlacementRebalancer,
            Initialized = raft.IsInitialized,
            LocalEndpoint = raft.GetLocalEndpoint()
        };

        foreach (RaftPartitionRange range in raft.GetPartitionMap())
        {
            bool hosted = raft.HostsPartition(range.PartitionId);

            KahunaPartitionPlacementResponse partition = new()
            {
                PartitionId = range.PartitionId,
                State = range.State.ToString(),
                Generation = range.Generation,
                EffectiveReplicationFactor = raft.GetEffectiveReplicationFactor(range.PartitionId),
                HostedLocally = hosted
            };

            foreach (RaftReplica replica in range.Replicas)
                partition.Replicas.Add(new KahunaPartitionReplicaResponse
                {
                    Endpoint = replica.Endpoint,
                    Role = replica.Role.ToString()
                });

            if (hosted && range.State != RaftPartitionState.Removed)
                response.HostedPartitionCount++;

            response.Partitions.Add(partition);
        }

        return response;
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
    /// be resolved and every request returns MustRetry) and a serving role in the roster. The role
    /// alone is misleading — during initialization a node can report Voter while unable to serve a
    /// single read — which is why the flag gates first.
    /// <para>
    /// Neither <c>NotMember</c> (evicted while down) nor <c>Leaving</c> (decommissioned, on its way
    /// out) is a serving role. Reporting a decommissioned node as ready would keep load balancers
    /// routing to a node the cluster has already dropped, which is the opposite of what asking it
    /// to leave was for.
    /// </para>
    /// </summary>
    public static KahunaClusterHealthResponse BuildHealthResponse(IRaft raft)
    {
        bool initialized;
        string localRole;
        int hostedPartitions = 0;

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

        // Informational only, never a readiness condition: a node hosting zero data partitions
        // still serves every key by forwarding to the hosting nodes — so a failure computing the
        // count must not flip readiness either.
        if (initialized)
        {
            try
            {
                foreach (RaftPartitionRange range in raft.GetPartitionMap())
                    if (range.State != RaftPartitionState.Removed && raft.HostsPartition(range.PartitionId))
                        hostedPartitions++;
            }
            catch (Exception)
            {
                hostedPartitions = 0;
            }
        }

        return new()
        {
            Ready = initialized
                && localRole != nameof(ClusterMemberRole.NotMember)
                && localRole != nameof(ClusterMemberRole.Leaving),
            Initialized = initialized,
            LocalRole = localRole,
            HostedPartitions = hostedPartitions
        };
    }
}


using Kommander;
using Kommander.System;

namespace Kahuna.Server;

/// <summary>
/// Placement-safe wrappers around the Kommander leadership APIs. Under per-partition replica
/// placement (replication factor &gt; 0) most ranges are not materialized on a given node, and the
/// raw <see cref="IRaft.AmILeader"/> / <see cref="IRaft.WaitForLeader"/> /
/// <see cref="IRaft.ConfirmLeadershipAsync"/> throw <see cref="PartitionNotHostedException"/> for
/// them. These wrappers turn that expected routing condition into <see langword="false"/> /
/// <see langword="null"/> so a caller answers <c>MustRetry</c> (or forwards) instead of leaking a
/// server error — with the replication factor off (every range's replica set empty) every
/// partition is hosted and the wrappers behave exactly like the raw calls.
///
/// <para>
/// The <see cref="IRaft.HostsPartition"/> check is an advisory fast path, not a correctness gate:
/// the committed map can list this node as a replica moments before the partition materializes
/// locally, so the guard and the throw inherently race. The typed catch is the authoritative
/// answer; both resolve to the same retryable outcome. Every other exception — including the
/// plain <see cref="RaftException"/> for a partition id that does not exist in the committed map
/// at all — propagates unchanged: that is a caller error or an election-window condition the
/// call sites already map per-operation.
/// </para>
///
/// <para>
/// All leadership questions in Kahuna go through these wrappers, including partition-0 scoped
/// ones (range map, snapshot floor, split/merge triggers) where the guard can never trip — one
/// pattern everywhere beats a per-site judgment call about which partitions are "safe".
/// </para>
/// </summary>
internal static class PartitionLeaderResolver
{
    /// <summary>
    /// <see cref="IRaft.AmILeader"/> that answers <see langword="false"/> for a partition this
    /// node does not host instead of throwing. A false answer means "not the leader here" for
    /// any reason; callers fall through to their existing resolve-and-forward or retry path.
    /// </summary>
    public static async ValueTask<bool> AmILeaderIfHosted(this IRaft raft, int partitionId, CancellationToken cancellationToken)
    {
        if (!raft.HostsPartition(partitionId))
            return false;

        try
        {
            return await raft.AmILeader(partitionId, cancellationToken).ConfigureAwait(false);
        }
        catch (PartitionNotHostedException)
        {
            return false;
        }
    }

    /// <summary>
    /// <see cref="IRaft.AmILeaderQuick"/> that answers <see langword="false"/> for a partition
    /// this node does not host instead of throwing.
    /// </summary>
    public static async ValueTask<bool> AmILeaderQuickIfHosted(this IRaft raft, int partitionId)
    {
        if (!raft.HostsPartition(partitionId))
            return false;

        try
        {
            return await raft.AmILeaderQuick(partitionId).ConfigureAwait(false);
        }
        catch (PartitionNotHostedException)
        {
            return false;
        }
    }

    /// <summary>
    /// <see cref="IRaft.ConfirmLeadershipAsync"/> (read-index) that answers <see langword="false"/>
    /// for a partition this node does not host instead of throwing. False keeps its existing
    /// meaning at every call site: do not serve an authoritative read from local state.
    /// </summary>
    public static async ValueTask<bool> ConfirmLeadershipIfHosted(this IRaft raft, int partitionId, CancellationToken cancellationToken)
    {
        if (!raft.HostsPartition(partitionId))
            return false;

        try
        {
            return await raft.ConfirmLeadershipAsync(partitionId, cancellationToken).ConfigureAwait(false);
        }
        catch (PartitionNotHostedException)
        {
            return false;
        }
    }

    /// <summary>
    /// <see cref="IRaft.WaitForLeader"/> extended with replica-placement targeting. For a hosted
    /// partition it behaves exactly like the raw call. For a partition this node does not host it
    /// answers the best forward target instead of throwing: the gossip-fed leader hint when one is
    /// known, else the first remote voter replica from the committed map — the receiver hosts the
    /// range, so it serves the operation or redirects once to its own leader. The target is
    /// best-effort, never a correctness gate; the receiver's leadership check is authoritative.
    ///
    /// <para>
    /// <see langword="null"/> means "no way to serve this from here" and the caller answers
    /// <c>MustRetry</c>: the range is legacy-but-unmaterialized, no replica is known yet, or this
    /// node is itself serving a forwarded request (<see cref="ForwardedRequestScope"/>) — a
    /// non-hosting receiver never forwards onward, or two nodes with mutually stale placement
    /// views could bounce an operation between them indefinitely. Every other resolution failure
    /// (node not ready, election still undecided) propagates so the call sites' existing
    /// catch-and-log handling keeps its diagnostics.
    /// </para>
    /// </summary>
    public static async ValueTask<string?> TryResolveLeader(this IRaft raft, int partitionId, CancellationToken cancellationToken)
    {
        if (!raft.HostsPartition(partitionId))
            return TryPickRemoteReplicaTarget(raft, partitionId);

        try
        {
            return await raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
        }
        catch (PartitionNotHostedException)
        {
            return TryPickRemoteReplicaTarget(raft, partitionId);
        }
    }

    /// <summary>
    /// Picks the node to forward an operation on a non-hosted partition to: the believed leader
    /// when the gossiped hint knows one, else the first remote voter from the committed replica
    /// set, else a transitional replica (it still hosts the range and can redirect to its
    /// leader). Null when nothing is known — or when this node is itself serving a forwarded
    /// request and must not chain forwards.
    /// </summary>
    private static string? TryPickRemoteReplicaTarget(IRaft raft, int partitionId)
    {
        // A chained forward is refused by design, not a placement-health signal — not counted.
        if (ForwardedRequestScope.IsActive)
            return null;

        string localEndpoint = raft.GetLocalEndpoint();

        string? hint = raft.GetPartitionLeaderHint(partitionId);
        if (!string.IsNullOrEmpty(hint) && !string.Equals(hint, localEndpoint, StringComparison.Ordinal))
        {
            PlacementMetrics.LeaderHintHits.Add(1);
            PlacementMetrics.ForwardsResolved.Add(1);
            return hint;
        }

        PlacementMetrics.LeaderHintMisses.Add(1);

        IReadOnlyList<RaftReplica> replicas = raft.GetPartitionReplicas(partitionId);

        foreach (RaftReplica replica in replicas)
            if (replica.Role == RaftReplicaRole.Voter && !string.Equals(replica.Endpoint, localEndpoint, StringComparison.Ordinal))
            {
                PlacementMetrics.ForwardsResolved.Add(1);
                return replica.Endpoint;
            }

        foreach (RaftReplica replica in replicas)
            if (replica.Role != RaftReplicaRole.Voter && !string.Equals(replica.Endpoint, localEndpoint, StringComparison.Ordinal))
            {
                PlacementMetrics.ForwardsResolved.Add(1);
                return replica.Endpoint;
            }

        PlacementMetrics.ForwardsUnresolved.Add(1);
        return null;
    }
}

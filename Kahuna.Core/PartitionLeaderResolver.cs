
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
    /// partition it answers that partition's leader. For a partition this node does not host it
    /// answers the best forward target instead of throwing: the gossip-fed leader hint when one is
    /// known, else the first remote voter replica from the committed map — the receiver hosts the
    /// range, so it serves the operation. The target is best-effort, never a correctness gate; the
    /// receiver's own leadership check is authoritative.
    ///
    /// <para>
    /// <see langword="null"/> means "no way to serve this from here" and the caller answers
    /// <c>MustRetry</c>: the range is legacy-but-unmaterialized, no replica is known yet, or this
    /// node is itself serving a request another node forwarded here
    /// (<see cref="ForwardedRequestScope"/>). Every other resolution failure (node not ready,
    /// election still undecided) propagates so the call sites' existing catch-and-log handling
    /// keeps its diagnostics.
    /// </para>
    ///
    /// <para><b>The forward chain is budgeted.</b> A hosted partition's leader is resolved from this
    /// node's own Raft state, which is stale during an election window: it still names the previous
    /// leader while that node has already stepped down and names this one. Each node would then
    /// forward to the other for as long as the disagreement lasts — an unbounded request loop on
    /// the gRPC transport, and mutual recursion ending in a fatal stack overflow on the in-memory
    /// transport, whose forward is a direct in-process call. Hosting the partition does not make
    /// the belief any less stale, so the budget in
    /// <see cref="ForwardedRequestScope.MaxForwardHops"/> covers this branch too: an operation that
    /// has already spent it resolves no remote target and its caller answers <c>MustRetry</c>,
    /// which costs one retry against a settled view. A non-hosting receiver is stricter still — see
    /// <see cref="TryPickRemoteReplicaTarget"/>.</para>
    /// </summary>
    public static async ValueTask<string?> TryResolveLeader(this IRaft raft, int partitionId, CancellationToken cancellationToken)
    {
        if (!raft.HostsPartition(partitionId))
            return TryPickRemoteReplicaTarget(raft, partitionId);

        string leader;

        try
        {
            leader = await raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
        }
        catch (PartitionNotHostedException)
        {
            return TryPickRemoteReplicaTarget(raft, partitionId);
        }

        // Answering with this node's own endpoint is not a forward — the caller serves the
        // operation locally — so the budget only gates a remote answer.
        if (ForwardedRequestScope.CanForward || string.Equals(leader, raft.GetLocalEndpoint(), StringComparison.Ordinal))
            return leader;

        PlacementMetrics.ChainedForwardsRefused.Add(1);

        return null;
    }

    /// <summary>
    /// Picks the node to forward an operation on a non-hosted partition to: the believed leader
    /// when the gossiped hint knows one, else the first remote voter from the committed replica
    /// set, else a transitional replica (it still hosts the range and can redirect to its
    /// leader). Null when nothing is known — or when this node is itself serving a forwarded
    /// request.
    ///
    /// <para>A forwarded request is refused here outright rather than against the hop budget: a
    /// node that does not host the partition has nothing but the same committed map the sender
    /// already read, so its target is another guess, not a better answer. Chaining guesses only
    /// lets two nodes with mutually stale placement views bounce the operation between them.</para>
    /// </summary>
    private static string? TryPickRemoteReplicaTarget(IRaft raft, int partitionId)
    {
        // A chained forward is refused by design, not a placement-health signal — it is counted on
        // its own instrument rather than as an unresolved forward.
        if (ForwardedRequestScope.IsActive)
        {
            PlacementMetrics.ChainedForwardsRefused.Add(1);

            return null;
        }

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

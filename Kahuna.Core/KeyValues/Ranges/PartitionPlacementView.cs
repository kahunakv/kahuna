
using Kommander;
using Kommander.System;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Per-node projection of the committed partition map answering "does this node host partition P,
/// and who else does?". This is the single Kahuna-side authority for placement-aware routing and
/// background-machinery gating; everything that needs a hosted-set answer reads it here instead of
/// interpreting <see cref="RaftPartitionRange.Replicas"/> ad hoc.
///
/// <para>
/// A range with an <b>empty replica set is legacy full replication</b> — every node hosts it — so
/// with the replication factor off (RF = 0) every answer matches today's behavior and consumers'
/// placement branches are inert. Partition 0 (the Kommander system partition) always replicates
/// everywhere and is always reported as hosted.
/// </para>
///
/// <para>
/// <b>Coherence contract:</b> this is a projection of the committed map applied on this node, so it
/// is always <em>behind</em> the cluster, never ahead. Consumers must treat a wrong answer as
/// retryable, never as an error: "reported hosted but the partition is not materialized yet" and
/// "reported remote but this node just gained the replica" are both real interleavings. The
/// authoritative answer stays with Kommander (a <see cref="PartitionNotHostedException"/> from the
/// leadership APIs); this view only picks the fast path.
/// </para>
///
/// <para>
/// Snapshots are immutable and swapped atomically, so readers never block and never observe a
/// partially applied map. Rebuilds re-read <see cref="IRaft.GetPartitionMap"/> instead of trusting
/// the event payload, so a rebuild can never regress to an older map than one already applied.
/// <see cref="HostedPartitionsChanged"/> fires on the map-application thread — handlers must be
/// fast and must not block.
/// </para>
/// </summary>
public sealed class PartitionPlacementView : IDisposable
{
    /// <summary>Placement of one partition as seen by this node.</summary>
    private readonly record struct PartitionPlacement(bool Hosted, string[] RemoteReplicaEndpoints);

    private sealed class Snapshot
    {
        public required Dictionary<int, PartitionPlacement> Placements { get; init; }

        public required HashSet<int> HostedPartitions { get; init; }
    }

    private static readonly string[] NoEndpoints = [];

    private readonly IRaft raft;

    private readonly string localEndpoint;

    private readonly object rebuildLock = new();

    private volatile Snapshot current;

    private bool disposed;

    /// <summary>
    /// Fired after the hosted set changes: first argument is the partitions gained, second the
    /// partitions lost, each non-null and fired at most once per transition. A partition that
    /// leaves the committed map entirely (merged away) is reported as lost — its per-partition
    /// state on this node is equally dead either way. Raised on the map-application thread;
    /// handlers must be fast and must not block.
    /// </summary>
    public event Action<IReadOnlySet<int>, IReadOnlySet<int>>? HostedPartitionsChanged;

    public PartitionPlacementView(IRaft raft)
    {
        this.raft = raft;
        localEndpoint = raft.GetLocalEndpoint();

        // Subscribe before seeding so a map applied between the two is never missed; Rebuild
        // re-reads the committed map, so whichever of the seed and a racing event runs last
        // still lands on the freshest state.
        raft.OnPartitionMapChanged += HandleMapChanged;
        current = BuildSnapshot(raft.GetPartitionMap());
    }

    /// <summary>
    /// Whether this node hosts <paramref name="partitionId"/>: the range's replica set is empty
    /// (legacy full replication) or lists the local endpoint in any role. Partition 0 and
    /// partitions unknown to the current snapshot report hosted — the unknown case means the map
    /// is behind (or not yet applied), where today's full-replication path is the correct
    /// fallback and Kommander remains the authority.
    /// </summary>
    public bool IsLocallyHosted(int partitionId)
    {
        if (partitionId == RangeMapStore.MetaPartitionId)
            return true;

        return !current.Placements.TryGetValue(partitionId, out PartitionPlacement placement) || placement.Hosted;
    }

    /// <summary>
    /// Remote endpoints hosting <paramref name="partitionId"/>, voters first, then transitional
    /// replicas (learners/removing), the local endpoint excluded. Empty when the range is legacy
    /// (empty replica set) or unknown — the caller falls back to today's full-replication path.
    /// </summary>
    public IReadOnlyList<string> ReplicaEndpoints(int partitionId)
    {
        return current.Placements.TryGetValue(partitionId, out PartitionPlacement placement)
            ? placement.RemoteReplicaEndpoints
            : NoEndpoints;
    }

    /// <summary>
    /// The data partitions this node currently hosts (legacy ranges included). Partition 0 is
    /// always hosted and never listed. Each call returns the immutable set of one snapshot.
    /// </summary>
    public IReadOnlySet<int> HostedPartitions => current.HostedPartitions;

    private void HandleMapChanged(IReadOnlyList<RaftPartitionRange> _)
    {
        HashSet<int> gained;
        HashSet<int> lost;

        lock (rebuildLock)
        {
            Snapshot previous = current;
            Snapshot next = BuildSnapshot(raft.GetPartitionMap());

            gained = [.. next.HostedPartitions];
            gained.ExceptWith(previous.HostedPartitions);

            lost = [.. previous.HostedPartitions];
            lost.ExceptWith(next.HostedPartitions);

            current = next;

            // Raised inside the lock so transitions are delivered in snapshot order; the
            // map-application thread is the only caller, so there is no lock contention to fear.
            if (gained.Count > 0 || lost.Count > 0)
                HostedPartitionsChanged?.Invoke(gained, lost);
        }
    }

    private Snapshot BuildSnapshot(IReadOnlyList<RaftPartitionRange> ranges)
    {
        Dictionary<int, PartitionPlacement> placements = new(ranges.Count);
        HashSet<int> hosted = [];

        foreach (RaftPartitionRange range in ranges)
        {
            if (range.State == RaftPartitionState.Removed || range.PartitionId == RangeMapStore.MetaPartitionId)
                continue;

            bool isHosted;
            string[] remoteEndpoints;

            if (range.Replicas.Count == 0)
            {
                isHosted = true;
                remoteEndpoints = NoEndpoints;
            }
            else
            {
                isHosted = false;
                List<string> voters = [];
                List<string> transitional = [];

                foreach (RaftReplica replica in range.Replicas)
                {
                    if (string.Equals(replica.Endpoint, localEndpoint, StringComparison.Ordinal))
                    {
                        isHosted = true;
                        continue;
                    }

                    if (replica.Role == RaftReplicaRole.Voter)
                        voters.Add(replica.Endpoint);
                    else
                        transitional.Add(replica.Endpoint);
                }

                remoteEndpoints = new string[voters.Count + transitional.Count];
                voters.CopyTo(remoteEndpoints);
                transitional.CopyTo(remoteEndpoints, voters.Count);
            }

            placements[range.PartitionId] = new(isHosted, remoteEndpoints);

            if (isHosted)
                hosted.Add(range.PartitionId);
        }

        return new() { Placements = placements, HostedPartitions = hosted };
    }

    public void Dispose()
    {
        if (disposed)
            return;

        disposed = true;
        raft.OnPartitionMapChanged -= HandleMapChanged;
    }
}

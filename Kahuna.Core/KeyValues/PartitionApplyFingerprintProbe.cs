
using Kommander;
using Kommander.System;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Compares one partition's apply fingerprint across its replicas. The committed-head ledger is a pure
/// function of the log, so two replicas at the same applied kv log id must hold the same number of
/// committed heads; a replica that does not is one whose apply stream diverged — a snapshot install it
/// acknowledged but never imported, a bundled commit it alone rejected. Such a replica serves reads
/// that miss acknowledged writes once it leads, and a split that copies from it moves the loss into a
/// new partition. The comparison runs at two moments: when this node is promoted (to make the
/// divergence visible in the cluster's own signals) and before a split's bulk copy (to refuse copying
/// from an incomplete source).
/// </summary>
/// <remarks>
/// A peer that does not answer inside <see cref="PeerTimeoutMs"/> counts as unknown, never as
/// divergent. A peer at a different applied log id is simply behind or ahead and is not compared: only
/// equal ids make the counts comparable.
/// </remarks>
internal sealed class PartitionApplyFingerprintProbe
{
    /// <summary>Bound on one peer's answer. A slow peer must not hold a promotion report or a split.</summary>
    internal const int PeerTimeoutMs = 2_000;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly Func<int, KeyValueApplyFingerprint?> readLocal;

    public PartitionApplyFingerprintProbe(
        IRaft raft,
        IInterNodeCommunication interNodeCommunication,
        Func<int, KeyValueApplyFingerprint?> readLocal)
    {
        this.raft = raft;
        this.interNodeCommunication = interNodeCommunication;
        this.readLocal = readLocal;
    }

    /// <summary>
    /// Compares the partition leader's fingerprint with every other replica's. The leader is this node
    /// when it leads the partition, else the leader this node resolves; the leader's own fingerprint is
    /// read locally or fetched. Returns an indeterminate comparison when no leader resolves or the
    /// leader did not answer — the caller must not read that as "complete".
    /// </summary>
    public async Task<ApplyFingerprintComparison> CompareWithReplicasAsync(int partitionId, CancellationToken cancellationToken)
    {
        string local = raft.GetLocalEndpoint();

        string? leader;

        try
        {
            leader = await raft.AmILeaderIfHosted(partitionId, cancellationToken).ConfigureAwait(false)
                ? local
                : await raft.TryResolveLeader(partitionId, cancellationToken).ConfigureAwait(false);
        }
        catch (RaftNodeNotReadyException)
        {
            // Cluster initialization still in flight (a promotion at startup): nothing to compare yet.
            return ApplyFingerprintComparison.Indeterminate(partitionId);
        }
        catch (RaftException)
        {
            // No leader resolved inside the wait bound: the comparison cannot name a reference.
            return ApplyFingerprintComparison.Indeterminate(partitionId);
        }

        if (leader is null)
            return ApplyFingerprintComparison.Indeterminate(partitionId);

        KeyValueApplyFingerprint? leaderFingerprint = leader == local
            ? readLocal(partitionId)
            : await FetchAsync(leader, partitionId, cancellationToken).ConfigureAwait(false);

        if (leaderFingerprint is null)
            return ApplyFingerprintComparison.Indeterminate(partitionId, leader);

        List<string> peers = PeersOf(partitionId, leader);
        if (peers.Count == 0)
            return new ApplyFingerprintComparison(partitionId, leader, leaderFingerprint.Value, [], 0, 0);

        Task<KeyValueApplyFingerprint?>[] fetches = new Task<KeyValueApplyFingerprint?>[peers.Count];
        for (int i = 0; i < peers.Count; i++)
        {
            string peer = peers[i];
            fetches[i] = peer == local
                ? Task.FromResult(readLocal(partitionId))
                : FetchAsync(peer, partitionId, cancellationToken);
        }

        await Task.WhenAll(fetches).ConfigureAwait(false);

        List<(string Peer, KeyValueApplyFingerprint Fingerprint)> divergent = [];
        int answered = 0;

        for (int i = 0; i < peers.Count; i++)
        {
            KeyValueApplyFingerprint? peerFingerprint = fetches[i].Result;
            if (peerFingerprint is null)
                continue;

            answered++;

            if (peerFingerprint.Value.AppliedLogId == leaderFingerprint.Value.AppliedLogId
                && peerFingerprint.Value.CommittedHeads != leaderFingerprint.Value.CommittedHeads)
                divergent.Add((peers[i], peerFingerprint.Value));
        }

        return new ApplyFingerprintComparison(partitionId, leader, leaderFingerprint.Value, divergent, peers.Count, answered);
    }

    /// <summary>
    /// Every replica of the partition except <paramref name="leader"/>: the committed replica set when
    /// the partition is placed, else every cluster node (legacy full replication hosts a partition
    /// everywhere).
    /// </summary>
    private List<string> PeersOf(int partitionId, string leader)
    {
        IReadOnlyList<RaftReplica> replicas = raft.GetPartitionReplicas(partitionId);
        List<string> peers;

        if (replicas.Count > 0)
        {
            peers = new List<string>(replicas.Count);
            foreach (RaftReplica replica in replicas)
                if (replica.Endpoint != leader)
                    peers.Add(replica.Endpoint);

            return peers;
        }

        IList<RaftNode> nodes = raft.GetNodes();
        peers = new List<string>(nodes.Count + 1);
        string local = raft.GetLocalEndpoint();

        if (local != leader)
            peers.Add(local);

        foreach (RaftNode node in nodes)
            if (node.Endpoint != leader && node.Endpoint != local)
                peers.Add(node.Endpoint);

        return peers;
    }

    private async Task<KeyValueApplyFingerprint?> FetchAsync(string node, int partitionId, CancellationToken cancellationToken)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(PeerTimeoutMs);

        try
        {
            (KeyValueResponseType type, KeyValueApplyFingerprint fingerprint) =
                await interNodeCommunication.GetPartitionApplyFingerprint(node, partitionId, timeout.Token).ConfigureAwait(false);

            return type == KeyValueResponseType.Get ? fingerprint : null;
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // An unreachable or slow peer is unknown, not divergent. The caller's own token still
            // propagates: only the per-peer bound and transport failures are absorbed here.
            return null;
        }
    }
}

/// <summary>Result of <see cref="PartitionApplyFingerprintProbe.CompareWithReplicasAsync"/>.</summary>
internal sealed class ApplyFingerprintComparison
{
    public int PartitionId { get; }

    /// <summary>The leader whose fingerprint was the reference, or null when none resolved.</summary>
    public string? Leader { get; }

    public KeyValueApplyFingerprint LeaderFingerprint { get; }

    /// <summary>Peers at the leader's applied log id whose committed-head count differs from the leader's.</summary>
    public IReadOnlyList<(string Peer, KeyValueApplyFingerprint Fingerprint)> Divergent { get; }

    public int PeersAsked { get; }

    public int PeersAnswered { get; }

    /// <summary>False when no leader resolved or the leader's own fingerprint could not be read.</summary>
    public bool IsDeterminate { get; }

    public bool HasDivergence => Divergent.Count > 0;

    public ApplyFingerprintComparison(
        int partitionId,
        string leader,
        KeyValueApplyFingerprint leaderFingerprint,
        IReadOnlyList<(string Peer, KeyValueApplyFingerprint Fingerprint)> divergent,
        int peersAsked,
        int peersAnswered)
    {
        PartitionId = partitionId;
        Leader = leader;
        LeaderFingerprint = leaderFingerprint;
        Divergent = divergent;
        PeersAsked = peersAsked;
        PeersAnswered = peersAnswered;
        IsDeterminate = true;
    }

    private ApplyFingerprintComparison(int partitionId, string? leader)
    {
        PartitionId = partitionId;
        Leader = leader;
        Divergent = [];
        IsDeterminate = false;
    }

    public static ApplyFingerprintComparison Indeterminate(int partitionId, string? leader = null) => new(partitionId, leader);
}

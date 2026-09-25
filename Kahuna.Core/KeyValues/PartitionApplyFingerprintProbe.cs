using Kommander;
using Kommander.System;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Compares one partition's apply fingerprint across its replicas. The committed-head ledger and the
/// prepared-intent slice are pure functions of the log, so two replicas at the same applied kv log id
/// must hold the same number of committed heads and the same number of live prepared intents; a
/// replica that does not is one whose apply stream diverged — a snapshot install that marked entries
/// applied without delivering them, an import that rewound the application below its cursor, a bundled
/// commit it alone rejected. Such a replica serves reads that miss acknowledged writes once it leads,
/// holds settled transactions' intents as read-only keys forever, and a split that copies from it moves
/// the loss into a new partition. The comparison runs at three moments: on every replica when the
/// partition's leader changes (to make the divergence visible and contain it), before a split's bulk
/// copy (to refuse copying from an incomplete source), and when recovery meets a record-less intent
/// (through the intent-presence cross-check, which shares the transport).
/// </summary>
/// <remarks>
/// A peer that does not answer inside <see cref="PeerTimeoutMs"/> counts as unknown, never as
/// divergent. A peer at a different applied log id is simply behind or ahead and is not compared: only
/// equal ids make the counts comparable. A comparison that could not compare every peer is reported as
/// inconclusive (<see cref="ApplyFingerprintComparison.IsConclusive"/>) so a caller can retry rather
/// than read "no divergence among the peers that happened to line up" as a pass.
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
            ? await ReadLocalAsync(partitionId, cancellationToken).ConfigureAwait(false)
            : await FetchAsync(leader, partitionId, cancellationToken).ConfigureAwait(false);

        if (leaderFingerprint is null)
            return ApplyFingerprintComparison.Indeterminate(partitionId, leader);

        List<string> peers = PeersOf(partitionId, leader);
        if (peers.Count == 0)
            return new ApplyFingerprintComparison(partitionId, leader, leaderFingerprint.Value, [], 0, 0, 0);

        Task<KeyValueApplyFingerprint?>[] fetches = new Task<KeyValueApplyFingerprint?>[peers.Count];
        for (int i = 0; i < peers.Count; i++)
        {
            string peer = peers[i];
            fetches[i] = peer == local
                ? ReadLocalAsync(partitionId, cancellationToken)
                : FetchAsync(peer, partitionId, cancellationToken);
        }

        await Task.WhenAll(fetches).ConfigureAwait(false);

        List<ApplyDivergentPeer> divergent = [];
        int answered = 0;
        int compared = 0;

        for (int i = 0; i < peers.Count; i++)
        {
            KeyValueApplyFingerprint? peerFingerprint = fetches[i].Result;
            if (peerFingerprint is null)
                continue;

            answered++;

            if (peerFingerprint.Value.AppliedLogId != leaderFingerprint.Value.AppliedLogId)
                continue;

            compared++;

            if (peerFingerprint.Value.CommittedHeads != leaderFingerprint.Value.CommittedHeads
                || peerFingerprint.Value.LiveIntents != leaderFingerprint.Value.LiveIntents)
                divergent.Add(new ApplyDivergentPeer(peers[i], peerFingerprint.Value, leaderFingerprint.Value));
        }

        return new ApplyFingerprintComparison(partitionId, leader, leaderFingerprint.Value, divergent, peers.Count, answered, compared);
    }

    /// <summary>
    /// Every replica of the partition except <paramref name="leader"/>: the committed replica set when
    /// the partition is placed, else every cluster node (legacy full replication hosts a partition
    /// everywhere).
    /// </summary>
    internal List<string> PeersOf(int partitionId, string leader)
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

    /// <summary>First pause between attempts at a replica whose fingerprint read keeps racing its apply stream; doubles per attempt.</summary>
    private const int TransientRetryDelayMs = 10;

    /// <summary>
    /// Cap on the pause between attempts. The gRPC transport also answers MustRetry for a retryable transport
    /// failure, so the backoff keeps a dead peer from being hammered for the whole bound.
    /// </summary>
    private const int TransientRetryMaxDelayMs = 200;

    /// <summary>
    /// This node's fingerprint, retried inside <see cref="PeerTimeoutMs"/> while the partition is hosted here
    /// and the consistent read keeps racing applies. That race is transient by construction (applies are
    /// serialized and short), so giving up on the first miss would report a healthy replica as unknown and
    /// make the whole comparison indeterminate or inconclusive under an ordinary apply stream.
    /// </summary>
    private async Task<KeyValueApplyFingerprint?> ReadLocalAsync(int partitionId, CancellationToken cancellationToken)
    {
        long deadline = Environment.TickCount64 + PeerTimeoutMs;
        int delay = TransientRetryDelayMs;

        while (true)
        {
            KeyValueApplyFingerprint? fingerprint = readLocal(partitionId);
            if (fingerprint is not null)
                return fingerprint;

            if (!raft.HostsPartition(partitionId) || Environment.TickCount64 >= deadline || cancellationToken.IsCancellationRequested)
                return null;

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            delay = Math.Min(delay * 2, TransientRetryMaxDelayMs);
        }
    }

    /// <summary>
    /// A peer's fingerprint, retried inside <see cref="PeerTimeoutMs"/> while the peer answers MustRetry
    /// (it hosts the partition but its consistent read raced an apply). Any other non-answer — not hosted,
    /// unreachable, slow past the bound — is unknown, never divergent.
    /// </summary>
    private async Task<KeyValueApplyFingerprint?> FetchAsync(string node, int partitionId, CancellationToken cancellationToken)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(PeerTimeoutMs);

        try
        {
            int delay = TransientRetryDelayMs;

            while (true)
            {
                (KeyValueResponseType type, KeyValueApplyFingerprint fingerprint) =
                    await interNodeCommunication.GetPartitionApplyFingerprint(node, partitionId, timeout.Token).ConfigureAwait(false);

                if (type == KeyValueResponseType.Get)
                    return fingerprint;

                if (type != KeyValueResponseType.MustRetry)
                    return null;

                await Task.Delay(delay, timeout.Token).ConfigureAwait(false);
                delay = Math.Min(delay * 2, TransientRetryMaxDelayMs);
            }
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // An unreachable or slow peer is unknown, not divergent. The caller's own token still
            // propagates: only the per-peer bound and transport failures are absorbed here.
            return null;
        }
    }
}

/// <summary>
/// One peer whose fingerprint differs from the leader's at the same applied kv log id, with the side
/// the evidence points at. The action a divergence demands depends on which replica is the incomplete
/// one, so the comparison names it instead of only reporting "differs".
/// </summary>
internal readonly record struct ApplyDivergentPeer(string Peer, KeyValueApplyFingerprint Fingerprint, KeyValueApplyFingerprint Leader)
{
    public bool HeadsDiffer => Fingerprint.CommittedHeads != Leader.CommittedHeads;

    public bool IntentsDiffer => Fingerprint.LiveIntents != Leader.LiveIntents;

    /// <summary>
    /// The replica this divergence indicts. Committed heads only ever grow along the log, so the replica
    /// holding fewer at the same applied id missed applies. Live-intent counts alone cannot name a side: a
    /// replica that missed settlements holds more (the record-less hold shape), a replica whose prepares
    /// were erased holds fewer (the import-below-cursor shape), and either way the other replica is the
    /// complete one. Acting on the intent count would gate a healthy replica for the other's loss, so an
    /// intent-only divergence is reported and left to the evidence that does name a side: the heads, which
    /// diverge as soon as the affected commits settle, and recovery's peer cross-check of a held intent.
    /// </summary>
    public ApplyDivergenceSide Side => !HeadsDiffer
        ? ApplyDivergenceSide.Undetermined
        : Fingerprint.CommittedHeads < Leader.CommittedHeads ? ApplyDivergenceSide.PeerBehind : ApplyDivergenceSide.LeaderBehind;

    public bool PeerIsBehind => Side == ApplyDivergenceSide.PeerBehind;

    public bool LeaderIsBehind => Side == ApplyDivergenceSide.LeaderBehind;

    /// <summary>What the evidence says about which replica is incomplete, for the report line.</summary>
    public string DescribeSide(string leader) => Side switch
    {
        ApplyDivergenceSide.PeerBehind => $"replica {Peer} is the incomplete one",
        ApplyDivergenceSide.LeaderBehind => $"leader {leader} is the incomplete one",
        _ => "only the live intents differ, which does not name the incomplete replica (a settlement missed on one side or a prepare lost on the other); the heads will, once the affected commits settle, as will recovery's peer cross-check of a held intent"
    };
}

/// <summary>Which replica of a divergent pair holds the incomplete apply projection.</summary>
internal enum ApplyDivergenceSide
{
    /// <summary>Only the live-intent counts differ: real divergence, but the counts do not say on which side.</summary>
    Undetermined,

    /// <summary>The peer missed applies the leader made.</summary>
    PeerBehind,

    /// <summary>The leader missed applies the peer made: it must not keep serving the partition.</summary>
    LeaderBehind
}

/// <summary>Result of <see cref="PartitionApplyFingerprintProbe.CompareWithReplicasAsync"/>.</summary>
internal sealed class ApplyFingerprintComparison
{
    public int PartitionId { get; }

    /// <summary>The leader whose fingerprint was the reference, or null when none resolved.</summary>
    public string? Leader { get; }

    public KeyValueApplyFingerprint LeaderFingerprint { get; }

    /// <summary>Peers at the leader's applied log id whose committed-head or live-intent count differs from the leader's.</summary>
    public IReadOnlyList<ApplyDivergentPeer> Divergent { get; }

    public int PeersAsked { get; }

    public int PeersAnswered { get; }

    /// <summary>Peers that answered at the leader's applied log id, i.e. whose counts were actually compared.</summary>
    public int PeersCompared { get; }

    /// <summary>False when no leader resolved or the leader's own fingerprint could not be read.</summary>
    public bool IsDeterminate { get; }

    /// <summary>
    /// True only when every peer was compared: a determinate comparison that skipped a peer (unreachable,
    /// or at another applied id) can still name a divergence among the peers it did compare, but its
    /// "no divergence" is not a pass — the skipped peer may be the diverged one.
    /// </summary>
    public bool IsConclusive => IsDeterminate && PeersCompared == PeersAsked;

    public bool HasDivergence => Divergent.Count > 0;

    public ApplyFingerprintComparison(
        int partitionId,
        string leader,
        KeyValueApplyFingerprint leaderFingerprint,
        IReadOnlyList<ApplyDivergentPeer> divergent,
        int peersAsked,
        int peersAnswered,
        int peersCompared)
    {
        PartitionId = partitionId;
        Leader = leader;
        LeaderFingerprint = leaderFingerprint;
        Divergent = divergent;
        PeersAsked = peersAsked;
        PeersAnswered = peersAnswered;
        PeersCompared = peersCompared;
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

    /// <summary>Whether the evidence indicts <paramref name="endpoint"/>: the leader when any peer holds more than it, a peer when it holds less than the leader.</summary>
    public bool IsBehind(string endpoint)
    {
        foreach (ApplyDivergentPeer peer in Divergent)
        {
            if (peer.LeaderIsBehind && endpoint == Leader)
                return true;

            if (peer.PeerIsBehind && endpoint == peer.Peer)
                return true;
        }

        return false;
    }

    /// <summary>
    /// The peer holding the fullest projection among those the leader is behind: the most committed heads,
    /// then the fewest live intents. Null when the leader is behind no peer.
    /// </summary>
    public string? FullerPeerThanLeader()
    {
        string? fuller = null;
        KeyValueApplyFingerprint best = default;

        foreach (ApplyDivergentPeer peer in Divergent)
        {
            if (!peer.LeaderIsBehind)
                continue;

            if (fuller is null
                || peer.Fingerprint.CommittedHeads > best.CommittedHeads
                || (peer.Fingerprint.CommittedHeads == best.CommittedHeads && peer.Fingerprint.LiveIntents < best.LiveIntents))
            {
                fuller = peer.Peer;
                best = peer.Fingerprint;
            }
        }

        return fuller;
    }
}

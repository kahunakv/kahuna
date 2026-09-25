using Kommander;
using Kommander.Time;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Data;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>What a partition's other replicas say about a prepared intent this node holds record-less past the retention horizon.</summary>
internal enum RecordlessIntentVerdict
{
    /// <summary>Not enough replicas answered at or past this node's applied kv log id to decide; hold as before.</summary>
    Unknown,

    /// <summary>A replica still holds the intent: its settlement has not landed anywhere; hold as before.</summary>
    StillHeld,

    /// <summary>
    /// A majority of the replica set, each at or past this node's applied kv log id, no longer holds it: the
    /// settlement is in the log below what this node claims to have applied, and this node missed it.
    /// </summary>
    Stale
}

/// <summary>
/// The cross-check's answer, with the evidence for the log line and the replica containment prefers as
/// the successor (the first peer that proved it settled the intent).
/// </summary>
internal readonly record struct RecordlessIntentPeerVerdict(
    RecordlessIntentVerdict Verdict,
    int NotHolding,
    int Consulted,
    string Peers,
    long LocalAppliedLogId,
    string? FullerPeer)
{
    public static RecordlessIntentPeerVerdict UnknownVerdict => new(RecordlessIntentVerdict.Unknown, 0, 0, "", 0, null);
}

/// <summary>
/// Asks a partition's other replicas whether they still hold a prepared intent this node is about to hold
/// forever. Recovery holds a record-less intent past the retention horizon because record absence cannot be
/// told apart from a reclaimed commit — but the intent stores are pure functions of the log, so a replica
/// at or past this node's applied kv log id that no longer holds the intent proves its settlement is in a
/// range this node has marked applied. That is not an outcome to presume locally (the settled value lives
/// on the peers, not here); it is replica divergence, and the answer routes to containment.
/// </summary>
/// <remarks>
/// Stale needs a majority of the replica set counted with this node as a holder: with three replicas both
/// peers must answer "not held" at or past the local applied id. Two replicas disagreeing cannot name the
/// wrong one, so a two-replica partition never reaches Stale. A peer that does not answer, or answers from
/// behind this node's applied id, is unknown, never evidence.
/// </remarks>
internal sealed class RecordlessIntentPeerCheck
{
    private const int PeerTimeoutMs = PartitionApplyFingerprintProbe.PeerTimeoutMs;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly PartitionApplyFingerprintProbe probe;

    private readonly Func<int, KeyValueApplyFingerprint?> readLocal;

    public RecordlessIntentPeerCheck(
        IRaft raft,
        IInterNodeCommunication interNodeCommunication,
        PartitionApplyFingerprintProbe probe,
        Func<int, KeyValueApplyFingerprint?> readLocal)
    {
        this.raft = raft;
        this.interNodeCommunication = interNodeCommunication;
        this.probe = probe;
        this.readLocal = readLocal;
    }

    public async Task<RecordlessIntentPeerVerdict> CheckAsync(int partitionId, PreparedIntent intent, CancellationToken cancellationToken)
    {
        KeyValueApplyFingerprint? local = readLocal(partitionId);
        if (local is null)
            return RecordlessIntentPeerVerdict.UnknownVerdict;

        string localEndpoint = raft.GetLocalEndpoint();
        List<string> peers = probe.PeersOf(partitionId, localEndpoint);
        if (peers.Count == 0)
            return RecordlessIntentPeerVerdict.UnknownVerdict;

        int replicaCount = peers.Count + 1;
        int needed = replicaCount / 2 + 1;

        Task<(KeyValueResponseType Type, bool Held, long AppliedLogId)>[] answers = new Task<(KeyValueResponseType, bool, long)>[peers.Count];
        for (int i = 0; i < peers.Count; i++)
            answers[i] = AskAsync(peers[i], partitionId, intent, cancellationToken);

        await Task.WhenAll(answers).ConfigureAwait(false);

        int notHolding = 0;
        int held = 0;
        int consulted = 0;
        string? fuller = null;
        List<string> names = new(peers.Count);

        for (int i = 0; i < peers.Count; i++)
        {
            (KeyValueResponseType type, bool peerHolds, long appliedLogId) = answers[i].Result;
            if (type != KeyValueResponseType.Get)
                continue;

            consulted++;

            if (peerHolds)
            {
                held++;
                names.Add($"{peers[i]}:held@{appliedLogId}");
                continue;
            }

            if (appliedLogId < local.Value.AppliedLogId)
            {
                // Behind this node: its "not held" may simply mean "not prepared yet here". No evidence.
                names.Add($"{peers[i]}:behind@{appliedLogId}");
                continue;
            }

            notHolding++;
            fuller ??= peers[i];
            names.Add($"{peers[i]}:settled@{appliedLogId}");
        }

        RecordlessIntentVerdict verdict = notHolding >= needed
            ? RecordlessIntentVerdict.Stale
            : held > 0 ? RecordlessIntentVerdict.StillHeld : RecordlessIntentVerdict.Unknown;

        return new RecordlessIntentPeerVerdict(verdict, notHolding, consulted, string.Join(", ", names), local.Value.AppliedLogId, fuller);
    }

    private async Task<(KeyValueResponseType, bool, long)> AskAsync(string node, int partitionId, PreparedIntent intent, CancellationToken cancellationToken)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(PeerTimeoutMs);

        try
        {
            return await interNodeCommunication.GetPreparedIntentPresence(
                node, partitionId, intent.TransactionId, intent.Epoch, intent.Key, timeout.Token).ConfigureAwait(false);
        }
        catch (Exception) when (!cancellationToken.IsCancellationRequested)
        {
            // An unreachable or slow peer is unknown, never evidence either way.
            return (KeyValueResponseType.MustRetry, false, 0);
        }
    }
}

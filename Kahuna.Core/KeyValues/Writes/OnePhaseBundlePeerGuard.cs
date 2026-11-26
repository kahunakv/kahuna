using System.Collections.Concurrent;

using Kommander;
using Kommander.System;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Replication;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// Decides, for the leader of a record anchor partition, whether a one-phase commit goes to the log as one
/// <see cref="ReplicationTypes.OnePhaseBundle"/> entry or as the three entries of the record init, the prepare and the
/// decision. A node of a build that does not know the bundle type skips the entry with no error, so the leader proposes
/// it only while each replica of the partition confirmed that it applies the type.
///
/// <para><b>State.</b> For each partition, one verdict: confirmed or not, for a term and a stamp of the replica set.
/// The commit path reads the verdict with no wait and no call. A verdict of another term or of an older stamp is not a
/// confirmation, so the commit proposes three entries and the leader asks again.</para>
///
/// <para><b>When the leader asks.</b> When it takes the leadership of the partition, when the replicas or the members
/// change, at a fixed interval while one replica did not confirm, and again after a longer interval while all
/// confirmed, so a replica that started again on another build is asked again. Each ask goes to every voter and
/// learner of the partition except this node. A replica that does not implement the question, does not answer, or
/// names no bundle type in its answer is not confirmed. A partition with no other replica, and a group whose replicas
/// all run in this process, are confirmed at once.</para>
///
/// <para><b>Limits.</b> The guard controls what the leader proposes from the moment of its verdict. It does not remove
/// a bundle entry that is in a log, and it does not see a node of an older build that joins later or that starts again
/// on an older build between two asks. The rule that every node runs a build that applies the type before the option
/// is on stays.</para>
/// </summary>
internal sealed class OnePhaseBundlePeerGuard : IDisposable
{
    /// <summary>The replicated log types that this build applies, as a leader asks a replica for them.</summary>
    internal static readonly IReadOnlyList<string> AppliedLogTypes =
    [
        ReplicationTypes.Locks,
        ReplicationTypes.KeyValues,
        ReplicationTypes.RangeMap,
        ReplicationTypes.SnapshotFloor,
        ReplicationTypes.CoordinatorDecision,
        ReplicationTypes.CompletionReceipt,
        ReplicationTypes.TransactionRecord,
        ReplicationTypes.PreparedIntent,
        ReplicationTypes.ProgressTick,
        ReplicationTypes.HistoryHold,
        ReplicationTypes.OnePhaseBundle
    ];

    /// <summary>How long after an ask that did not confirm the leader asks again.</summary>
    internal static readonly TimeSpan DefaultRetryInterval = TimeSpan.FromSeconds(1);

    /// <summary>How long a confirmation holds before the leader asks again in the background.</summary>
    internal static readonly TimeSpan DefaultRefreshInterval = TimeSpan.FromSeconds(30);

    /// <summary>The bound on one ask of one replica.</summary>
    private static readonly TimeSpan AskTimeout = TimeSpan.FromSeconds(2);

    /// <summary>The most (partition, term, replica) refusals that are logged, so a long refusal does not grow the set.</summary>
    private const int MaxLoggedRefusals = 4_096;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication transport;

    private readonly ILogger<IKahuna> logger;

    private readonly bool enabled;

    // Every replica runs in this process, on this build: its phantom witnesses apply no entries and are not asked.
    private readonly bool singleProcessGroup;

    private readonly long retryMs;

    private readonly long refreshMs;

    private readonly CancellationTokenSource stopping = new();

    private readonly ConcurrentDictionary<int, PartitionState> partitions = new();

    private readonly ConcurrentDictionary<(int PartitionId, long Term, string Replica), byte> loggedRefusals = new();

    // The size of loggedRefusals, kept beside it: the count of a concurrent dictionary takes every lock.
    private int loggedRefusalCount;

    // Moves on each change of the partition map or of the members, so a verdict of an older replica set is not used.
    private long replicaStamp;

    // Instance-owned counts, beside the process-wide counter, for a test that reads one node.
    private long fallbacks;
    private long asks;

    /// <summary>One verdict. Replaced as a whole, so a reader never sees the fields of two verdicts.</summary>
    private sealed record Verdict(long Term, long Stamp, bool Confirmed, long CheckedAtMs);

    private sealed class PartitionState
    {
        public Verdict? Verdict;

        // 1 while an ask runs.
        public int Asking;
    }

    internal OnePhaseBundlePeerGuard(
        IRaft raft,
        IInterNodeCommunication transport,
        ILogger<IKahuna> logger,
        bool enabled,
        bool singleProcessGroup = false,
        TimeSpan? retryInterval = null,
        TimeSpan? refreshInterval = null)
    {
        this.raft = raft;
        this.transport = transport;
        this.logger = logger;
        this.enabled = enabled;
        this.singleProcessGroup = singleProcessGroup;
        retryMs = (long)(retryInterval ?? DefaultRetryInterval).TotalMilliseconds;
        refreshMs = (long)(refreshInterval ?? DefaultRefreshInterval).TotalMilliseconds;

        if (enabled)
        {
            raft.OnPartitionMapChanged += OnPartitionMapChanged;
            raft.OnMembershipChanged += OnMembershipChanged;
        }
    }

    /// <summary>The one-phase commits that this node proposed as three entries because a replica did not confirm.</summary>
    internal long Fallbacks => Interlocked.Read(ref fallbacks);

    /// <summary>The asks of a replica that this node made.</summary>
    internal long Asks => Interlocked.Read(ref asks);

    /// <summary>
    /// Whether the one-phase commit that this node proposes now on <paramref name="partitionId"/>, under
    /// <paramref name="term"/>, goes to the log as one bundle entry. Reads the verdict only. A missing or old verdict
    /// starts an ask in the background and answers false.
    /// </summary>
    internal bool ShouldProposeBundle(int partitionId, long term)
    {
        if (!enabled)
            return false;

        if (singleProcessGroup)
            return true;

        PartitionState state = partitions.GetOrAdd(partitionId, static _ => new PartitionState());
        Verdict? verdict = Volatile.Read(ref state.Verdict);
        long stamp = Volatile.Read(ref replicaStamp);
        long now = Environment.TickCount64;

        if (verdict is { Confirmed: true } && verdict.Term == term && verdict.Stamp == stamp)
        {
            if (now - verdict.CheckedAtMs >= refreshMs)
                StartAsk(partitionId, state, term);

            return true;
        }

        if (verdict is null || verdict.Term != term || verdict.Stamp != stamp)
        {
            // A partition with no other replica has nobody to ask. While an ask runs, the replicas are not read again.
            if (Volatile.Read(ref state.Asking) == 0 && !ReplicasOf(partitionId).Any())
            {
                Volatile.Write(ref state.Verdict, new Verdict(term, stamp, true, now));
                return true;
            }

            StartAsk(partitionId, state, term);
        }
        else if (now - verdict.CheckedAtMs >= retryMs)
            StartAsk(partitionId, state, term);

        Interlocked.Increment(ref fallbacks);
        DurableTransactionMetrics.OnePhaseBundleGuardFallback();
        return false;
    }

    /// <summary>The leadership of <paramref name="partitionId"/> moved to <paramref name="leader"/>. When it is this
    /// node, the leader asks its replicas at once, so its first commits wait for no ask.</summary>
    internal void OnLeaderChanged(int partitionId, string leader)
    {
        if (!enabled || singleProcessGroup || !string.Equals(leader, raft.GetLocalEndpoint(), StringComparison.Ordinal))
            return;

        PartitionState state = partitions.GetOrAdd(partitionId, static _ => new PartitionState());
        StartAsk(partitionId, state, raft.GetPartitionTerm(partitionId));
    }

    private void OnPartitionMapChanged(IReadOnlyList<RaftPartitionRange> map) => Interlocked.Increment(ref replicaStamp);

    private void OnMembershipChanged(ClusterMembership membership) => Interlocked.Increment(ref replicaStamp);

    private void StartAsk(int partitionId, PartitionState state, long term)
    {
        if (stopping.IsCancellationRequested || Interlocked.CompareExchange(ref state.Asking, 1, 0) != 0)
            return;

        // The stamp before the replica set is read: a change during the ask leaves the verdict old.
        long stamp = Volatile.Read(ref replicaStamp);
        _ = AskAsync(partitionId, state, term, stamp);
    }

    private async Task AskAsync(int partitionId, PartitionState state, long term, long stamp)
    {
        bool confirmed = true;

        try
        {
            foreach (string replica in ReplicasOf(partitionId))
            {
                Interlocked.Increment(ref asks);

                IReadOnlyList<string>? types;
                using (CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(stopping.Token))
                {
                    timeout.CancelAfter(AskTimeout);
                    types = await transport.GetAppliedLogTypes(replica, timeout.Token).ConfigureAwait(false);
                }

                if (types is not null && types.Contains(ReplicationTypes.OnePhaseBundle))
                    continue;

                confirmed = false;
                LogRefusal(partitionId, term, replica, types is null);
            }
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            confirmed = false;

            if (!stopping.IsCancellationRequested)
                logger.LogWarning(ex, "Could not ask the replicas of partition {PartitionId} in term {Term} which log types they apply; its one-phase commits use three entries", partitionId, term);
        }
        finally
        {
            Volatile.Write(ref state.Verdict, new Verdict(term, stamp, confirmed, Environment.TickCount64));
            Volatile.Write(ref state.Asking, 0);
        }
    }

    /// <summary>The endpoints of the voters and learners of <paramref name="partitionId"/> other than this node. A
    /// partition with no replica set of its own is on every node, so its replicas are the nodes this node knows and the
    /// members of the roster that apply entries.</summary>
    private IEnumerable<string> ReplicasOf(int partitionId)
    {
        string self = raft.GetLocalEndpoint();
        HashSet<string> replicas = new(StringComparer.Ordinal);

        IReadOnlyList<RaftReplica> placed = raft.GetPartitionReplicas(partitionId);
        if (placed.Count > 0)
        {
            foreach (RaftReplica replica in placed)
                replicas.Add(replica.Endpoint);
        }
        else
        {
            foreach (RaftNode node in raft.GetNodes())
                replicas.Add(node.Endpoint);

            foreach (ClusterMember member in raft.GetMembership().Members)
                replicas.Add(member.Endpoint);
        }

        replicas.Remove(self);
        replicas.Remove(string.Empty);
        return replicas;
    }

    private void LogRefusal(int partitionId, long term, string replica, bool unanswered)
    {
        if (Volatile.Read(ref loggedRefusalCount) >= MaxLoggedRefusals || !loggedRefusals.TryAdd((partitionId, term, replica), 0))
            return;

        Interlocked.Increment(ref loggedRefusalCount);

        if (unanswered)
            logger.LogWarning(
                "Replica {Replica} of partition {PartitionId} did not say in term {Term} which log types it applies; the one-phase commits of the partition use three entries until it confirms the bundle entry",
                replica, partitionId, term);
        else
            logger.LogWarning(
                "Replica {Replica} of partition {PartitionId} does not apply the one-phase bundle entry in term {Term}; the one-phase commits of the partition use three entries until it does",
                replica, partitionId, term);
    }

    public void Dispose()
    {
        if (enabled)
        {
            raft.OnPartitionMapChanged -= OnPartitionMapChanged;
            raft.OnMembershipChanged -= OnMembershipChanged;
        }

        stopping.Cancel();
        stopping.Dispose();
    }
}

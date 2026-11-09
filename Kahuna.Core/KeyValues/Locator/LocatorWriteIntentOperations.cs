
using Kommander;
using Kommander.Time;
using Kommander.Diagnostics;

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

using Kahuna.Server.Routing;
using Kahuna.Shared.Routing;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Write-intent checks for single and many keys, confirmed or unconfirmed.
/// </summary>
internal sealed class LocatorWriteIntentOperations
{
    private readonly KeyValuesManager manager;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly ILogger<IKahuna> logger;

    private readonly LocatorRouting routing;

    internal LocatorWriteIntentOperations(LocatorRouting routing)
    {
        this.manager = routing.Manager;
        this.raft = routing.Raft;
        this.interNodeCommunication = routing.InterNodeCommunication;
        this.logger = routing.Logger;
        this.routing = routing;
    }

    /// <summary>
    /// Locates the leader node for the given key and checks whether a live write intent from another
    /// transaction exists. Used at commit time by optimistic transactions as a write-skew guard.
    /// Returns Aborted when a conflicting write intent is found; DoesNotExist otherwise.
    /// </summary>
    public async Task<KeyValueResponseType> LocateAndTryCheckWriteIntent(
        HLCTimestamp transactionId,
        string key,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(key))
            return KeyValueResponseType.InvalidInput;

        int partitionId = routing.RouteKey(key);

        if (!raft.Joined)
            return KeyValueResponseType.MustRetry;

        if (await routing.ConfirmLeadershipForRead(partitionId, cancellationToken))
            return await manager.TryCheckWriteIntentValue(transactionId, key, durability);

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null)
            return KeyValueResponseType.MustRetry;
        if (leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;

        logger.LogCheckWriteIntentRedirect(key, partitionId, leader);

        return await interNodeCommunication.TryCheckWriteIntentValue(leader, transactionId, key, durability, cancellationToken);
    }

    /// <summary>
    /// Probes several keys for concurrent write intents, grouping them by the node that leads each key's
    /// partition so the whole set costs one call per owning node instead of one per key. Used at commit time by
    /// optimistic transactions as a write-skew guard over the read set.
    ///
    /// Returns exactly one result per requested key, in no particular order — callers correlate by key and
    /// durability. The per-key coverage is part of the contract: a caller must never have to treat a key it asked
    /// about but did not hear back on as "no conflict", so every rejection path fills in the affected keys rather
    /// than dropping them. A remote group that faults propagates its exception, exactly as a single-key probe
    /// does, so a failed probe is never silently read as a pass.
    ///
    /// Only the routing is grouped. Keys the local node owns are still probed one actor request at a time
    /// (see <see cref="KeyValuesManager.TryCheckManyWriteIntentValues"/>), which measurement showed is not the
    /// cost worth removing — the round trip per remote key is.
    /// </summary>
    public async Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> LocateAndTryCheckManyWriteIntents(
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys,
        CancellationToken cancellationToken
    )
    {
        if (keys.Count == 0)
            return [];

        if (!raft.Joined)
            return BuildManyWriteIntentRejection(keys, KeyValueResponseType.MustRetry);

        string localNode = raft.GetLocalEndpoint();
        Dictionary<string, List<KeyValueConflictProbe>> probePlan = [];
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses = new(keys.Count);

        Dictionary<int, string> leaderByPartition = [];

        foreach (KeyValueConflictProbe item in keys)
        {
            // A malformed key is reported against that key alone; the rest of the set is still probed, so one
            // bad entry cannot quietly cancel the write-skew guard for every other read dependency.
            if (string.IsNullOrEmpty(item.Key))
            {
                responses.Add((KeyValueResponseType.InvalidInput, item.Key, item.Durability));
                continue;
            }

            int partitionId = routing.RouteKey(item.Key);
            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);
            if (leader is null)
                return BuildManyWriteIntentRejection(keys, KeyValueResponseType.MustRetry);

            if (probePlan.TryGetValue(leader, out List<KeyValueConflictProbe>? list))
                list.Add(item);
            else
                probePlan[leader] = [item];
        }

        Lock lockSync = new();
        List<Task> tasks = new(probePlan.Count);

        foreach ((string leader, List<KeyValueConflictProbe> xkeys) in probePlan)
            tasks.Add(TryCheckManyWriteIntentsOnNode(transactionId, leader, localNode, xkeys, lockSync, responses, cancellationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    /// <summary>
    /// Serves a batched conflict probe for keys grouped to this node, after confirming this node's leadership
    /// (read-index) of every partition the keys route to. This is the local leg of
    /// <see cref="LocateAndTryCheckManyWriteIntents"/> and the entry for a probe that arrived over the wire
    /// already grouped: the confirmation has to run on the node that answers, because a follower that has not
    /// yet heard of a newer term would otherwise vouch for staged state of a leadership that ended. The keys of
    /// a partition that does not confirm here, and leader-term probes this node cannot vouch for, are asked of
    /// the leader this node's own Raft names, once, within the forward budget (see
    /// <see cref="ForwardUnansweredToLeaders"/>); a key with no such leader answers
    /// <see cref="KeyValueResponseType.MustRetry"/>.
    ///
    /// <para>A key whose own staged intent the probe found held (<see cref="KeyValueConflictChecks.OwnStagedIntent"/>)
    /// reports the leadership term it was confirmed under into the open <see cref="LockGrantScope"/>, as a lock
    /// grant does. The term is read <b>before</b> the confirmation, for the reason given on
    /// <see cref="LockGrantTermBeforeConfirmation"/>: a proposal later fenced to that term can only be appended
    /// by this very leadership, uninterrupted since the probe, with the intent still in its memory.</para>
    /// </summary>
    internal async Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> TryCheckManyWriteIntentsConfirmedLocally(
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys,
        CancellationToken cancellationToken
    )
    {
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses = new(keys.Count);

        if (keys.Count == 0)
            return responses;

        if (!raft.Joined)
            return BuildManyWriteIntentRejection(keys, KeyValueResponseType.MustRetry);

        bool captureTerms = LockGrantScope.Current is not null;

        // The term of every partition involved, read before any confirmation; null once the partition failed to
        // confirm. One entry per partition, so a probe over many keys of one partition confirms it once.
        Dictionary<int, long?> termByPartition = [];
        List<int> partitions = [];
        int[] partitionOf = new int[keys.Count];

        for (int i = 0; i < keys.Count; i++)
        {
            if (string.IsNullOrEmpty(keys[i].Key))
            {
                partitionOf[i] = -1;
                continue;
            }

            int partitionId = routing.RouteKey(keys[i].Key);
            partitionOf[i] = partitionId;

            // A leader-term probe confirms the leadership of the node that answers it by itself
            // (CheckLeaderTerm), and answers a term the partition is already past as proof on any node; a
            // confirmation here would turn that proof into a retry on a follower. It never reports a term.
            if ((keys[i].Checks & KeyValueConflictChecks.LeaderTerm) != 0)
                continue;

            if (termByPartition.TryAdd(partitionId, captureTerms ? raft.GetPartitionTerm(partitionId) : 0))
                partitions.Add(partitionId);
        }

        foreach (int partitionId in partitions)
        {
            if (!await routing.ConfirmLeadershipForRead(partitionId, cancellationToken))
                termByPartition[partitionId] = null;
        }

        List<KeyValueConflictProbe>? confirmedProbes = null;
        int[]? confirmedPartitions = null;

        // Keys this node could not answer for and that its own Raft may place with another leader: (probe,
        // partition, whether the partition failed this node's confirmation).
        List<(KeyValueConflictProbe Probe, int PartitionId, bool Unconfirmed)>? unanswered = null;

        for (int i = 0; i < keys.Count; i++)
        {
            KeyValueConflictProbe item = keys[i];

            if (partitionOf[i] < 0)
            {
                responses.Add((KeyValueResponseType.InvalidInput, item.Key, item.Durability));
                continue;
            }

            if ((item.Checks & KeyValueConflictChecks.LeaderTerm) == 0 && termByPartition[partitionOf[i]] is null)
            {
                (unanswered ??= []).Add((item, partitionOf[i], true));
                continue;
            }

            confirmedProbes ??= new(keys.Count);
            confirmedPartitions ??= new int[keys.Count];
            confirmedPartitions[confirmedProbes.Count] = partitionOf[i];
            confirmedProbes.Add(item);
        }

        if (confirmedProbes is not null)
        {
            List<(KeyValueResponseType type, string key, KeyValueDurability durability)> answers =
                await manager.TryCheckManyWriteIntentValues(transactionId, confirmedProbes);

            // The answers come back one per probe, in probe order. A held own staged intent is the answer that
            // passed every check the probe asked for; any refusal (lost intent, foreign range lock, a gated key)
            // reports no term, because nothing was proven about the key.
            for (int i = 0; i < answers.Count; i++)
            {
                (KeyValueResponseType type, string key, KeyValueDurability durability) answer = answers[i];

                // A leader-term probe a follower cannot vouch for (its term equals the grant's, and only the
                // leader can confirm that) is asked of the leader instead, like a key that failed confirmation.
                if (answer.type == KeyValueResponseType.MustRetry
                    && i < confirmedProbes.Count
                    && (confirmedProbes[i].Checks & KeyValueConflictChecks.LeaderTerm) != 0
                    && string.Equals(answer.key, confirmedProbes[i].Key, StringComparison.Ordinal))
                {
                    (unanswered ??= []).Add((confirmedProbes[i], confirmedPartitions![i], false));
                    continue;
                }

                responses.Add(answer);

                if (!captureTerms || i >= confirmedProbes.Count)
                    continue;

                KeyValueConflictProbe probe = confirmedProbes[i];

                if ((probe.Checks & KeyValueConflictChecks.OwnStagedIntent) == 0
                    || answer.type != KeyValueResponseType.DoesNotExist
                    || !string.Equals(answer.key, probe.Key, StringComparison.Ordinal))
                    continue;

                int partitionId = confirmedPartitions![i];
                if (termByPartition.TryGetValue(partitionId, out long? confirmedTerm) && confirmedTerm is { } term && term > 0)
                    LockGrantScope.Record(partitionId, term, probe.Key);
            }
        }

        if (unanswered is not null)
            await ForwardUnansweredToLeaders(transactionId, unanswered, responses, cancellationToken);

        return responses;
    }

    /// <summary>
    /// The second hop of a grouped probe that reached a node which cannot answer part of it. A sender that does
    /// not host a partition targets a guess, either the gossiped leader hint or the first voter of the committed
    /// map, and a guess can name a follower. Every retry of the sender then lands on that same follower, which can
    /// neither confirm a leadership it does not hold nor vouch for a leader term it only follows. The commit's
    /// probe would answer MustRetry until the coordinator gives up.
    ///
    /// <para>This node hosts the partition, so its own Raft names the leader with better knowledge than the
    /// sender's guess, which is the one extra hop <see cref="ForwardedRequestScope.MaxForwardHops"/> budgets for.
    /// The keys go there as one grouped probe per leader. That leader confirms its own leadership. The terms
    /// it confirmed the caller's staged intents under, and their commit floor, come back on its response and are
    /// replayed into the capture open here, so they reach the original sender as if it had asked the leader
    /// directly. A key whose leader this node cannot name past itself, or past the hop budget, still answers
    /// <see cref="KeyValueResponseType.MustRetry"/>.</para>
    /// </summary>
    private async Task ForwardUnansweredToLeaders(
        HLCTimestamp transactionId,
        List<(KeyValueConflictProbe Probe, int PartitionId, bool Unconfirmed)> unanswered,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();
        Dictionary<int, string?> leaderByPartition = [];
        Dictionary<string, List<KeyValueConflictProbe>>? forwardPlan = null;
        HashSet<int>? refusedUnconfirmed = null;

        foreach ((KeyValueConflictProbe probe, int partitionId, bool unconfirmed) in unanswered)
        {
            if (!leaderByPartition.TryGetValue(partitionId, out string? leader))
            {
                leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
                if (string.Equals(leader, localNode, StringComparison.Ordinal))
                    leader = null;

                leaderByPartition[partitionId] = leader;
            }

            if (leader is null)
            {
                responses.Add((KeyValueResponseType.MustRetry, probe.Key, probe.Durability));

                if (unconfirmed)
                    (refusedUnconfirmed ??= []).Add(partitionId);

                continue;
            }

            forwardPlan ??= [];
            if (forwardPlan.TryGetValue(leader, out List<KeyValueConflictProbe>? group))
                group.Add(probe);
            else
                forwardPlan[leader] = [probe];
        }

        if (refusedUnconfirmed is not null)
        {
            foreach (int partitionId in refusedUnconfirmed)
                logger.LogCommitProbeLeadershipUnconfirmed(transactionId, partitionId, raft.GetPartitionTerm(partitionId));
        }

        if (forwardPlan is null)
            return;

        Lock lockSync = new();
        List<Task> tasks = new(forwardPlan.Count);

        foreach ((string leader, List<KeyValueConflictProbe> group) in forwardPlan)
            tasks.Add(TryCheckManyWriteIntentsOnNode(transactionId, leader, localNode, group, lockSync, responses, cancellationToken));

        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Staged-base variant of <see cref="LocateAndTryCheckManyWriteIntents"/> for the finalizer's pre-propose
    /// write-side compare-and-set: probes keys this node believes it leads <b>without</b> a read-index
    /// confirmation round or a per-key leader wait; keys it does not believe it leads fall back to the
    /// ordinary confirmed path. The leadership contract that makes the unconfirmed local answer safe for this
    /// caller and no other is the one on <see cref="LocateAndTryExistsManyValuesUnconfirmed"/>: the answer only
    /// decides whether the finalizer drives its durable decision proposal, and a deposed leader's stale answer
    /// becomes a failed proposal or a conservative abort, never a durably wrong outcome.
    ///
    /// <para>The probe is served in the key's actor as a conflict check rather than as a read, so the caller's
    /// own prefix lock over the key's bucket — a pessimistic bucket scan that then writes a member — does not
    /// refuse it the way it refuses an ordinary non-transactional read.</para>
    /// </summary>
    public async Task<List<(KeyValueResponseType type, string key, KeyValueDurability durability)>> LocateAndTryCheckManyWriteIntentsUnconfirmed(
        HLCTimestamp transactionId,
        List<KeyValueConflictProbe> keys,
        CancellationToken cancellationToken
    )
    {
        if (keys.Count == 0)
            return [];

        List<KeyValueConflictProbe>? localKeys = null;
        List<KeyValueConflictProbe>? fallbackKeys = null;

        foreach (KeyValueConflictProbe item in keys)
        {
            if (string.IsNullOrEmpty(item.Key))
                return BuildManyWriteIntentRejection(keys, KeyValueResponseType.InvalidInput);

            // Belief check only (no ack round): its fast path is a field comparison. A false
            // negative just routes the key through the confirmed path, which is always correct.
            if (await routing.AmILeaderQuickIfHosted(routing.RouteKey(item.Key)))
                (localKeys ??= new(keys.Count)).Add(item);
            else
                (fallbackKeys ??= []).Add(item);
        }

        if (fallbackKeys is null)
            return await manager.TryCheckManyWriteIntentValues(transactionId, localKeys!);

        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses = new(keys.Count);

        if (localKeys is not null)
            responses.AddRange(await manager.TryCheckManyWriteIntentValues(transactionId, localKeys));

        responses.AddRange(await LocateAndTryCheckManyWriteIntents(transactionId, fallbackKeys, cancellationToken));

        return responses;
    }

    private async Task TryCheckManyWriteIntentsOnNode(
        HLCTimestamp transactionId,
        string leader,
        string localNode,
        List<KeyValueConflictProbe> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogCheckManyWriteIntentsRedirect(xkeys.Count, leader);

        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> nodeResponses;

        if (leader == localNode)
            nodeResponses = await TryCheckManyWriteIntentsConfirmedLocally(transactionId, xkeys, cancellationToken);
        else
            nodeResponses = await interNodeCommunication.TryCheckManyWriteIntents(leader, transactionId, xkeys, cancellationToken);

        lock (lockSync)
        {
            foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) item in nodeResponses)
                responses.Add(item);
        }
    }

    /// <summary>
    /// Builds one result per requested key carrying the same rejection type, for the paths that never reach a
    /// leader. Callers correlate results by key, so a rejection has to name every key it covers.
    /// </summary>
    private static List<(KeyValueResponseType type, string key, KeyValueDurability durability)> BuildManyWriteIntentRejection(
        List<KeyValueConflictProbe> keys,
        KeyValueResponseType type
    )
    {
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> rejected = new(keys.Count);

        foreach (KeyValueConflictProbe probe in keys)
            rejected.Add((type, probe.Key, probe.Durability));

        return rejected;
    }
}

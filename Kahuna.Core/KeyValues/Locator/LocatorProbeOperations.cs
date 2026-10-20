
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
/// Lock-and-read probes over points and ranges, including the fencing of probe locks whose range moved while the probe ran.
/// </summary>
internal sealed class LocatorProbeOperations
{
    private readonly KeyValuesManager manager;

    private readonly KahunaConfiguration configuration;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly ILogger<IKahuna> logger;

    private readonly LocatorRouting routing;

    private readonly LocatorLockOperations locks;

    private readonly LocatorScanOperations scans;

    internal LocatorProbeOperations(LocatorRouting routing, LocatorLockOperations locks, LocatorScanOperations scans)
    {
        this.manager = routing.Manager;
        this.configuration = routing.Configuration;
        this.raft = routing.Raft;
        this.interNodeCommunication = routing.InterNodeCommunication;
        this.keySpaceRegistry = routing.KeySpaceRegistry;
        this.logger = routing.Logger;
        this.routing = routing;
        this.locks = locks;
        this.scans = scans;
    }

    /// <summary>
    /// Locates the leaders of a batched probe's keys and runs the probe there (see
    /// <see cref="Handlers.TryProbeManyHandler"/>). Returns one result per key, in input order.
    ///
    /// <para>The keys must belong to one key space. With key-range sharding one key space can span several
    /// partitions, so the keys are grouped by partition: a group this node leads is one actor message after the
    /// partition's leadership is confirmed (a quorum read-index for a read, the actor-mutation confirmation when
    /// the probe takes locks, as a range-lock acquire does), and the groups another node leads travel as one
    /// inter-node message per node. Each group stops at its own first lock refusal; a refusal in one partition
    /// does not stop the others, so every key's answer says for itself whether it holds a lock.</para>
    ///
    /// <para>A key-range split or merge can move a locked key to another partition between the route and the
    /// grant. A lock taken on the partition the key no longer routes to is released again and the key answers
    /// <c>MustRetry</c>, which is the generation fence a single range-lock acquire applies. A lock whose release
    /// does not land keeps <see cref="KeyValueProbeResult.Locked"/>, so the caller still records and releases it.</para>
    /// </summary>
    public async Task<KeyValueProbeResult[]> LocateAndTryProbeMany(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        IReadOnlyList<string> keys,
        KeyValueProbeLock lockMode,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        KeyValueProbeResult[] results = new KeyValueProbeResult[keys.Count];

        if (!IsWellFormedProbe(transactionId, keys, lockMode, expiresMs))
        {
            Array.Fill(results, KeyValueProbeResult.MustRetry with { Type = KeyValueResponseType.InvalidInput });
            return results;
        }

        if (!raft.Joined)
        {
            Array.Fill(results, KeyValueProbeResult.MustRetry);
            return results;
        }

        // Group the key positions by partition, keeping input order inside each group.
        int[] partitionOf = new int[keys.Count];
        Dictionary<int, List<int>> groups = new(1);

        for (int i = 0; i < keys.Count; i++)
        {
            int partitionId = routing.RouteKey(keys[i]);
            partitionOf[i] = partitionId;

            if (groups.TryGetValue(partitionId, out List<int>? positions))
                positions.Add(i);
            else
                groups[partitionId] = [i];
        }

        string localNode = raft.GetLocalEndpoint();
        Dictionary<int, string> leaderByPartition = [];
        Dictionary<string, List<int>>? remoteGroups = null;
        List<Task>? work = null;

        foreach ((int partitionId, List<int> positions) in groups)
        {
            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);

            if (leader is null)
            {
                foreach (int position in positions)
                    results[position] = KeyValueProbeResult.MustRetry;
                continue;
            }

            foreach (int position in positions)
                routing.RecordBatchKeyRoute(keys[position], partitionId, leader);

            if (leader == localNode)
            {
                (work ??= []).Add(ProbeLocalPartition(partitionId, positions));
                continue;
            }

            remoteGroups ??= [];
            if (remoteGroups.TryGetValue(leader, out List<int>? forLeader))
                forLeader.AddRange(positions);
            else
                remoteGroups[leader] = [.. positions];
        }

        if (remoteGroups is not null)
        {
            foreach ((string leader, List<int> positions) in remoteGroups)
            {
                // A node's group may join several partitions; the receiver groups them again by partition.
                if (positions.Count > 1)
                    positions.Sort();

                (work ??= []).Add(ProbeRemote(leader, positions));
            }
        }

        if (work is not null)
            await Task.WhenAll(work);

        if (lockMode == KeyValueProbeLock.Shared)
            await FenceMovedProbeLocks(transactionId, keys, partitionOf, results, durability, cancellationToken);

        return results;

        async Task ProbeLocalPartition(int partitionId, List<int> positions)
        {
            long grantTerm = routing.LockGrantTermBeforeConfirmation(partitionId);

            bool confirmed = lockMode == KeyValueProbeLock.None
                ? await routing.ConfirmLeadershipForRead(partitionId, cancellationToken)
                : await routing.ConfirmLeadershipForActorMutation(partitionId, cancellationToken);

            if (!confirmed)
            {
                foreach (int position in positions)
                    results[position] = KeyValueProbeResult.MustRetry;
                return;
            }

            KeyValueProbeResult[] answers = await manager.TryProbeMany(
                transactionId, readTimestamp, Subset(keys, positions), lockMode, expiresMs, durability);

            string? firstLocked = null;
            for (int j = 0; j < positions.Count; j++)
            {
                results[positions[j]] = answers[j];
                if (answers[j].Locked)
                    firstLocked ??= keys[positions[j]];
            }

            if (firstLocked is not null && LockGrantScope.Current is not null)
                LockGrantScope.Record(partitionId, grantTerm, firstLocked);
        }

        async Task ProbeRemote(string leader, List<int> positions)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("PROBE-MANY {Count} keys redirected to {Leader}", positions.Count, leader);

            KeyValueProbeResult[] answers = await interNodeCommunication.TryProbeMany(
                leader, transactionId, readTimestamp, Subset(keys, positions), lockMode, expiresMs, durability, cancellationToken);

            for (int j = 0; j < positions.Count; j++)
                results[positions[j]] = j < answers.Length ? answers[j] : KeyValueProbeResult.MustRetry;
        }
    }

    /// <summary>
    /// The request shape every batched probe must have before it is routed: one to the configured maximum keys,
    /// none reserved, all of one key space, and a transaction and a key space bucket when it takes locks.
    /// </summary>
    private bool IsWellFormedProbe(HLCTimestamp transactionId, IReadOnlyList<string> keys, KeyValueProbeLock lockMode, int expiresMs)
    {
        if (keys.Count == 0 || keys.Count > configuration.MaxProbeBatchSize)
            return false;

        if (lockMode != KeyValueProbeLock.None && (transactionId == HLCTimestamp.Zero || expiresMs < 0 || keys[0].IndexOf('/') < 0))
            return false;

        if (!Handlers.TryProbeManyHandler.SameKeySpace(keys))
            return false;

        foreach (string key in keys)
        {
            if (ReservedKeys.IsReserved(key))
                return false;
        }

        return true;
    }

    private static string[] Subset(IReadOnlyList<string> keys, List<int> positions)
    {
        string[] subset = new string[positions.Count];
        for (int j = 0; j < positions.Count; j++)
            subset[j] = keys[positions[j]];
        return subset;
    }

    /// <summary>
    /// Releases the probe locks granted on a partition a key no longer routes to, and answers those keys
    /// <c>MustRetry</c> with no lock. A lock the release cannot reach stays until its lease or the session ends.
    /// </summary>
    private async Task FenceMovedProbeLocks(
        HLCTimestamp transactionId,
        IReadOnlyList<string> keys,
        int[] partitionOf,
        KeyValueProbeResult[] results,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        for (int i = 0; i < results.Length; i++)
        {
            if (!results[i].Locked || routing.RouteKey(keys[i]) == partitionOf[i])
                continue;

            string key = keys[i];
            string keySpace = KeyValueKeySpace.OfKey(key)!;

            KeyValueResponseType released = await locks.ReleaseRangeLockOnPartition(
                transactionId, partitionOf[i], keySpace, key, true, key, true, durability, cancellationToken);

            if (released == KeyValueResponseType.Unlocked)
            {
                results[i] = KeyValueProbeResult.MustRetry;
                continue;
            }

            // Still held as far as this node knows: keep reporting it, so the caller records it and its
            // finalize releases it.
            logger.LogWarning("PROBE-MANY {Key} P{Pid}: release after the key moved returned {Status}; the lock stays reported as held",
                key, partitionOf[i], released);

            results[i] = KeyValueProbeResult.MustRetry with { Locked = true };
        }
    }

    /// <summary>
    /// Locates the leaders of a batched range probe's seeks and runs the probe there (see
    /// <see cref="Handlers.TryProbeManyRangesHandler"/>). Returns one result per seek, in input order.
    ///
    /// <para>Every seek is under <paramref name="prefix"/>. A seek that lies inside one partition is grouped with the
    /// other seeks of that partition: a group this node leads is one actor message after the partition's leadership is
    /// confirmed (a quorum read-index for a read, the actor-mutation confirmation when the probe takes locks, as a
    /// range-lock acquire does), and the groups another node leads travel as one inter-node message per node. Each
    /// group stops at its own first lock refusal; a refusal in one partition does not stop the others, so every
    /// seek's answer says for itself whether it holds a lock.</para>
    ///
    /// <para>A seek that crosses a partition boundary of a split key space is answered by the single-range paths
    /// instead: the multi-partition Shared acquire, with its rollback and generation fence, when the probe locks, then
    /// the stitched range page. It answers exactly what those single calls answer. A replay
    /// (<see cref="KeyValueProbeLock.SharedReentryOnly"/>) cannot renew such a split lock in one step, so that seek
    /// answers <c>MustRetry</c>.</para>
    ///
    /// <para>A split or merge can change the partitions under a locked seek between the route and the grant. A lock
    /// taken on a partition set that changed is released again and the seek answers <c>MustRetry</c>, which is the
    /// generation fence a single range-lock acquire applies. A lock whose release does not land keeps
    /// <see cref="KeyValueProbeRangeResult.Locked"/>, so the caller still records and releases it.</para>
    /// </summary>
    public async Task<KeyValueProbeRangeResult[]> LocateAndTryProbeManyRanges(
        HLCTimestamp transactionId,
        HLCTimestamp readTimestamp,
        string prefix,
        IReadOnlyList<KeyValueProbeRange> seeks,
        KeyValueProbeLock lockMode,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        KeyValueProbeRangeResult[] results = new KeyValueProbeRangeResult[seeks.Count];

        if (!Handlers.TryProbeManyRangesHandler.IsWellFormed(
                prefix, seeks, transactionId, lockMode, expiresMs, configuration.MaxProbeBatchSize, configuration.MaxProbeRangeEntries))
        {
            Array.Fill(results, KeyValueProbeRangeResult.MustRetry with { Type = KeyValueResponseType.InvalidInput });
            return results;
        }

        if (!raft.Joined)
        {
            for (int i = 0; i < results.Length; i++)
                results[i] = KeyValueProbeRangeResult.MustRetry;
            return results;
        }

        string keySpace = KeyValueKeySpace.OfPrefix(prefix);
        RangeMap rangeMap = manager.RangeMapStore.Current;
        bool singlePartition = RangeRouting.IsPrefixOpSafe(keySpaceRegistry, rangeMap, prefix);
        int prefixPartition = singlePartition ? routing.RoutePrefixKey(prefix) : 0;

        // The partitions under each locked seek when it was routed, for the generation fence after the grants.
        ArraySegment<RangeDescriptor>[]? routedOn = lockMode == KeyValueProbeLock.Shared
            ? new ArraySegment<RangeDescriptor>[seeks.Count]
            : null;

        int[] partitionOf = new int[seeks.Count];
        Dictionary<int, List<int>> groups = new(1);
        List<int>? spanning = null;

        for (int i = 0; i < seeks.Count; i++)
        {
            KeyValueProbeRange seek = seeks[i];

            // An unsplit space routes by its prefix alone; its descriptors are needed only for the lock fence.
            ArraySegment<RangeDescriptor> descriptors = singlePartition && routedOn is null
                ? default
                : rangeMap.FindIntersecting(keySpace, seek.StartKey, seek.EndKey);

            if (routedOn is not null)
                routedOn[i] = descriptors;

            int partitionId;

            if (singlePartition)
                partitionId = prefixPartition;
            else if (descriptors.Count == 0 && lockMode == KeyValueProbeLock.None)
            {
                // No range of the split key space intersects the seek: the empty page a single range page answers.
                results[i] = new(KeyValueResponseType.Get, [], false, null, HLCTimestamp.Zero, false);
                continue;
            }
            else if (descriptors.Count == 1 && WithinDescriptor(seek, descriptors[0]))
                partitionId = descriptors[0].PartitionId;
            else
            {
                (spanning ??= []).Add(i);
                continue;
            }

            partitionOf[i] = partitionId;

            if (groups.TryGetValue(partitionId, out List<int>? positions))
                positions.Add(i);
            else
                groups[partitionId] = [i];
        }

        string localNode = raft.GetLocalEndpoint();
        Dictionary<int, string> leaderByPartition = [];
        Dictionary<string, List<int>>? remoteGroups = null;
        List<Task>? work = null;

        foreach ((int partitionId, List<int> positions) in groups)
        {
            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancellationToken);

            if (leader is null)
            {
                foreach (int position in positions)
                    results[position] = KeyValueProbeRangeResult.MustRetry;
                continue;
            }

            if (leader == localNode)
            {
                (work ??= []).Add(ProbeLocalPartition(partitionId, positions));
                continue;
            }

            remoteGroups ??= [];
            if (remoteGroups.TryGetValue(leader, out List<int>? forLeader))
                forLeader.AddRange(positions);
            else
                remoteGroups[leader] = [.. positions];
        }

        if (remoteGroups is not null)
        {
            foreach ((string leader, List<int> positions) in remoteGroups)
            {
                // A node's group may join several partitions; the receiver groups them again by partition.
                if (positions.Count > 1)
                    positions.Sort();

                (work ??= []).Add(ProbeRemote(leader, positions));
            }
        }

        if (spanning is not null)
        {
            foreach (int position in spanning)
                (work ??= []).Add(ProbeSpanning(position));
        }

        if (work is not null)
            await Task.WhenAll(work);

        if (routedOn is not null)
            await FenceMovedProbeRangeLocks(transactionId, prefix, keySpace, seeks, partitionOf, routedOn, spanning, results, durability, cancellationToken);

        return results;

        async Task ProbeLocalPartition(int partitionId, List<int> positions)
        {
            long grantTerm = routing.LockGrantTermBeforeConfirmation(partitionId);

            bool confirmed = lockMode == KeyValueProbeLock.None
                ? await routing.ConfirmLeadershipForRead(partitionId, cancellationToken)
                : await routing.ConfirmLeadershipForActorMutation(partitionId, cancellationToken);

            if (!confirmed)
            {
                foreach (int position in positions)
                    results[position] = KeyValueProbeRangeResult.MustRetry;
                return;
            }

            KeyValueProbeRangeResult[] answers = await manager.TryProbeManyRanges(
                transactionId, readTimestamp, prefix, Subset(seeks, positions), lockMode, expiresMs, durability);

            bool anyLocked = false;
            string? routingKey = null;

            for (int j = 0; j < positions.Count; j++)
            {
                results[positions[j]] = answers[j];

                if (!answers[j].Locked)
                    continue;

                anyLocked = true;
                KeyValueProbeRange seek = seeks[positions[j]];
                routingKey ??= routing.RangeLockRoutingKey(partitionId, prefix, seek.StartKey, seek.EndKey);
            }

            if (!anyLocked || LockGrantScope.Current is null)
                return;

            if (routingKey is not null)
                LockGrantScope.Record(partitionId, grantTerm, routingKey);
            else
                logger.LogWarning(
                    "PROBE-MANY-RANGES {Prefix} P{Pid}: no key of the locked seeks routes to the granting partition, so a leader change that drops these locks will not be detected at commit",
                    prefix, partitionId);
        }

        async Task ProbeRemote(string leader, List<int> positions)
        {
            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("PROBE-MANY-RANGES {Count} seeks under {Prefix} redirected to {Leader}", positions.Count, prefix, leader);

            KeyValueProbeRangeResult[] answers = await interNodeCommunication.TryProbeManyRanges(
                leader, transactionId, readTimestamp, prefix, Subset(seeks, positions), lockMode, expiresMs, durability, cancellationToken);

            for (int j = 0; j < positions.Count; j++)
                results[positions[j]] = j < answers.Length ? answers[j] : KeyValueProbeRangeResult.MustRetry;
        }

        async Task ProbeSpanning(int position)
        {
            KeyValueProbeRange seek = seeks[position];
            bool locked = false;

            if (lockMode == KeyValueProbeLock.SharedReentryOnly)
            {
                results[position] = KeyValueProbeRangeResult.MustRetry;
                return;
            }

            if (lockMode == KeyValueProbeLock.Shared)
            {
                (KeyValueResponseType granted, HLCTimestamp holder) = await locks.LocateAndTryAcquireRangeLock(
                    transactionId, prefix, seek.StartKey, seek.StartInclusive, seek.EndKey, seek.EndInclusive,
                    expiresMs, durability, RangeLockMode.Shared, null, cancellationToken);

                if (granted != KeyValueResponseType.Locked)
                {
                    results[position] = KeyValueProbeRangeResult.MustRetry with { Type = granted, HolderTransactionId = holder };
                    return;
                }

                locked = true;
            }

            KeyValueGetByRangeResult page = await scans.LocateAndGetByRange(
                transactionId, prefix, seek.StartKey, seek.StartInclusive, seek.EndKey, seek.EndInclusive,
                seek.Limit, readTimestamp, durability, cancellationToken);

            results[position] = Handlers.ProbeRangesContinuation.FromPage(page, locked);
        }
    }

    /// <summary>True when the seek's bounds lie inside the descriptor's half-open range.</summary>
    private static bool WithinDescriptor(KeyValueProbeRange seek, RangeDescriptor descriptor)
    {
        (string? start, bool startInclusive, string? end, bool endInclusive) =
            LocatorScanOperations.ClipRange(seek.StartKey, seek.StartInclusive, seek.EndKey, seek.EndInclusive, descriptor);

        return string.Equals(start, seek.StartKey, StringComparison.Ordinal)
            && string.Equals(end, seek.EndKey, StringComparison.Ordinal)
            && (start is null || startInclusive == seek.StartInclusive)
            && (end is null || endInclusive == seek.EndInclusive);
    }

    private static KeyValueProbeRange[] Subset(IReadOnlyList<KeyValueProbeRange> seeks, List<int> positions)
    {
        KeyValueProbeRange[] subset = new KeyValueProbeRange[positions.Count];
        for (int j = 0; j < positions.Count; j++)
            subset[j] = seeks[positions[j]];
        return subset;
    }

    /// <summary>
    /// Releases the probe locks a partition group granted over a seek whose partitions changed since the seek was
    /// routed, and answers those seeks <c>MustRetry</c> with no lock. A seek served by the single-range paths carries
    /// that path's own fence and is skipped. A lock the release cannot reach stays until its lease or the session ends.
    /// </summary>
    private async Task FenceMovedProbeRangeLocks(
        HLCTimestamp transactionId,
        string prefix,
        string keySpace,
        IReadOnlyList<KeyValueProbeRange> seeks,
        int[] partitionOf,
        ArraySegment<RangeDescriptor>[] routedOn,
        List<int>? spanning,
        KeyValueProbeRangeResult[] results,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        RangeMap current = manager.RangeMapStore.Current;

        for (int i = 0; i < results.Length; i++)
        {
            if (!results[i].Locked || (spanning is not null && spanning.Contains(i)))
                continue;

            KeyValueProbeRange seek = seeks[i];

            if (LocatorLockOperations.DescriptorSetStable(routedOn[i], current.FindIntersecting(keySpace, seek.StartKey, seek.EndKey)))
                continue;

            KeyValueResponseType released = await locks.ReleaseRangeLockOnPartition(
                transactionId, partitionOf[i], prefix, seek.StartKey, seek.StartInclusive, seek.EndKey, seek.EndInclusive,
                durability, cancellationToken);

            if (released == KeyValueResponseType.Unlocked)
            {
                results[i] = KeyValueProbeRangeResult.MustRetry;
                continue;
            }

            // Still held as far as this node knows: keep reporting it, so the caller records it and its finalize
            // releases it.
            logger.LogWarning("PROBE-MANY-RANGES {Prefix} P{Pid}: release after the seek's partitions changed returned {Status}; the lock stays reported as held",
                prefix, partitionOf[i], released);

            results[i] = KeyValueProbeRangeResult.MustRetry with { Locked = true };
        }
    }
}

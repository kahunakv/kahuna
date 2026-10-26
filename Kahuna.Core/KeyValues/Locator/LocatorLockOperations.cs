
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
/// Exclusive point, prefix and range lock acquire and release, single and many-key.
/// </summary>
internal sealed class LocatorLockOperations
{
    private readonly KeyValuesManager manager;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly ILogger<IKahuna> logger;

    private readonly LocatorRouting routing;

    internal LocatorLockOperations(LocatorRouting routing)
    {
        this.manager = routing.Manager;
        this.raft = routing.Raft;
        this.interNodeCommunication = routing.InterNodeCommunication;
        this.keySpaceRegistry = routing.KeySpaceRegistry;
        this.logger = routing.Logger;
        this.routing = routing;
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryAcquireExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    /// <summary>Acquires a point lock on the key's leader; the fifth element is the committed base the granted
    /// lock protects (<see cref="PointLockBase"/>), or <see cref="PointLockBase.None"/> when it was not granted.</summary>
    public async Task<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId, long BaseRevision)> LocateAndTryAcquireExclusiveLock(HLCTimestamp transactionId, string key, int expiresMs, KeyValueDurability durability, CancellationToken cancelationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, key, durability, HLCTimestamp.Zero, PointLockBase.None);

        int partitionId = routing.RouteKey(key);

        if (!raft.Joined)
            return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero, PointLockBase.None);

        long grantTerm = routing.LockGrantTermBeforeConfirmation(partitionId);

        if (await routing.ConfirmLeadershipForActorMutation(partitionId, cancelationToken))
        {
            (KeyValueResponseType, string, KeyValueDurability, HLCTimestamp, long) granted =
                await manager.TryAcquireExclusiveLockObserved(transactionId, key, expiresMs, durability);

            if (granted.Item1 == KeyValueResponseType.Locked)
                LockGrantScope.Record(partitionId, grantTerm, key);

            return granted;
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancelationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero, PointLockBase.None);

        logger.LogAcquireLockKeyValueRedirected(key, partitionId, leader);

        return await interNodeCommunication.TryAcquireExclusiveLock(leader, transactionId, key, expiresMs, durability, cancelationToken);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> LocateAndTryAcquireExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefixKey))
            return KeyValueResponseType.InvalidInput;

        if (!RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefixKey))
        {
            // Deprecated by design: a prefix lock is a single-partition bucket lock and cannot cover
            // a key space that has been key-range split across partitions. Callers must use the
            // per-range exclusive range lock instead (TryAcquireExclusiveRangeLock).
            logger.LogWarning("ACQUIRE-PREFIX-LOCK: prefix {Prefix} is on a key-range-split space — prefix locks are unsupported there; use a range lock", prefixKey);
            return KeyValueResponseType.PrefixLockUnsupportedOnRangedSpace;
        }

        int partitionId = routing.RoutePrefixKey(prefixKey);

        if (!raft.Joined)
            return KeyValueResponseType.MustRetry;

        long grantTerm = routing.LockGrantTermBeforeConfirmation(partitionId);

        if (await routing.ConfirmLeadershipForActorMutation(partitionId, cancellationToken))
        {
            KeyValueResponseType granted = await manager.TryAcquireExclusivePrefixLock(transactionId, prefixKey, expiresMs, durability);

            if (granted == KeyValueResponseType.Locked)
                LockGrantScope.Record(partitionId, grantTerm, KeyValueKeySpace.OfPrefix(prefixKey) + "/");

            return granted;
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;

        logger.LogAcquirePrefixLockKeyValueRedirected(prefixKey, partitionId, leader);
        
        return await interNodeCommunication.TryAcquireExclusivePrefixLock(leader, transactionId, prefixKey, expiresMs, durability, cancellationToken);
    }
    
    /// <summary>
    /// Locates the leader node for the given keys and executes the TryAcquireManyExclusiveLocks request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp HolderTransactionId, long BaseRevision)>> LocateAndTryAcquireManyExclusiveLocks(
        HLCTimestamp transactionId,
        List<(string key, int expiresMs, KeyValueDurability durability)> keys,
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();

        Dictionary<string, List<(string key, int expiresMs, KeyValueDurability durability)>> acquisitionPlan = [];

        Dictionary<int, string> leaderByPartition = [];
        
        foreach ((string key, int expiresMs, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, key.key, key.durability, HLCTimestamp.Zero, PointLockBase.None)];

            int partitionId = routing.RouteKey(key.key);
            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancelationToken);
            if (leader is null)
                return [.. keys.Select(static k => (KeyValueResponseType.MustRetry, k.key, k.durability, HLCTimestamp.Zero, PointLockBase.None))];

            if (acquisitionPlan.TryGetValue(leader, out List<(string key, int expiresMs, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }

        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, KeyValueDurability, HLCTimestamp, long)> responses = new(keys.Count);

        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, int expiresMs, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryAcquireNodeExclusiveLocks(transactionId, leader, localNode, leaderByPartition, xkeys, lockSync, responses, cancelationToken));

        await Task.WhenAll(tasks);

        return responses;
    }

    private async Task TryAcquireNodeExclusiveLocks(
        HLCTimestamp transactionId,
        string leader,
        string localNode,
        Dictionary<int, string> leaderByPartition,
        List<(string key, int expiresMs, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder, long baseRevision)> responses,
        CancellationToken cancellationToken
    )
    {
        logger.LogAcquireManyLocksKeyValueRedirect(xkeys.Count, leader);

        if (leader == localNode)
        {
            // The terms are read before the leadership confirmation, as for a single lock, and only for the
            // partitions this node is about to grant on.
            Dictionary<int, long>? grantTerms = null;
            if (LockGrantScope.Current is not null)
            {
                grantTerms = new(leaderByPartition.Count);
                foreach ((int partitionId, string partitionLeader) in leaderByPartition)
                    if (partitionLeader == leader)
                        grantTerms[partitionId] = raft.GetPartitionTerm(partitionId);
            }

            if (!await routing.ConfirmLeadershipForGroupMutation(leader, leaderByPartition, cancellationToken))
            {
                lock (lockSync)
                {
                    foreach ((string key, int _, KeyValueDurability durability) in xkeys)
                        responses.Add((KeyValueResponseType.MustRetry, key, durability, HLCTimestamp.Zero, PointLockBase.None));
                }

                return;
            }

            List<(KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder, long baseRevision)> acquireResponses = await manager.TryAcquireManyExclusiveLocksObserved(transactionId, xkeys);

            if (grantTerms is not null)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability _, HLCTimestamp _, long _) in acquireResponses)
                {
                    if (type != KeyValueResponseType.Locked)
                        continue;

                    int partitionId = routing.RouteKey(key);
                    if (grantTerms.TryGetValue(partitionId, out long grantTerm))
                        LockGrantScope.Record(partitionId, grantTerm, key);
                }
            }

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability durability, HLCTimestamp holder, long baseRevision) item in acquireResponses)
                    responses.Add((item.type, item.key, item.durability, item.holder, item.baseRevision));
            }

            return;
        }

        await interNodeCommunication.TryAcquireNodeExclusiveLocks(leader, transactionId, xkeys, lockSync, responses, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given key and executes the TryReleaseExclusiveLock request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="key"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, string)> LocateAndTryReleaseExclusiveLock(HLCTimestamp transactionId, string key, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(key))
            return (KeyValueResponseType.InvalidInput, key);
        
        int partitionId = routing.RouteKey(key);

        // Served here on a confirmed leadership, or on belief alone for what this node holds of the
        // transaction (see LocatorRouting.AdmitLockRelease).
        LockReleaseAdmission admission = raft.Joined
            ? await routing.AdmitLockRelease(partitionId, cancellationToken)
            : LockReleaseAdmission.Confirmed;

        if (admission == LockReleaseAdmission.Confirmed)
            return await manager.TryReleaseExclusiveLock(transactionId, key, durability);

        if (admission == LockReleaseAdmission.HeldOnly)
            return await manager.TryReleaseExclusiveLockHeldHere(transactionId, key, durability);
            
        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, key);
        
        logger.LogReleaseLockKeyValueRedirected(key, partitionId, leader);
        
        return await interNodeCommunication.TryReleaseExclusiveLock(leader, transactionId, key, durability, cancellationToken);
    }
    
    /// <summary>
    /// 
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="prefixKey"></param>
    /// <param name="expiresMs"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueResponseType> LocateAndTryReleaseExclusivePrefixLock(
        HLCTimestamp transactionId,
        string prefixKey,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefixKey))
            return KeyValueResponseType.InvalidInput;

        if (!RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefixKey))
        {
            // Deprecated by design (see acquire): prefix locks are unsupported on key-range-split
            // spaces. A release on such a space can only be a caller error — there is no prefix lock
            // to release — so surface the typed response rather than a generic error.
            logger.LogWarning("RELEASE-PREFIX-LOCK: prefix {Prefix} is on a key-range-split space — prefix locks are unsupported there; use a range lock", prefixKey);
            return KeyValueResponseType.PrefixLockUnsupportedOnRangedSpace;
        }

        int partitionId = routing.RoutePrefixKey(prefixKey);

        // Served here on a confirmed leadership, or on belief alone for what this node holds of the
        // transaction (see LocatorRouting.AdmitLockRelease).
        LockReleaseAdmission admission = raft.Joined
            ? await routing.AdmitLockRelease(partitionId, cancellationToken)
            : LockReleaseAdmission.Confirmed;

        if (admission == LockReleaseAdmission.Confirmed)
            return await manager.TryReleaseExclusivePrefixLock(transactionId, prefixKey, durability);

        if (admission == LockReleaseAdmission.HeldOnly)
            return await manager.TryReleaseExclusivePrefixLockHeldHere(transactionId, prefixKey, durability);
            
        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;
        
        logger.LogReleasePrefixLockKeyValueRedirected(prefixKey, partitionId, leader);
        
        return await interNodeCommunication.TryReleaseExclusivePrefixLock(leader, transactionId, prefixKey, durability, cancellationToken);
    }
    
    /// <summary>
    /// Acquires an exclusive range lock covering <c>[startKey, endKey)</c> within <paramref name="prefix"/>.
    /// Fan-outs to one sub-lock per intersecting <see cref="RangeDescriptor"/>; rolls back partial
    /// acquisitions on failure.
    ///
    /// <para><b>Local-snapshot fence.</b> After acquiring all sub-locks the method re-reads
    /// <see cref="RangeMapStore.Current"/> and compares descriptor sets via
    /// <see cref="DescriptorSetStable"/>. If any descriptor was added, removed, or bumped (a split
    /// or merge committed in the acquire window <em>and replicated to this node</em>), all sub-locks
    /// are released and <c>MustRetry</c> is returned so the caller re-routes on the fresh map.</para>
    ///
    /// <para><b>Limitation — local-node visibility only.</b> The fence compares two consecutive
    /// reads of this node's local descriptor map. A split that committed on the meta-partition leader
    /// but has not yet replicated here is invisible to both reads: the lock lands on the pre-split
    /// partition and a writer on an ahead node that already sees the new partition is not blocked.
    /// The write-path generation fence (carried on every <c>TrySet</c> / 2PC-prepare RPC) remains
    /// the primary serializability guard; this fence is a best-effort defence for the
    /// frequently-consistent case. Fully closing the cross-node skew window would require the lock
    /// to carry and verify a descriptor generation end-to-end against the writer's view.</para>
    /// </summary>
    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken
    ) => LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, null, cancellationToken);

    public Task<(KeyValueResponseType, HLCTimestamp HolderTransactionId)> LocateAndTryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    ) => LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive, null, cancellationToken);

    internal Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        Func<Task>? afterSnapshot,
        CancellationToken cancellationToken
    ) => LocateAndTryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, RangeLockMode.Exclusive, afterSnapshot, cancellationToken);

    internal async Task<(KeyValueResponseType, HLCTimestamp)> LocateAndTryAcquireRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        Func<Task>? afterSnapshot,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefix))
            return (KeyValueResponseType.InvalidInput, HLCTimestamp.Zero);

        ArraySegment<RangeDescriptor> descriptors =
            manager.RangeMapStore.Current.FindIntersecting(KeyValueKeySpace.OfPrefix(prefix), startKey, endKey);

        if (afterSnapshot != null)
            await afterSnapshot();

        if (descriptors.Count == 0)
        {
            // Hash-space or range space with no descriptors yet: single-partition path.
            // Hash spaces never split, so no generation fence is needed.
            int hashPartitionId = routing.RoutePrefixKey(prefix);
            return await AcquireRangeLockOnPartition(transactionId, hashPartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);
        }

        if (descriptors.Count == 1)
        {
            (KeyValueResponseType result, HLCTimestamp holder) = await AcquireRangeLockOnPartition(
                transactionId, descriptors[0].PartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);

            if (result != KeyValueResponseType.Locked)
                return (result, holder);

            // Generation fence: a split that committed after FindIntersecting but before the
            // sub-lock RPC would leave P' un-locked. Re-check the map; if the descriptor set
            // changed, roll back and signal the caller to re-resolve.
            if (!DescriptorSetStable(descriptors, manager.RangeMapStore.Current.FindIntersecting(KeyValueKeySpace.OfPrefix(prefix), startKey, endKey)))
            {
                KeyValueResponseType rel = await ReleaseRangeLockOnPartition(transactionId, descriptors[0].PartitionId, prefix,
                    startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);

                if (rel != KeyValueResponseType.Unlocked)
                    logger.LogWarning("ACQUIRE-RANGE-LOCK {Prefix} P{Pid}: fence rollback release returned {Status} — sub-lock leaks until TTL",
                        prefix, descriptors[0].PartitionId, rel);

                return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
            }

            return (KeyValueResponseType.Locked, HLCTimestamp.Zero);
        }

        // Multi-descriptor: per-range sub-locks with roll-back on partial failure.
        var acquired = new List<(int PartitionId, string? ClampStart, bool ClampStartIncl, string? ClampEnd, bool ClampEndIncl)>(descriptors.Count);

        foreach (RangeDescriptor desc in descriptors)
        {
            (string? cs, bool csI, string? ce, bool ceI) = LocatorScanOperations.ClipRange(
                startKey, startInclusive, endKey, endInclusive, desc);

            (KeyValueResponseType result, HLCTimestamp holder) = await AcquireRangeLockOnPartition(
                transactionId, desc.PartitionId, prefix, cs, csI, ce, ceI, expiresMs, durability, mode, cancellationToken);

            if (result == KeyValueResponseType.Locked)
            {
                acquired.Add((desc.PartitionId, cs, csI, ce, ceI));
                continue;
            }

            foreach ((int pid, string? rcs, bool rcsi, string? rce, bool rcei) in acquired)
            {
                KeyValueResponseType rel = await ReleaseRangeLockOnPartition(
                    transactionId, pid, prefix, rcs, rcsi, rce, rcei, durability, cancellationToken);

                if (rel != KeyValueResponseType.Unlocked)
                    logger.LogWarning("ACQUIRE-RANGE-LOCK {Prefix} P{Pid}: partial-acquire rollback release returned {Status} — sub-lock leaks until TTL",
                        prefix, pid, rel);
            }

            return (result, holder);
        }

        // Generation fence: re-check after all sub-locks are held. If the map changed
        // (split committed in the acquire window), roll everything back and MustRetry.
        if (!DescriptorSetStable(descriptors, manager.RangeMapStore.Current.FindIntersecting(KeyValueKeySpace.OfPrefix(prefix), startKey, endKey)))
        {
            logger.LogAcquireRangeLockDescriptorChanged(prefix);

            foreach ((int pid, string? rcs, bool rcsi, string? rce, bool rcei) in acquired)
            {
                KeyValueResponseType rel = await ReleaseRangeLockOnPartition(
                    transactionId, pid, prefix, rcs, rcsi, rce, rcei, durability, cancellationToken);

                if (rel != KeyValueResponseType.Unlocked)
                    logger.LogWarning("ACQUIRE-RANGE-LOCK {Prefix} P{Pid}: fence rollback release returned {Status} — sub-lock leaks until TTL",
                        prefix, pid, rel);
            }

            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);
        }

        return (KeyValueResponseType.Locked, HLCTimestamp.Zero);
    }

    /// <summary>
    /// Returns true when both snapshots cover the same descriptors in the same order with the
    /// same generations. Used as the acquire-time generation fence: if false, a split or merge
    /// committed <em>and replicated to this node</em> in the window between
    /// <c>FindIntersecting</c> and the sub-lock RPCs, and the caller must roll back acquired
    /// sub-locks and retry. Splits that have not yet replicated here are not detected — see the
    /// <c>LocateAndTryAcquireExclusiveRangeLock</c> doc for the full local-snapshot caveat.
    /// </summary>
    internal static bool DescriptorSetStable(
        ArraySegment<RangeDescriptor> before,
        ArraySegment<RangeDescriptor> after)
    {
        if (before.Count != after.Count)
            return false;

        for (int i = 0; i < before.Count; i++)
        {
            if (before[i].PartitionId != after[i].PartitionId ||
                before[i].Generation  != after[i].Generation)
                return false;
        }

        return true;
    }

    public async Task<KeyValueResponseType> LocateAndTryReleaseExclusiveRangeLock(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        if (string.IsNullOrEmpty(prefix))
            return KeyValueResponseType.InvalidInput;

        ArraySegment<RangeDescriptor> descriptors =
            manager.RangeMapStore.Current.FindIntersecting(KeyValueKeySpace.OfPrefix(prefix), startKey, endKey);

        if (descriptors.Count == 0)
        {
            int hashPartitionId = routing.RoutePrefixKey(prefix);
            return await ReleaseRangeLockOnPartition(transactionId, hashPartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
        }

        if (descriptors.Count == 1)
        {
            return await ReleaseRangeLockOnPartition(transactionId, descriptors[0].PartitionId, prefix,
                startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
        }

        // Release all sub-locks even if one fails (best-effort). Return Unlocked only when
        // every descriptor released successfully; return the first non-Unlocked result otherwise
        // so the caller knows at least one sub-lock was not cleaned up.
        KeyValueResponseType firstFailure = KeyValueResponseType.Unlocked;
        foreach (RangeDescriptor desc in descriptors)
        {
            (string? cs, bool csI, string? ce, bool ceI) = LocatorScanOperations.ClipRange(
                startKey, startInclusive, endKey, endInclusive, desc);

            KeyValueResponseType rel = await ReleaseRangeLockOnPartition(
                transactionId, desc.PartitionId, prefix, cs, csI, ce, ceI, durability, cancellationToken);

            if (rel != KeyValueResponseType.Unlocked)
            {
                logger.LogWarning("RELEASE-RANGE-LOCK {Prefix} P{Pid}: release returned {Status} — sub-lock leaks until TTL",
                    prefix, desc.PartitionId, rel);

                if (firstFailure == KeyValueResponseType.Unlocked)
                    firstFailure = rel;
            }
        }

        return firstFailure;
    }

    private async Task<(KeyValueResponseType, HLCTimestamp)> AcquireRangeLockOnPartition(
        HLCTimestamp transactionId,
        int partitionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int expiresMs,
        KeyValueDurability durability,
        RangeLockMode mode,
        CancellationToken cancellationToken)
    {
        long grantTerm = routing.LockGrantTermBeforeConfirmation(partitionId);

        if (!raft.Joined || await routing.ConfirmLeadershipForActorMutation(partitionId, cancellationToken))
        {
            (KeyValueResponseType, HLCTimestamp) granted =
                await manager.TryAcquireRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode);

            if (granted.Item1 == KeyValueResponseType.Locked && LockGrantScope.Current is not null)
            {
                if (routing.RangeLockRoutingKey(partitionId, prefix, startKey, endKey) is { } routingKey)
                    LockGrantScope.Record(partitionId, grantTerm, routingKey);
                else
                    logger.LogWarning(
                        "ACQUIRE-RANGE-LOCK {Prefix} P{Pid}: no key of the locked range routes to the granting partition, so a leader change that drops this lock will not be detected at commit",
                        prefix, partitionId);
            }

            return granted;
        }

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return (KeyValueResponseType.MustRetry, HLCTimestamp.Zero);

        logger.LogAcquireRangeLockKeyValueRedirected(prefix, partitionId, leader);

        return await interNodeCommunication.TryAcquireRangeLock(leader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, expiresMs, durability, mode, cancellationToken);
    }

    internal async Task<KeyValueResponseType> ReleaseRangeLockOnPartition(
        HLCTimestamp transactionId,
        int partitionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        KeyValueDurability durability,
        CancellationToken cancellationToken)
    {
        // Served here on a confirmed leadership, or on belief alone for what this node holds of the
        // transaction: the commit-time lock proof refuses a transaction when the confirmation does not come,
        // and the cleanup that follows the refusal must still release the range locks the transaction holds
        // here (see LocatorRouting.AdmitLockRelease).
        LockReleaseAdmission admission = raft.Joined
            ? await routing.AdmitLockRelease(partitionId, cancellationToken)
            : LockReleaseAdmission.Confirmed;

        if (admission == LockReleaseAdmission.Confirmed)
            return await manager.TryReleaseExclusiveRangeLock(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability);

        if (admission == LockReleaseAdmission.HeldOnly)
            return await manager.TryReleaseExclusiveRangeLockHeldHere(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability);

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return KeyValueResponseType.MustRetry;

        logger.LogReleaseRangeLockKeyValueRedirected(prefix, partitionId, leader);

        return await interNodeCommunication.TryReleaseExclusiveRangeLock(leader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, durability, cancellationToken);
    }

    /// <summary>
    /// Locates the leader node for the given keys and executes the TryReleaseManyExclusiveLocks request.
    /// </summary>
    /// <param name="transactionId"></param>
    /// <param name="keys"></param>
    /// <param name="cancelationToken"></param>
    /// <returns></returns>
    public async Task<List<(KeyValueResponseType, string, KeyValueDurability)>> LocateAndTryReleaseManyExclusiveLocks(
        HLCTimestamp transactionId, 
        List<(string key, KeyValueDurability durability)> keys, 
        CancellationToken cancelationToken
    )
    {
        string localNode = raft.GetLocalEndpoint();
        
        Dictionary<string, List<(string key, KeyValueDurability durability)>> acquisitionPlan = [];

        Dictionary<int, string> leaderByPartition = [];
        
        foreach ((string key, KeyValueDurability durability) key in keys)
        {
            if (string.IsNullOrEmpty(key.key))
                return [(KeyValueResponseType.InvalidInput, key.key, key.durability)];

            int partitionId = routing.RouteKey(key.key);
            string? leader = await routing.TryWaitForLeader(partitionId, leaderByPartition, cancelationToken);
            if (leader is null)
                return [.. keys.Select(static k => (KeyValueResponseType.MustRetry, k.key, k.durability))];

            if (acquisitionPlan.TryGetValue(leader, out List<(string key, KeyValueDurability durability)>? list))
                list.Add(key);
            else
                acquisitionPlan[leader] = [key];
        }
        
        Lock lockSync = new();
        List<Task> tasks = new(acquisitionPlan.Count);
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = new(keys.Count);
        
        // Requests to nodes are sent in parallel
        foreach ((string leader, List<(string key, KeyValueDurability durability)> xkeys) in acquisitionPlan)
            tasks.Add(TryReleaseNodeExclusiveLocks(transactionId, leader, localNode, leaderByPartition, xkeys, lockSync, responses, cancelationToken));
        
        await Task.WhenAll(tasks);

        return responses;
    }
    
    private async Task TryReleaseNodeExclusiveLocks(
        HLCTimestamp transactionId, 
        string leader, 
        string localNode, 
        Dictionary<int, string> leaderByPartition,
        List<(string key, KeyValueDurability durability)> xkeys,
        Lock lockSync,
        List<(KeyValueResponseType type, string key, KeyValueDurability durability)> responses,
        CancellationToken cancelationToken
    )
    {
        logger.LogReleaseManyLocksKeyValueRedirect(xkeys.Count, leader);
        
        if (leader == localNode)
        {
            // Served here on a confirmed leadership, or on belief alone for what this node holds of the
            // transaction (see LocatorRouting.AdmitLockRelease).
            LockReleaseAdmission admission = await routing.AdmitGroupLockRelease(leader, leaderByPartition, cancelationToken);

            if (admission == LockReleaseAdmission.NotHere)
            {
                lock (lockSync)
                {
                    foreach ((string key, KeyValueDurability durability) in xkeys)
                        responses.Add((KeyValueResponseType.MustRetry, key, durability));
                }

                return;
            }

            List<(KeyValueResponseType type, string key, KeyValueDurability durability)> acquireResponses = admission == LockReleaseAdmission.Confirmed
                ? await manager.TryReleaseManyExclusiveLocks(transactionId, xkeys)
                : await manager.TryReleaseManyExclusiveLocksHeldHere(transactionId, xkeys);

            lock (lockSync)
            {
                foreach ((KeyValueResponseType type, string key, KeyValueDurability durability) item in acquireResponses)
                    responses.Add((item.type, item.key, item.durability));
            }

            return;
        }
            
        await interNodeCommunication.TryReleaseNodeExclusiveLocks(leader, transactionId, xkeys, lockSync, responses, cancelationToken);
    }
}


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
/// Bucket, range and prefix scans, including the per-descriptor fan-out of a range scan.
/// </summary>
internal sealed class LocatorScanOperations
{
    private readonly KeyValuesManager manager;

    private readonly IRaft raft;

    private readonly IInterNodeCommunication interNodeCommunication;

    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly ILogger<IKahuna> logger;

    private readonly LocatorRouting routing;

    internal LocatorScanOperations(LocatorRouting routing)
    {
        this.manager = routing.Manager;
        this.raft = routing.Raft;
        this.interNodeCommunication = routing.InterNodeCommunication;
        this.keySpaceRegistry = routing.KeySpaceRegistry;
        this.logger = routing.Logger;
        this.routing = routing;
    }

    /// <summary>
    /// Locates the appropriate node for the specified key prefix and retrieves the corresponding key-value items.
    /// For unsplit spaces routes to the single partition leader. For split key-range spaces fans out across
    /// all descriptors in parallel, pages through each with <see cref="QueryDescriptorRange"/>, and
    /// returns the concatenated result.
    ///
    /// <para>
    /// <b>Fan-out model:</b> all descriptors are queried concurrently via <c>Task.WhenAll</c>,
    /// bounded by <c>MaxParallelBucketDescriptors = 8</c>. One <see cref="QueryDescriptorRange"/> task
    /// is issued per descriptor; the concurrency limit ensures the fan-out does not stampede the cluster
    /// with descriptors at the boundaries of that limit.
    /// </para>
    ///
    /// <para>
    /// <b>Leader coalescing — transport-delegated, locator-level deferred.</b> When several descriptors
    /// share the same leader endpoint, this method still issues one <see cref="QueryDescriptorRange"/>
    /// call per descriptor (not one per unique leader). On the gRPC transport, concurrent calls to the
    /// same endpoint are automatically multiplexed onto a single streaming connection by
    /// <c>GrpcServerBatcher</c>, so the wire cost is already one stream per leader — but the locator
    /// does not merge the descriptor ranges before dispatching. True locator-level grouping (resolve
    /// leaders → merge adjacent same-leader ranges → one <c>GetByRange</c> per leader) would further
    /// reduce RPC message count on the in-memory and gRPC transports alike; that optimisation is
    /// deferred for now.
    /// </para>
    ///
    /// <para>
    /// Full materialisation: the entire bucket is buffered before returning. Callers needing
    /// bounded-memory streaming should use <see cref="LocateAndGetByRange"/> instead.
    /// </para>
    /// </summary>
    public Task<KeyValueGetByBucketResult> LocateAndGetByBucket(
        HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability,
        CancellationToken cancellationToken) =>
        LocateAndGetByBucket(transactionId, prefixedKey, readTimestamp, durability, null, null, cancellationToken);

    /// <summary>
    /// Internal overload with test hooks.
    /// <paramref name="beforeQuery"/> is called (with the descriptor index) after the semaphore is
    /// acquired but before the first page RPC for that descriptor — lets tests gate all tasks to prove
    /// concurrency.
    /// <paramref name="afterDescriptor"/> is called after all pages for a descriptor are collected —
    /// lets tests inject a mid-fan-out split for <c>Bucket_SplitMidScan_NoDupNoMissing</c>.
    /// </summary>
    internal async Task<KeyValueGetByBucketResult> LocateAndGetByBucket(
        HLCTimestamp transactionId, string prefixedKey, HLCTimestamp readTimestamp, KeyValueDurability durability,
        Func<int, Task>? beforeQuery,
        Func<int, Task>? afterDescriptor,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(prefixedKey))
            return new(KeyValueResponseType.Errored, []);

        // Fast path: unsplit (or hash/schema-log) space — single partition, no fan-out overhead.
        if (RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefixedKey))
        {
            int singlePartitionId = routing.RoutePrefixKey(prefixedKey);

            if (!raft.Joined || await routing.ConfirmLeadershipForRead(singlePartitionId, cancellationToken))
                return await manager.GetByBucket(transactionId, prefixedKey, readTimestamp, durability);

            string? singleLeader = await routing.TryWaitForLeader(singlePartitionId, cancellationToken);
            if (singleLeader is null || singleLeader == raft.GetLocalEndpoint())
                return new(KeyValueResponseType.MustRetry, []);

            logger.LogGetPrefixKeyValueRedirected(prefixedKey, singlePartitionId, singleLeader);

            return await interNodeCommunication.GetByBucket(singleLeader, transactionId, prefixedKey, readTimestamp, durability, cancellationToken);
        }

        // Multi-range path (parallel): key-range space is split; fan out to all descriptors
        // concurrently. Snapshot the map once — safe because orphan retention + MVCC means the source
        // partition still answers snapshot reads for stale entries after a cutover.
        // The map knows the key space by its bare name; the scan prefix may spell it with a trailing slash.
        string keySpace = KeyValueKeySpace.OfPrefix(prefixedKey);
        IReadOnlyList<RangeDescriptor> descriptors = manager.RangeMapStore.Current.FindAll(keySpace);

        if (descriptors.Count == 0)
            return new(KeyValueResponseType.Get, []);

        const int bucketPageSize = 512;
        const int maxParallelDescriptors = 8;

        using var sem = new SemaphoreSlim(maxParallelDescriptors, maxParallelDescriptors);

        // Pre-allocate one slot per descriptor; tasks write by index so no lock is needed.
        var slots = new (KeyValueResponseType Type, List<(string, ReadOnlyKeyValueEntry)> Items)[descriptors.Count];

        Task[] fanOutTasks = Enumerable.Range(0, descriptors.Count)
            .Select(idx => FetchDescriptorSlotAsync(
                idx, descriptors[idx], transactionId, prefixedKey,
                readTimestamp, durability, bucketPageSize, sem, slots,
                beforeQuery, afterDescriptor, cancellationToken))
            .ToArray();

        await Task.WhenAll(fanOutTasks);

        // Propagate any early-exit response (MustRetry / WaitingForReplication).
        foreach ((KeyValueResponseType type, _) in slots)
            if (type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                return new(type, []);

        // Concatenate in descriptor StartKey order (FindAll is sorted; ranges are disjoint, so
        // the concatenation is already globally ordered — same guarantee as the sequential version).
        var allItems = new List<(string, ReadOnlyKeyValueEntry)>();
        foreach ((_, List<(string, ReadOnlyKeyValueEntry)> items) in slots)
            allItems.AddRange(items);

        return new(KeyValueResponseType.Get, allItems);
    }

    private async Task FetchDescriptorSlotAsync(
        int idx, RangeDescriptor descriptor,
        HLCTimestamp transactionId, string prefixedKey,
        HLCTimestamp readTimestamp, KeyValueDurability durability,
        int bucketPageSize, SemaphoreSlim sem,
        (KeyValueResponseType, List<(string, ReadOnlyKeyValueEntry)>)[] slots,
        Func<int, Task>? beforeQuery, Func<int, Task>? afterDescriptor,
        CancellationToken cancellationToken)
    {
        await sem.WaitAsync(cancellationToken);
        try
        {
            if (beforeQuery is not null)
                await beforeQuery(idx);

            (string? clStart, bool clStartInc, string? clEnd, bool clEndInc) =
                ClipRange(null, true, null, false, descriptor);

            string? cursorKey = clStart;
            bool    cursorInc = clStartInc;
            var items = new List<(string, ReadOnlyKeyValueEntry)>();

            while (true)
            {
                KeyValueGetByRangeResult page = await QueryDescriptorRange(
                    descriptor.PartitionId, transactionId, prefixedKey,
                    cursorKey, cursorInc, clEnd, clEndInc,
                    bucketPageSize, readTimestamp, durability, cancellationToken);

                if (page.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
                {
                    slots[idx] = (page.Type, []);
                    return;
                }

                if (page.Type != KeyValueResponseType.Get)
                    break;

                items.AddRange(page.Items);

                if (!page.HasMore || page.NextCursor is null)
                    break;

                if (!KeyValueRangeCursor.TryDecode(page.NextCursor, out string lastKey, out _, out _, out _))
                    break;

                cursorKey = lastKey;
                cursorInc = false;
            }

            slots[idx] = (KeyValueResponseType.Get, items);

            if (afterDescriptor is not null)
                await afterDescriptor(idx);
        }
        finally
        {
            sem.Release();
        }
    }

    /// <summary>
    /// Locates the leader for the given prefix and executes a bounded, cursor-paged range scan.
    /// For unsplit spaces routes to the single partition leader directly. For split key-range spaces
    /// fans out across all intersecting descriptors in StartKey order, clips each sub-range, and
    /// merges results maintaining key order (multi-range stitch).
    ///
    /// <para><paramref name="snapshotAtLeader"/> replaces <paramref name="readTimestamp"/> with a snapshot
    /// minted on each serving leader, after its leadership check; see <see cref="IKahuna.LocateAndGetByRange"/>.</para>
    /// </summary>
    public async Task<KeyValueGetByRangeResult> LocateAndGetByRange(
        HLCTimestamp transactionId,
        string prefix,
        string? startKey,
        bool startInclusive,
        string? endKey,
        bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        bool snapshotAtLeader = false)
    {
        if (string.IsNullOrEmpty(prefix))
            return new(KeyValueResponseType.Errored, [], null, false);

        // Fast path: unsplit space (or hash space) — single partition, no fan-out overhead.
        if (RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefix))
        {
            int singlePartitionId = routing.RoutePrefixKey(prefix);

            if (!raft.Joined || await routing.ConfirmLeadershipForRead(singlePartitionId, cancellationToken))
                return await manager.GetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, ServedPageSnapshot(readTimestamp, snapshotAtLeader), durability);

            string? singleLeader = await routing.TryWaitForLeader(singlePartitionId, cancellationToken);
            if (singleLeader is null || singleLeader == raft.GetLocalEndpoint())
                return new(KeyValueResponseType.MustRetry, [], null, false);

            logger.LogGetRangeKeyValueRedirected(prefix, singlePartitionId, singleLeader);

            return MarkServedRemotely(await interNodeCommunication.GetByRange(singleLeader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken, snapshotAtLeader));
        }

        // Multi-range path: key-range space has been split; fan out across intersecting descriptors.
        // RangeMap is snapshotted once for this page. A split landing mid-fan-out means the loop
        // may query a now-stale source partition, but that is safe: the split transaction orphan-retains [K,E) on
        // the source, so the fixed readTimestamp (MVCC) still resolves correctly from there. The
        // next page re-resolves RangeMapStore.Current fresh and routes to the new partition.
        // The map knows the key space by its bare name; the scan prefix may spell it with a trailing slash.
        string keySpace = KeyValueKeySpace.OfPrefix(prefix);
        RangeMap rangeMap = manager.RangeMapStore.Current;
        ArraySegment<RangeDescriptor> descriptors = rangeMap.FindIntersecting(keySpace, startKey, endKey);

        if (descriptors.Count == 0)
            return new(KeyValueResponseType.Get, [], null, false, default(KeyValueScanObservation));

        var accumulated = new List<(string, ReadOnlyKeyValueEntry)>();
        int remaining   = limit > 0 ? limit : int.MaxValue;
        bool hasMore    = false;
        // The stitched page costs the sum of its parts; one part with an unknown cost makes the sum unknown.
        KeyValueScanObservation? observation = default(KeyValueScanObservation);

        foreach (RangeDescriptor descriptor in descriptors)
        {
            if (remaining <= 0) { hasMore = true; break; }

            (string? clStart, bool clStartInc, string? clEnd, bool clEndInc) =
                ClipRange(startKey, startInclusive, endKey, endInclusive, descriptor);

            int pageLimit = remaining == int.MaxValue ? 0 : remaining;

            KeyValueGetByRangeResult part = await QueryDescriptorRange(
                descriptor.PartitionId, transactionId, prefix,
                clStart, clStartInc, clEnd, clEndInc,
                pageLimit, readTimestamp, durability, cancellationToken, snapshotAtLeader);

            observation = KeyValueScanObservation.Combine(observation, part.Observation);

            if (part.Type is KeyValueResponseType.MustRetry or KeyValueResponseType.WaitingForReplication)
            {
                // The refused part reports what the whole fan-out cost so far, not only its own attempt.
                part.Observation = observation;
                return part;
            }

            if (part.Type != KeyValueResponseType.Get)
                continue;

            accumulated.AddRange(part.Items);

            if (limit > 0)
                remaining -= part.Items.Count;

            if (part.HasMore) { hasMore = true; break; }
        }

        if (accumulated.Count == 0)
            return new(KeyValueResponseType.Get, [], null, false, observation);

        string? cursor = null;
        if (hasMore)
        {
            string lastKey = accumulated[^1].Item1;
            HLCTimestamp ts = readTimestamp.IsNull() ? HLCTimestamp.Zero : readTimestamp;
            // Intentionally generation-free: each page re-resolves FindIntersecting from lastKey
            // against the live map, so a split between pages is handled unconditionally — no
            // generation miss-detection needed. Do not add rangeGeneration here.
            cursor = KeyValueRangeCursor.Encode(lastKey, durability, prefix, ts);
        }

        return new(KeyValueResponseType.Get, accumulated, cursor, hasMore, observation);
    }

    /// <summary>
    /// Flags a page a peer served. A page with no observation (a peer on an older build) stays unknown.
    /// </summary>
    private static KeyValueGetByRangeResult MarkServedRemotely(KeyValueGetByRangeResult result)
    {
        if (result.Observation is { } observation)
            result.Observation = observation with { ServedRemotely = true };

        return result;
    }

    /// <summary>Routes a GetByRange page to <paramref name="partitionId"/>'s leader.</summary>
    private async Task<KeyValueGetByRangeResult> QueryDescriptorRange(
        int partitionId,
        HLCTimestamp transactionId,
        string prefix,
        string? startKey, bool startInclusive,
        string? endKey,   bool endInclusive,
        int limit,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken,
        bool snapshotAtLeader = false)
    {
        if (!raft.Joined || await routing.ConfirmLeadershipForRead(partitionId, cancellationToken))
            return await manager.GetByRange(transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, ServedPageSnapshot(readTimestamp, snapshotAtLeader), durability);

        string? leader = await routing.TryWaitForLeader(partitionId, cancellationToken);
        if (leader is null || leader == raft.GetLocalEndpoint())
            return new(KeyValueResponseType.MustRetry, [], null, false);

        return MarkServedRemotely(await interNodeCommunication.GetByRange(leader, transactionId, prefix, startKey, startInclusive, endKey, endInclusive, limit, readTimestamp, durability, cancellationToken, snapshotAtLeader));
    }

    /// <summary>
    /// The snapshot a range page served by this node reads at. A leader-minted snapshot is taken here —
    /// on the serving node, after its leadership was confirmed — so it orders after every write intent
    /// already planted on this node's actors: each plant folds the writer's transaction id into this
    /// node's clock, and a later local event is strictly above it. That is what makes the safe-time
    /// rule wait on all of those intents. Minted anywhere else, the snapshot only orders after the
    /// intents whose transaction ids happen to sort below the minting node's clock.
    /// </summary>
    private HLCTimestamp ServedPageSnapshot(HLCTimestamp readTimestamp, bool snapshotAtLeader) =>
        snapshotAtLeader ? raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId()) : readTimestamp;

    /// <summary>
    /// Clips the caller's query range <c>[queryStart, queryEnd)</c> to the descriptor's half-open
    /// interval <c>[d.StartKey, d.EndKey)</c>, preserving the caller's inclusive/exclusive flags
    /// where they dominate; the descriptor boundary is always inclusive at start, exclusive at end.
    /// </summary>
    internal static (string? start, bool startInc, string? end, bool endInc) ClipRange(
        string? queryStart, bool queryStartInc,
        string? queryEnd,   bool queryEndInc,
        RangeDescriptor d)
    {
        string? clStart;
        bool    clStartInc;

        if (d.StartKey is null)
        {
            // descriptor starts at -∞; query start is the effective lower bound
            clStart    = queryStart;
            clStartInc = queryStartInc;
        }
        else if (queryStart is null)
        {
            // query unbounded below; descriptor's start is the effective lower bound (inclusive)
            clStart    = d.StartKey;
            clStartInc = true;
        }
        else
        {
            int cmp = string.CompareOrdinal(queryStart, d.StartKey);
            if (cmp >= 0) { clStart = queryStart; clStartInc = queryStartInc; }
            else          { clStart = d.StartKey;  clStartInc = true; }
        }

        string? clEnd;
        bool    clEndInc;

        if (d.EndKey is null && queryEnd is null)
        {
            clEnd    = null;
            clEndInc = false;
        }
        else if (d.EndKey is null)
        {
            clEnd    = queryEnd;
            clEndInc = queryEndInc;
        }
        else if (queryEnd is null)
        {
            clEnd    = d.EndKey;
            clEndInc = false;  // descriptor boundary is always exclusive
        }
        else
        {
            int cmp = string.CompareOrdinal(queryEnd, d.EndKey);
            if (cmp <= 0) { clEnd = queryEnd;  clEndInc = queryEndInc; }
            else          { clEnd = d.EndKey;   clEndInc = false; }
        }

        return (clStart, clStartInc, clEnd, clEndInc);
    }

    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix
    /// </summary>
    /// <param name="prefixKeyName"></param>
    /// <param name="durability"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<KeyValueGetByBucketResult> ScanAllByPrefix(string prefixKeyName, HLCTimestamp readTimestamp, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        // Every per-node view (and the local disk page) can lag the commit frontier: a node that has
        // not yet applied a committed delete or overwrite still contributes the previous version of
        // the key. Merging newest-wins by (revision, commit HLC) — with tombstones traveling so a
        // delete is a mergeable version rather than an absence — lets the freshest contribution
        // (the key's partition leader applies synchronously before acking, and is always scanned)
        // suppress the stale copies. First-wins union would resurrect deleted keys and serve stale
        // values whenever a lagging node happened to be merged first.
        Dictionary<string, ReadOnlyKeyValueEntry> merged = new(StringComparer.Ordinal);

        static void MergeItems(Dictionary<string, ReadOnlyKeyValueEntry> merged, List<(string, ReadOnlyKeyValueEntry)> items)
        {
            foreach ((string key, ReadOnlyKeyValueEntry entry) in items)
            {
                if (merged.TryGetValue(key, out ReadOnlyKeyValueEntry? existing)
                    && (existing.Revision > entry.Revision
                        || (existing.Revision == entry.Revision && existing.LastModified >= entry.LastModified)))
                    continue;

                merged[key] = entry;
            }
        }

        KeyValueGetByBucketResult items = await manager.ScanByPrefix(prefixKeyName, readTimestamp, durability, includeTombstones: true);

        if (items.Type == KeyValueResponseType.Get)
            MergeItems(merged, items.Items);

        IReadOnlyList<string> scanEndpoints = ResolveScanFanOutEndpoints(prefixKeyName);

        Task<KeyValueGetByBucketResult>[] tasks = new Task<KeyValueGetByBucketResult>[scanEndpoints.Count];
        for (int i = 0; i < scanEndpoints.Count; i++)
            tasks[i] = NodeScanByPrefix(scanEndpoints[i], prefixKeyName, readTimestamp, durability, cancellationToken);

        KeyValueGetByBucketResult[] nodeResults = await Task.WhenAll(tasks);

        foreach (KeyValueGetByBucketResult nodeResult in nodeResults)
        {
            if (nodeResult.Type == KeyValueResponseType.Get)
                MergeItems(merged, nodeResult.Items);
        }

        if (durability == KeyValueDurability.Persistent)
        {
            KeyValueGetByBucketResult result = await manager.ScanByPrefixFromDisk(prefixKeyName, readTimestamp, includeTombstones: true);

            if (result.Type == KeyValueResponseType.Get)
                MergeItems(merged, result.Items);
        }

        List<(string, ReadOnlyKeyValueEntry)> unionItems = new(merged.Count);

        foreach ((string key, ReadOnlyKeyValueEntry entry) in merged)
        {
            if (entry.State != KeyValueState.Deleted)
                unionItems.Add((key, entry));
        }

        return new(KeyValueResponseType.Get, unionItems);
    }

    /// <summary>
    /// Resolves the remote nodes a prefix scan must consult. Ephemeral data is leader-local and
    /// leadership moves, so the scan must reach every node that may ever have led the prefix's
    /// partitions. Under legacy full replication (any touched partition with an empty replica
    /// set) that is every peer — today's broadcast, unchanged. Under replica placement only a
    /// partition's replicas can ever have led it, so the fan-out narrows to the union of their
    /// remote endpoints (every role: a replica marked for removal may have led moments ago).
    /// Narrowing is also the correct filter, not just cheaper: a node that lost its replica may
    /// still hold stale leftovers until they are purged, and consulting it would resurface data
    /// that point reads can no longer return. The local node is excluded — its in-memory and
    /// disk contributions are merged separately by the caller.
    /// </summary>
    private IReadOnlyList<string> ResolveScanFanOutEndpoints(string prefixKeyName)
    {
        // Same prefix classification the bucket fan-out uses: an unsplit/hash space routes whole
        // to one partition; a split key-range space touches each of its descriptors' partitions.
        List<int> partitions = [];

        if (RangeRouting.IsPrefixOpSafe(keySpaceRegistry, manager.RangeMapStore.Current, prefixKeyName))
            partitions.Add(routing.RoutePrefixKey(prefixKeyName));
        else
        {
            foreach (RangeDescriptor descriptor in manager.RangeMapStore.Current.FindAll(KeyValueKeySpace.OfPrefix(prefixKeyName)))
                partitions.Add(descriptor.PartitionId);
        }

        string localEndpoint = raft.GetLocalEndpoint();
        HashSet<string> endpoints = new(StringComparer.Ordinal);

        foreach (int partitionId in partitions)
        {
            IReadOnlyList<Kommander.System.RaftReplica> replicas = raft.GetPartitionReplicas(partitionId);

            // Legacy full replication: any peer may hold a copy — keep the all-node broadcast. The
            // phantom witnesses of a standalone node are peers in the roster only; they hold no data.
            if (replicas.Count == 0)
            {
                List<string> peers = [];

                foreach (RaftNode node in raft.GetNodes())
                {
                    if (!EmbeddedRaftCommunication.IsWitness(node.Endpoint))
                        peers.Add(node.Endpoint);
                }

                return peers;
            }

            foreach (Kommander.System.RaftReplica replica in replicas)
            {
                if (!string.Equals(replica.Endpoint, localEndpoint, StringComparison.Ordinal))
                    endpoints.Add(replica.Endpoint);
            }
        }

        return [.. endpoints];
    }

    private async Task<KeyValueGetByBucketResult> NodeScanByPrefix(
        string endpoint,
        string prefixKeyName,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        CancellationToken cancellationToken
    )
    {
        return await interNodeCommunication.ScanByPrefix(endpoint, prefixKeyName, readTimestamp, durability, includeTombstones: true, cancellationToken);
    }
}

using Kommander;
using Kommander.System;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Routing;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Routing;

namespace Kahuna.Server.KeyValues.Watch;

/// <summary>
/// Opens key-value watches on this node: resolves a scope to the data partitions that can hold its keys,
/// checks that this node is a replica of each, and registers the watch with the broker. Any replica serves a
/// watch, leader or follower, with no leadership check and no read index: every event is emitted from this
/// node's own ordered apply, so a lagging replica only delivers later.
///
/// <para><b>Range changes.</b> A split or a merge moves keys between partitions, and registering a key space
/// as range-routed moves all of its keys. After every applied range-map entry the service re-resolves each open
/// watch and ends, with <see cref="KeyValueWatchLagReason.RangeMapChanged"/>, every watch whose partition set
/// changed, rather than let it go quiet on a partition that no longer receives its keys.</para>
///
/// <para><b>Resume.</b> A request with a cursor replays each named partition's retained history above the cursor
/// before its live events (see <see cref="KeyValueWatchReplayer"/>); the replay needs the commit admission window, and
/// a partition whose history is gone ends the watch with <see cref="KeyValueWatchFrameType.HistoryUnavailable"/>.</para>
///
/// <para><b>Replica placement.</b> When the replication factor is below the node count, one node can host only
/// some of a scope's partitions. The service then refuses with <see cref="KeyValueResponseType.DoesNotExist"/>
/// and names the replicas of every covered partition, and the client opens one watch per node, each limited to
/// the partitions that node hosts.</para>
/// </summary>
internal sealed class KeyValueWatchService
{
    /// <summary>Frames a watch buffers for a slow reader when the request names no capacity.</summary>
    internal const int DefaultCapacity = 4096;

    // A registration that keeps racing range-map changes gives up and asks the caller to retry.
    private const int MaxRegistrationAttempts = 4;

    private readonly KeyValueWatchBroker broker;

    private readonly IRaft raft;

    private readonly RangeMapStore rangeMapStore;

    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly DataPartitionRouter dataPartitionRouter;

    private readonly ClientEndpointAdvertiser advertiser;

    private readonly KeyValueWatchReplayer? replayer;

    public KeyValueWatchService(
        KeyValueWatchBroker broker,
        IRaft raft,
        RangeMapStore rangeMapStore,
        KeySpaceRegistry keySpaceRegistry,
        ClientEndpointAdvertiser advertiser,
        KeyValueWatchReplayer? replayer = null)
    {
        this.broker = broker;
        this.raft = raft;
        this.rangeMapStore = rangeMapStore;
        this.keySpaceRegistry = keySpaceRegistry;
        this.advertiser = advertiser;
        this.replayer = replayer;
        dataPartitionRouter = new DataPartitionRouter(raft);
    }

    /// <summary>
    /// Opens a watch. On <see cref="KeyValueResponseType.Get"/> the watch is registered on every partition the
    /// request covers before this returns. Refusals:
    /// <list type="bullet">
    /// <item><see cref="KeyValueResponseType.WatchUnsupportedOnEphemeral"/>: the request names ephemeral keys.</item>
    /// <item><see cref="KeyValueResponseType.DoesNotExist"/>: this node is not a replica of a partition the request
    /// covers. The opening's routes name the replicas of each covered partition.</item>
    /// <item><see cref="KeyValueResponseType.MustRetry"/>: the range map has no descriptor for part of the scope yet,
    /// kept changing during the registration, or no longer covers a partition the request named.</item>
    /// </list>
    /// </summary>
    public KeyValueWatchOpening Watch(KeyValueWatchRequest request)
    {
        if (request.Durability != KeyValueDurability.Persistent)
            return Refuse(KeyValueResponseType.WatchUnsupportedOnEphemeral);

        int capacity = request.Capacity > 0 ? request.Capacity : DefaultCapacity;

        for (int attempt = 0; attempt < MaxRegistrationAttempts; attempt++)
        {
            long mapVersion = rangeMapStore.MapVersion;

            int[]? resolved = ResolvePartitions(request.Scope, rangeMapStore.Current);
            if (resolved is null)
                return Refuse(KeyValueResponseType.MustRetry);

            int[]? partitions = request.Partitions is null ? resolved : Restrict(resolved, request.Partitions);
            if (partitions is null)
                return Refuse(KeyValueResponseType.MustRetry);

            foreach (int partitionId in partitions)
            {
                if (!raft.HostsPartition(partitionId))
                    return Refuse(KeyValueResponseType.DoesNotExist, RoutesOf(partitions));
            }

            List<int>? replay = null;
            if (request.Resumes)
            {
                foreach (int partitionId in partitions)
                {
                    if (request.ResumeCursorOf(partitionId) != Kommander.Time.HLCTimestamp.Zero)
                        (replay ??= []).Add(partitionId);
                }
            }

            KeyValueWatch watch = broker.Register(request.Scope, request.KeysOnly, capacity, partitions, resolved, replay);

            // A range-map entry applied between the resolution and the registration re-checked every watch
            // except this one. Re-resolve against the map now in force; a watch registered on the old
            // partition set must not stay open.
            if (rangeMapStore.MapVersion == mapVersion || SamePartitions(resolved, ResolvePartitions(request.Scope, rangeMapStore.Current)))
            {
                // The partitions to replay hold their live frames back from the registration on; each replay hands
                // its partition over to the live stream when it is done.
                if (replay is not null)
                {
                    if (replayer is null)
                        watch.End(KeyValueWatchFrame.ForHistoryUnavailable(replay[0]));
                    else
                        foreach (int partitionId in replay)
                            replayer.Start(watch, partitionId, request.ResumeCursorOf(partitionId), broker.TracksTransactionEnds);
                }

                return new(KeyValueResponseType.Get, watch);
            }

            watch.End(null);
        }

        return Refuse(KeyValueResponseType.MustRetry);
    }

    /// <summary>
    /// The partitions of <paramref name="requested"/>, sorted and distinct, or null when one of them is not in
    /// <paramref name="resolved"/>: the range map moved since the client resolved the scope, so its split of the
    /// partitions across nodes is stale. An empty request is malformed and resolves to null too.
    /// </summary>
    private static int[]? Restrict(int[] resolved, IReadOnlyList<int> requested)
    {
        if (requested.Count == 0)
            return null;

        SortedSet<int> partitions = [];

        for (int i = 0; i < requested.Count; i++)
        {
            if (Array.BinarySearch(resolved, requested[i]) < 0)
                return null;

            partitions.Add(requested[i]);
        }

        return [.. partitions];
    }

    /// <summary>
    /// Where each of <paramref name="partitions"/> is hosted: whether this node hosts it, and the advertised client
    /// endpoints of its replicas, the believed leader first, then the voters, then the other replicas. Under full
    /// replication the committed map lists no replica set, and the leader hint is the only endpoint.
    /// </summary>
    private List<KeyValueWatchRoute> RoutesOf(int[] partitions)
    {
        List<KeyValueWatchRoute> routes = new(partitions.Length);
        string local = advertiser.LocalAdvertised;

        foreach (int partitionId in partitions)
        {
            bool hostedHere = raft.HostsPartition(partitionId);
            List<string> endpoints = [];

            AddEndpoint(endpoints, advertiser.AdvertiseLeaderOf(partitionId), hostedHere, local);

            IReadOnlyList<RaftReplica> replicas = raft.GetPartitionReplicas(partitionId);

            foreach (RaftReplica replica in replicas)
            {
                if (replica.Role == RaftReplicaRole.Voter)
                    AddEndpoint(endpoints, advertiser.Advertise(replica.Endpoint), hostedHere, local);
            }

            foreach (RaftReplica replica in replicas)
            {
                if (replica.Role != RaftReplicaRole.Voter)
                    AddEndpoint(endpoints, advertiser.Advertise(replica.Endpoint), hostedHere, local);
            }

            routes.Add(new(partitionId, hostedHere, endpoints));
        }

        return routes;
    }

    // The committed map can list this node as a replica before it materialized the partition. Never send the
    // client back to a node that just said it does not host the partition.
    private static void AddEndpoint(List<string> endpoints, string endpoint, bool hostedHere, string local)
    {
        if (endpoint.Length == 0 || endpoints.Contains(endpoint))
            return;

        if (!hostedHere && string.Equals(endpoint, local, StringComparison.Ordinal))
            return;

        endpoints.Add(endpoint);
    }

    /// <summary>
    /// Ends every open watch whose partition set the current range map no longer matches. Called after every
    /// applied range-map entry, on the meta partition's apply path; the work is a resolution per open watch.
    /// </summary>
    public void OnRangeMapApplied()
    {
        if (broker.RegistrationCount == 0)
            return;

        RangeMap map = rangeMapStore.Current;

        foreach (KeyValueWatch watch in broker.OpenWatches())
        {
            int[]? partitions = ResolvePartitions(watch.Scope, map);
            if (partitions is not null && SamePartitions(watch.ResolvedPartitions, partitions))
                continue;

            watch.End(KeyValueWatchFrame.ForLagged(-1, KeyValueWatchLagReason.RangeMapChanged, Kommander.Time.HLCTimestamp.Zero));
        }
    }

    /// <summary>
    /// The data partitions that can hold a key of <paramref name="scope"/> under <paramref name="map"/>, sorted
    /// and distinct, or null when part of the scope lies in a range-routed key space the map does not cover yet.
    /// A superset is safe (the broker matches every key against the scope); a missing partition is not.
    /// </summary>
    internal int[]? ResolvePartitions(KeyValueWatchScope scope, RangeMap map)
    {
        SortedSet<int> partitions = [];

        switch (scope.Kind)
        {
            case KeyValueWatchScopeKind.Key:
                if (!RangeRouting.TryLocate(keySpaceRegistry, map, dataPartitionRouter, scope.Value, out int keyPartition))
                    return null;

                partitions.Add(keyPartition);
                break;

            case KeyValueWatchScopeKind.Range:
                if (keySpaceRegistry.GetMode(scope.Value) == RoutingMode.KeyRange)
                {
                    ArraySegment<RangeDescriptor> descriptors = map.FindIntersecting(scope.Value, scope.StartKey, scope.EndKey);
                    if (descriptors.Count == 0)
                        return null;

                    foreach (RangeDescriptor descriptor in descriptors)
                        partitions.Add(descriptor.PartitionId);
                }
                else
                {
                    partitions.Add(DataPartitionRouter.FirstUserPartitionId + HashPlacement.BucketOfKeySpace(scope.Value, dataPartitionRouter.PoolSize));
                }
                break;

            default:
                AddPrefixPartitions(scope.Value, map, partitions);
                break;
        }

        return [.. partitions];
    }

    /// <summary>
    /// The partitions a prefix can reach. A key belongs to the key space before its last <c>'/'</c>, and hash
    /// routing places the key space's placement group, the text before its first <c>'|'</c>.
    /// <list type="bullet">
    /// <item><b>Hash-routed keys.</b> When the prefix contains a <c>'|'</c> and no <c>'/'</c> comes before it,
    /// every key that starts with the prefix has that same placement group, so one partition holds them all
    /// (the shape of a table-group prefix such as <c>"db:t|"</c>). Any other prefix can match keys of many
    /// groups, so every hash partition is covered.</item>
    /// <item><b>Range-routed keys.</b> Every descriptor whose key space can hold a key that starts with the
    /// prefix and whose interval can hold such a key.</item>
    /// </list>
    /// </summary>
    private void AddPrefixPartitions(string prefix, RangeMap map, SortedSet<int> partitions)
    {
        int group = prefix.IndexOf(HashPlacement.GroupSeparator);

        if (group >= 0 && prefix.AsSpan(0, group).IndexOf(HashPlacement.KeySpaceSeparator) < 0)
        {
            partitions.Add(DataPartitionRouter.FirstUserPartitionId + HashPlacement.BucketOfKeySpace(prefix[..group], dataPartitionRouter.PoolSize));
        }
        else
        {
            for (int i = 0; i < dataPartitionRouter.PoolSize; i++)
                partitions.Add(DataPartitionRouter.FirstUserPartitionId + i);
        }

        foreach (RangeDescriptor descriptor in map.Descriptors)
        {
            if (DescriptorCanHoldPrefix(descriptor, prefix))
                partitions.Add(descriptor.PartitionId);
        }
    }

    /// <summary>
    /// Whether a key that starts with <paramref name="prefix"/> can route through <paramref name="descriptor"/>.
    /// Its key space must allow it: either the key space itself starts with the prefix, or the prefix runs past
    /// the key space's separator into the key's last segment. Then the descriptor's interval must allow it.
    /// </summary>
    private static bool DescriptorCanHoldPrefix(RangeDescriptor descriptor, string prefix)
    {
        string keySpace = descriptor.KeySpace;

        if (keySpace.StartsWith(prefix, StringComparison.Ordinal))
            return true;

        bool prefixInsideKeySpace = prefix.Length > keySpace.Length
            && prefix[keySpace.Length] == HashPlacement.KeySpaceSeparator
            && prefix.StartsWith(keySpace, StringComparison.Ordinal)
            && prefix.AsSpan(keySpace.Length + 1).IndexOf(HashPlacement.KeySpaceSeparator) < 0;

        if (!prefixInsideKeySpace)
            return false;

        // Every key with the prefix is at or above the prefix, and below any bound above the prefix that does
        // not itself start with the prefix.
        bool endAbove = descriptor.EndKey is null || string.CompareOrdinal(descriptor.EndKey, prefix) > 0;
        bool startAllows = descriptor.StartKey is null
            || descriptor.StartKey.StartsWith(prefix, StringComparison.Ordinal)
            || string.CompareOrdinal(descriptor.StartKey, prefix) < 0;

        return endAbove && startAllows;
    }

    private static bool SamePartitions(IReadOnlyList<int> current, int[]? resolved)
    {
        if (resolved is null || resolved.Length != current.Count)
            return false;

        for (int i = 0; i < resolved.Length; i++)
        {
            if (resolved[i] != current[i])
                return false;
        }

        return true;
    }

    private static KeyValueWatchOpening Refuse(KeyValueResponseType type, IReadOnlyList<KeyValueWatchRoute>? routes = null)
    {
        KeyValueWatchMetrics.WatchesRefused.Add(1, new KeyValuePair<string, object?>("type", type.ToString()));
        return new(type, null, routes ?? []);
    }
}

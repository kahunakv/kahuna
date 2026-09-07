using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Sequencer;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.Routing;
using Kommander;

namespace Kahuna.Server.Routing;

/// <summary>
/// Builds the scoped routing metadata a client needs to resolve a resource it has never seen: the
/// hash rule for hash-routed key spaces, the descriptor intervals for key-range routed ones, and
/// the advisory leader of each partition.
///
/// <para>
/// Ownership and leadership are published as two separate lists so one kind of change invalidates
/// one kind of answer. A leader election replaces entries in the leader list; a split, merge or
/// move replaces entries in the key-space list.
/// </para>
///
/// <para>
/// Everything here is advisory. A client that resolves a key from this map still sends an ordinary
/// public request, which re-resolves the key, re-applies the live range fence and re-checks
/// leadership, so metadata that is stale or wrong costs a forward and never a wrong result.
/// </para>
/// </summary>
internal sealed class RoutingMetadataService
{
    /// <summary>
    /// Contract version of the payload. A client that does not recognise it falls back to ordinary
    /// server routing rather than guessing at fields it does not understand.
    /// </summary>
    public const int SchemaVersion = 1;

    /// <summary>
    /// Identifier of the key-to-partition placement function (<see cref="HashPlacement"/>). It names
    /// the exact function, not a family: a client that implements a different jump-consistent hash,
    /// the same one over a different digest, or the same digest without the placement-group rule,
    /// would resolve most keys correctly and a few silently to the wrong partition — which reads as
    /// intermittent extra forwarding rather than as a version mismatch.
    /// </summary>
    public const string HashAlgorithm = HashPlacement.AlgorithmIdentifier;

    /// <summary>The character whose last occurrence in a key ends its key space.</summary>
    public static readonly string PrefixSeparator = HashPlacement.KeySpaceSeparator.ToString();

    /// <summary>The character whose first occurrence in a key space ends its placement group.</summary>
    public static readonly string GroupSeparator = HashPlacement.GroupSeparator.ToString();

    private readonly IRaft raft;

    private readonly RangeMapStore rangeMapStore;

    private readonly KeySpaceRegistry keySpaceRegistry;

    private readonly ClientEndpointAdvertiser advertiser;

    public RoutingMetadataService(
        IRaft raft,
        RangeMapStore rangeMapStore,
        KeySpaceRegistry keySpaceRegistry,
        ClientEndpointAdvertiser advertiser
    )
    {
        this.raft = raft;
        this.rangeMapStore = rangeMapStore;
        this.keySpaceRegistry = keySpaceRegistry;
        this.advertiser = advertiser;
    }

    /// <summary>
    /// The routing map as this node has applied it, optionally narrowed to one key space.
    ///
    /// <para>
    /// The descriptor map is read from a single snapshot taken once, and the map version is read
    /// before and after. The store swaps the map wholesale, so an unchanged version across the read
    /// proves no registration, split, merge or move landed inside it. A changed version marks the
    /// answer incoherent instead of publishing a mixture of two maps, and the client re-reads.
    /// </para>
    /// </summary>
    public async Task<KahunaRoutingMetadataResponse> BuildAsync(string? keySpace)
    {
        long versionBefore = rangeMapStore.MapVersion;

        RangeMap map = rangeMapStore.Current;

        KahunaRoutingMetadataResponse response = new()
        {
            Initialized = raft.IsInitialized,
            SchemaVersion = SchemaVersion,
            HashAlgorithm = HashAlgorithm,
            PrefixSeparator = PrefixSeparator,
            GroupSeparator = GroupSeparator,
            HashPoolSize = raft.Configuration.InitialPartitions,
            HashPartitionOffset = DataPartitionRouter.FirstUserPartitionId,
            SequenceStorageKeyFormat = SequenceActor.ReservedPrefix + "{0}",
            ReservedKeyPrefix = ReservedKeys.SystemPrefix,
            LocalEndpoint = advertiser.LocalAdvertised,
            SnapshotVersion = versionBefore
        };

        SortedSet<string> spaces = new(StringComparer.Ordinal);

        if (keySpace is null)
        {
            // The union of both halves: a space may carry descriptors without being registered here
            // (the routing mode is node-local and unreplicated), and a space registered here may have
            // no descriptor yet. Either half alone hides one of those states from the client.
            foreach (string space in map.KeySpaces)
                spaces.Add(space);

            foreach (string space in keySpaceRegistry.RegisteredKeySpaces)
                spaces.Add(space);
        }
        else
            spaces.Add(keySpace);

        foreach (string space in spaces)
        {
            KahunaRoutingKeySpaceResponse entry = new()
            {
                KeySpace = space,
                RoutingMode = keySpaceRegistry.GetMode(space).ToString()
            };

            // Already ordered by start key, ordinally, which is the order the router itself searches.
            foreach (RangeDescriptor descriptor in map.FindAll(space))
                entry.Ranges.Add(new KahunaRoutingRangeResponse
                {
                    StartKey = descriptor.StartKey,
                    EndKey = descriptor.EndKey,
                    PartitionId = descriptor.PartitionId,
                    Generation = descriptor.Generation
                });

            response.KeySpaces.Add(entry);
        }

        await AddLeadersAsync(response);

        response.Coherent = rangeMapStore.MapVersion == versionBefore;

        return response;
    }

    /// <summary>
    /// Adds one entry per partition a client could be routed to: the whole hash pool plus every
    /// partition named by a descriptor, since a split allocates partition ids above the pool. A
    /// partition whose leader is unknown is listed with an empty endpoint rather than omitted, so a
    /// client can tell "no leader known" from "not a partition".
    /// </summary>
    private async Task AddLeadersAsync(KahunaRoutingMetadataResponse response)
    {
        SortedSet<int> partitions = [];

        int poolSize = raft.Configuration.InitialPartitions;

        for (int i = 0; i < poolSize; i++)
            partitions.Add(DataPartitionRouter.FirstUserPartitionId + i);

        foreach (KahunaRoutingKeySpaceResponse space in response.KeySpaces)
            foreach (KahunaRoutingRangeResponse range in space.Ranges)
                partitions.Add(range.PartitionId);

        foreach (int partitionId in partitions)
            response.Leaders.Add(new KahunaPartitionLeaderResponse
            {
                PartitionId = partitionId,
                Endpoint = await ResolveLeaderEndpointAsync(partitionId)
            });
    }

    /// <summary>
    /// The advertised endpoint of the node this one believes leads the partition. For a partition
    /// hosted here the local belief is authoritative enough for an advisory hint; otherwise the
    /// gossiped hint answers, and an unknown leader stays unknown.
    /// </summary>
    private async Task<string> ResolveLeaderEndpointAsync(int partitionId)
    {
        try
        {
            if (await raft.AmILeaderQuickIfHosted(partitionId))
                return advertiser.LocalAdvertised;
        }
        catch (RaftException)
        {
            // A partition id that is not in the committed map at all, or an election still running.
            // Neither is a leader, and neither is worth failing the whole map for.
            return "";
        }

        return advertiser.AdvertiseLeaderOf(partitionId);
    }
}

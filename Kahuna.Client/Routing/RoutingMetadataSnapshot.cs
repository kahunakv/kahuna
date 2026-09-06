using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.Routing;
using Kommander;

namespace Kahuna.Client.Routing;

/// <summary>
/// One coherent view of how the cluster routes resources, parsed from a server's routing metadata
/// and frozen. Replaced wholesale on refresh, so a reader always sees one complete map rather than
/// a mixture of two.
///
/// <para>
/// Everything it answers is advisory. The node that receives the request re-resolves the resource
/// against its own live map, re-applies range fences and re-checks leadership, so a snapshot that
/// has gone stale costs a forward and never a wrong result.
/// </para>
/// </summary>
internal sealed class RoutingMetadataSnapshot
{
    /// <summary>
    /// The only payload version this client implements. A server announcing anything else is not
    /// approximated: the client falls back to learned routes, which are always safe.
    /// </summary>
    private const int SupportedSchemaVersion = 1;

    /// <summary>
    /// The only hash this client implements, named exactly. A near-match would resolve most keys
    /// correctly and a few silently to the wrong partition, which shows up as unexplained forwarding
    /// rather than as the version mismatch it is.
    /// </summary>
    private const string SupportedHashAlgorithm = "kommander.inverse-prefixed-jump-xxh32-v1";

    private readonly int hashPoolSize;

    private readonly int hashPartitionOffset;

    private readonly string sequenceKeyPrefix;

    private readonly string reservedKeyPrefix;

    /// <summary>Key spaces that route by range, with their intervals ordered by start key.</summary>
    private readonly Dictionary<string, RangeInterval[]> rangedSpaces;

    /// <summary>Partition to the advertised endpoint of its believed leader. Absent means unknown.</summary>
    private readonly Dictionary<int, string> leaders;

    /// <summary>Local deadline past which the snapshot must be read again.</summary>
    public long ExpiresAtTicks { get; }

    private RoutingMetadataSnapshot(
        int hashPoolSize,
        int hashPartitionOffset,
        string sequenceKeyPrefix,
        string reservedKeyPrefix,
        Dictionary<string, RangeInterval[]> rangedSpaces,
        Dictionary<int, string> leaders,
        long expiresAtTicks)
    {
        this.hashPoolSize = hashPoolSize;
        this.hashPartitionOffset = hashPartitionOffset;
        this.sequenceKeyPrefix = sequenceKeyPrefix;
        this.reservedKeyPrefix = reservedKeyPrefix;
        this.rangedSpaces = rangedSpaces;
        this.leaders = leaders;
        ExpiresAtTicks = expiresAtTicks;
    }

    public bool IsValidAt(long nowTicks) => nowTicks < ExpiresAtTicks;

    /// <summary>
    /// Builds a snapshot from a server response, or returns null with the reason when the response
    /// cannot be turned into one. Every rejection leaves the client on its existing behaviour; none
    /// of them is filled in by guessing.
    /// </summary>
    public static RoutingMetadataSnapshot? TryCreate(KahunaRoutingMetadataResponse response, TimeSpan lifetime, out string rejection)
    {
        if (!response.Initialized)
        {
            rejection = "not_initialized";
            return null;
        }

        if (!response.Coherent)
        {
            rejection = "incoherent";
            return null;
        }

        if (response.SchemaVersion != SupportedSchemaVersion)
        {
            rejection = "unsupported_schema";
            return null;
        }

        if (!string.Equals(response.HashAlgorithm, SupportedHashAlgorithm, StringComparison.Ordinal)
            || !string.Equals(response.PrefixSeparator, "/", StringComparison.Ordinal)
            || response.HashPoolSize <= 0)
        {
            rejection = "unsupported_hash";
            return null;
        }

        Dictionary<string, RangeInterval[]> ranged = new(StringComparer.Ordinal);

        foreach (KahunaRoutingKeySpaceResponse space in response.KeySpaces)
        {
            if (!string.Equals(space.RoutingMode, "KeyRange", StringComparison.Ordinal))
                continue;

            // A ranged space with no intervals covers nothing. Recording it as ranged-but-empty is
            // what makes a key in it resolve to "unknown" instead of falling through to the hash
            // answer, which would be a different partition entirely.
            RangeInterval[] intervals = new RangeInterval[space.Ranges.Count];

            for (int i = 0; i < intervals.Length; i++)
            {
                KahunaRoutingRangeResponse range = space.Ranges[i];
                intervals[i] = new RangeInterval(range.StartKey, range.EndKey, range.PartitionId);
            }

            // The server sends them ordered, but the client sorts rather than trusting the order: a
            // binary search over an unsorted array answers wrongly instead of not at all.
            Array.Sort(intervals, static (a, b) => CompareStarts(a.StartKey, b.StartKey));

            ranged[space.KeySpace] = intervals;
        }

        Dictionary<int, string> leaders = new(response.Leaders.Count);

        foreach (KahunaPartitionLeaderResponse leader in response.Leaders)
        {
            // An unknown leader is reported with an empty endpoint. Recording it would make the
            // client believe it knows a destination it does not.
            if (!string.IsNullOrEmpty(leader.Endpoint))
                leaders[leader.PartitionId] = leader.Endpoint;
        }

        // The storage-key rule is applied as a prefix, so a format that puts anything after the name
        // is one this client cannot honour. Refusing it leaves sequences on learned routes rather
        // than resolving every one of them to the wrong partition.
        string sequencePrefix = "";

        if (response.SequenceStorageKeyFormat.EndsWith("{0}", StringComparison.Ordinal))
            sequencePrefix = response.SequenceStorageKeyFormat[..^3];
        else if (response.SequenceStorageKeyFormat.Length > 0)
        {
            rejection = "unsupported_sequence_key";
            return null;
        }

        rejection = "";

        return new RoutingMetadataSnapshot(
            response.HashPoolSize,
            response.HashPartitionOffset,
            sequencePrefix,
            response.ReservedKeyPrefix,
            ranged,
            leaders,
            Environment.TickCount64 + (long)lifetime.TotalMilliseconds);
    }

    /// <summary>
    /// The advertised endpoint believed to own <paramref name="resource"/> in
    /// <paramref name="domain"/>, or null when this snapshot cannot resolve it. Null is the answer
    /// for an unknown routing mode, a ranged space with a gap over the key, and a partition with no
    /// known leader — never a guess.
    /// </summary>
    public string? TryResolve(KahunaRoutingDomain domain, string resource)
    {
        if (!TryResolvePartition(domain, resource, out int partitionId))
            return null;

        return leaders.TryGetValue(partitionId, out string? endpoint) ? endpoint : null;
    }

    /// <summary>
    /// The partition <paramref name="resource"/> belongs to, or false when this snapshot cannot
    /// resolve it: an unknown domain, a reserved key, a ranged key space with a gap over the key, or
    /// a sequence whose storage-key rule the server did not publish. Never a guess.
    /// </summary>
    public bool TryResolvePartition(KahunaRoutingDomain domain, string resource, out int partitionId)
    {
        partitionId = 0;

        if (string.IsNullOrEmpty(resource))
            return false;

        // Server-managed records are routed by the subsystem that owns them; a client neither reads
        // nor caches routes under that prefix.
        if (reservedKeyPrefix.Length > 0 && domain != KahunaRoutingDomain.Sequence
            && resource.StartsWith(reservedKeyPrefix, StringComparison.Ordinal))
            return false;

        switch (domain)
        {
            case KahunaRoutingDomain.KeyValue:
                return TryResolveKeyValuePartition(resource, out partitionId);

            case KahunaRoutingDomain.Lock:
                // Locks are hash-routed only: the lock subsystem does not consult the range map, so
                // resolving a lock through range descriptors would answer a different partition.
                partitionId = HashPartition(resource);
                return true;

            case KahunaRoutingDomain.Sequence:
                // A sequence routes by the partition of its storage key, not by a hash of the bare
                // name, so the storage-key rule the server published is applied first.
                if (sequenceKeyPrefix.Length == 0)
                    return false;

                partitionId = HashPartition(sequenceKeyPrefix + resource);
                return true;

            default:
                return false;
        }
    }

    private bool TryResolveKeyValuePartition(string key, out int partitionId)
    {
        int separator = key.LastIndexOf('/');
        string keySpace = separator < 0 ? key : key[..separator];

        if (!rangedSpaces.TryGetValue(keySpace, out RangeInterval[]? intervals))
        {
            partitionId = HashPartition(key);
            return true;
        }

        // Rightmost interval whose start is at or below the key, then a containment check — the same
        // two steps the server's own router takes.
        int lo = 0, hi = intervals.Length - 1, found = -1;

        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);

            if (StartAtOrBelow(intervals[mid].StartKey, key))
            {
                found = mid;
                lo = mid + 1;
            }
            else
                hi = mid - 1;
        }

        if (found < 0 || !intervals[found].Contains(key))
        {
            partitionId = 0;
            return false;
        }

        partitionId = intervals[found].PartitionId;
        return true;
    }

    /// <summary>
    /// The partition a hash-routed resource lands on. Calls the cluster's own hash rather than a
    /// re-implementation of it, so the two cannot drift.
    /// </summary>
    private int HashPartition(string key) =>
        hashPartitionOffset + (int)HashUtils.InversePrefixedHash(key, '/', hashPoolSize);

    private static bool StartAtOrBelow(string? start, string key) =>
        start is null || string.CompareOrdinal(start, key) <= 0;

    private static int CompareStarts(string? left, string? right)
    {
        if (left is null)
            return right is null ? 0 : -1;

        return right is null ? 1 : string.CompareOrdinal(left, right);
    }

    /// <summary>One half-open ordinal interval. A null bound is an open end, not an empty string.</summary>
    private readonly struct RangeInterval(string? startKey, string? endKey, int partitionId)
    {
        public readonly string? StartKey = startKey;

        private readonly string? endKey = endKey;

        public readonly int PartitionId = partitionId;

        public bool Contains(string key)
        {
            if (StartKey is not null && string.CompareOrdinal(key, StartKey) < 0)
                return false;

            return endKey is null || string.CompareOrdinal(key, endKey) < 0;
        }
    }
}

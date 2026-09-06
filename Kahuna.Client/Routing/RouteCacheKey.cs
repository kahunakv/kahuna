using Kahuna.Shared.Routing;

namespace Kahuna.Client.Routing;

/// <summary>
/// The identity of one cached route: a routing domain and the exact resource name.
///
/// <para>
/// The domain is part of the key because the three subsystems have separate name spaces — a lock
/// and a key may both be called <c>"orders/1"</c> and live on different partitions — so a route
/// learned in one must never be served to another.
/// </para>
///
/// <para>
/// The resource is exact. Nothing is coalesced by prefix: two keys that share a prefix today can be
/// separated by a range split tomorrow, and a prefix-keyed entry would then send one of them to a
/// partition that no longer owns it on every request until the entry expired.
/// </para>
/// </summary>
internal readonly struct RouteCacheKey : IEquatable<RouteCacheKey>
{
    public readonly KahunaRoutingDomain Domain;

    public readonly string Resource;

    /// <summary>
    /// Computed once at construction. A lookup hashes the key on the way into the dictionary and
    /// again in every bucket comparison, and hashing a long key is the most expensive part of a
    /// cache hit, so it is paid once per key rather than once per probe.
    /// </summary>
    private readonly int hash;

    public RouteCacheKey(KahunaRoutingDomain domain, string resource)
    {
        Domain = domain;
        Resource = resource;

        // Ordinal: a resource name is an identifier, never linguistic text.
        hash = HashCode.Combine((byte)domain, string.GetHashCode(resource.AsSpan()));
    }

    public bool Equals(RouteCacheKey other) =>
        Domain == other.Domain && string.Equals(Resource, other.Resource, StringComparison.Ordinal);

    public override bool Equals(object? obj) => obj is RouteCacheKey other && Equals(other);

    public override int GetHashCode() => hash;

    /// <summary>Which shard of the cache owns this key. Reuses the stored hash.</summary>
    public int ShardOf(int shardMask) => (hash & 0x7FFFFFFF) & shardMask;
}

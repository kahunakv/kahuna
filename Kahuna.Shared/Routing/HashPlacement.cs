using Kommander;

namespace Kahuna.Shared.Routing;

/// <summary>
/// The one hash-placement rule for everything Kahuna routes by hash: key-value keys, lock
/// resources and sequence storage keys. The server's routers and the client's local resolver both
/// call this class, so the two cannot drift.
///
/// <para>
/// A key belongs to a <b>key space</b>: the prefix before the last <see cref="KeySpaceSeparator"/>
/// (<c>"t:r/0001"</c> → <c>"t:r"</c>; a key with no separator is its own key space). The key space
/// is the unit of buckets, prefix scans and range descriptors, and nothing here changes it.
/// </para>
///
/// <para>
/// A key space belongs to a <b>placement group</b>: the prefix before the first
/// <see cref="GroupSeparator"/> in the key space (<c>"t:r|i:pk"</c> → <c>"t:r"</c>; a key space
/// with no separator is its own group). Hash routing places the <i>group</i>, so every key space
/// that names the same group lands on the same partition. That is how a consumer keeps a table's
/// rows and its index entries on one partition without a registry: the rule is a pure function of
/// the key, identical on every node and in every client, and it needs no replication. A key space
/// without the separator places exactly as it did before the rule existed.
/// </para>
///
/// <para>
/// The digest is Kommander's jump-consistent hash over the UTF-8 bytes of the group
/// (<see cref="HashUtils.ConsistentHash"/>), published to clients under
/// <see cref="AlgorithmIdentifier"/>. The identifier names this exact function; a client that
/// implements anything else must refuse to hash locally rather than approximate it.
/// </para>
/// </summary>
public static class HashPlacement
{
    /// <summary>Ends a key's key space: the prefix before the <i>last</i> occurrence.</summary>
    public const char KeySpaceSeparator = '/';

    /// <summary>Ends a key space's placement group: the prefix before the <i>first</i> occurrence.</summary>
    public const char GroupSeparator = '|';

    /// <summary>
    /// Identifier of the placement function published in routing metadata. It names the exact
    /// function, not a family: the key-space rule, the group rule and the digest together.
    /// </summary>
    public const string AlgorithmIdentifier = "kahuna.placement-group-jump-xxh32-v1";

    /// <summary>The key space of <paramref name="key"/>: the prefix before the last
    /// <see cref="KeySpaceSeparator"/>, or the whole key when it has none.</summary>
    public static ReadOnlySpan<char> KeySpaceOf(ReadOnlySpan<char> key)
    {
        int separator = key.LastIndexOf(KeySpaceSeparator);
        return separator < 0 ? key : key[..separator];
    }

    /// <summary>The placement group of <paramref name="keySpace"/>: the prefix before the first
    /// <see cref="GroupSeparator"/>, or the whole key space when it has none.</summary>
    public static ReadOnlySpan<char> GroupOf(ReadOnlySpan<char> keySpace)
    {
        int separator = keySpace.IndexOf(GroupSeparator);
        return separator < 0 ? keySpace : keySpace[..separator];
    }

    /// <summary>String form of <see cref="GroupOf(ReadOnlySpan{char})"/>: returns
    /// <paramref name="keySpace"/> itself (no copy) when it names no group.</summary>
    public static string GroupOf(string keySpace)
    {
        int separator = keySpace.IndexOf(GroupSeparator);
        return separator < 0 ? keySpace : keySpace[..separator];
    }

    /// <summary>
    /// The bucket in <c>[0, poolSize)</c> that <paramref name="key"/>'s placement group hashes to.
    /// Allocates only when the group is a proper prefix of the key (the digest takes a string);
    /// a key that is its own group is hashed in place.
    /// </summary>
    public static int BucketOfKey(string key, int poolSize)
    {
        ReadOnlySpan<char> group = GroupOf(KeySpaceOf(key));
        return HashUtils.ConsistentHash(group.Length == key.Length ? key : group.ToString(), poolSize);
    }

    /// <summary>The bucket in <c>[0, poolSize)</c> that <paramref name="keySpace"/>'s placement group
    /// hashes to. Same allocation profile as <see cref="BucketOfKey"/>.</summary>
    public static int BucketOfKeySpace(string keySpace, int poolSize)
    {
        ReadOnlySpan<char> group = GroupOf(keySpace.AsSpan());
        return HashUtils.ConsistentHash(group.Length == keySpace.Length ? keySpace : group.ToString(), poolSize);
    }
}

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The key space a key or a prefix belongs to. A key space is the text of a key before its last <c>'/'</c>
/// (<c>"doctors/1"</c> → <c>"doctors"</c>). It is the unit the key-value actor ring shards by, the bucket a resident
/// entry is indexed under, and the name a prefix lock or a range lock is recorded under.
///
/// <para>A prefix handed to a bucket scan, a prefix lock, or a range lock names a key space either bare
/// (<c>"doctors"</c>) or with a trailing slash (<c>"doctors/"</c>). Both spellings must resolve to the same key
/// space: the same actor, the same lock record, and the same intent set. Otherwise a scan lands on an actor that
/// never receives the key space's commit notifications and serves a resident copy that can only go stale, and a
/// prefix lock is recorded under a name the write path never looks up.</para>
/// </summary>
internal static class KeyValueKeySpace
{
    /// <summary>The key space of a full key: the text before its last <c>'/'</c>, or null when the key has no
    /// separator (such a key is its own key space for routing but is indexed under no bucket).</summary>
    public static string? OfKey(string key)
    {
        int index = key.LastIndexOf('/');
        return index == -1 ? null : key[..index];
    }

    /// <summary>The key space a scan or lock prefix names: the prefix without one trailing <c>'/'</c>. A bare
    /// prefix is returned as-is without allocating.</summary>
    public static string OfPrefix(string prefix) =>
        prefix.Length > 0 && prefix[^1] == '/' ? prefix[..^1] : prefix;
}

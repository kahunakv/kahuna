using System.Collections.Concurrent;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The per-node registry of which key spaces route by key-range vs hash (per-key-space opt-in).
/// Default is <see cref="RoutingMode.Hash"/>; a key space becomes <see cref="RoutingMode.KeyRange"/>
/// when the installed range map holds a descriptor for it (opt-in seeds <c>{tableId}:r</c> /
/// <c>{tableId}:i:{indexId}</c>; <c>{db}/meta</c> is never registered). The registry is the
/// routing-mode projection of that map: <see cref="RangeMapStore"/> reconciles it through
/// <see cref="ReconcileTo"/> on every map it installs, so an explicit <see cref="RegisterKeyRange"/>
/// only brings a space forward until the next install.
///
/// <para>
/// A <b>key space</b> is the prefix of a key up to (and excluding) its last <c>'/'</c> — the same
/// boundary Kommander hashes in <c>InversePrefixedStaticHash(key, '/')</c>. So <c>"t:r/0001"</c> →
/// <c>"t:r"</c>, <c>"t:i:5/abc"</c> → <c>"t:i:5"</c>, <c>"mydb/meta/orders"</c> → <c>"mydb/meta"</c>.
/// </para>
/// </summary>
internal sealed class KeySpaceRegistry
{
    private readonly ConcurrentDictionary<string, RoutingMode> modes = new(StringComparer.Ordinal);

    /// <summary>Span keyed view of <see cref="modes"/> so a key's mode can be looked up without
    /// allocating the key-space substring (the ordinal comparer supports span lookups).</summary>
    private readonly ConcurrentDictionary<string, RoutingMode>.AlternateLookup<ReadOnlySpan<char>> modesBySpan;

    public KeySpaceRegistry()
    {
        modesBySpan = modes.GetAlternateLookup<ReadOnlySpan<char>>();
    }

    /// <summary>Marks <paramref name="keySpace"/> as key-range routed (idempotent).</summary>
    /// <exception cref="ArgumentException">
    /// <paramref name="keySpace"/> ends with <c>/meta</c> — schema-log spaces must stay hash-routed.
    /// </exception>
    public void RegisterKeyRange(string keySpace)
    {
        EnsureRegistrable(keySpace);
        modes[keySpace] = RoutingMode.KeyRange;
    }

    /// <summary>The validity rules of <see cref="RegisterKeyRange"/> alone, so a caller that seeds a space's
    /// descriptor before it flips the mode can reject an invalid space before anything replicates.</summary>
    /// <exception cref="ArgumentException">
    /// <paramref name="keySpace"/> is empty, or ends with <c>/meta</c> — schema-log spaces must stay hash-routed.
    /// </exception>
    public static void EnsureRegistrable(string keySpace)
    {
        if (string.IsNullOrEmpty(keySpace))
            throw new ArgumentException("Key space must be non-empty.", nameof(keySpace));

        if (keySpace.EndsWith("/meta", StringComparison.Ordinal))
            throw new ArgumentException(
                $"Key space '{keySpace}' is a schema-log space and must not be registered as key-range.",
                nameof(keySpace));
    }

    /// <summary>
    /// Reconciles the registry to exactly the key spaces that hold a descriptor in <paramref name="map"/>.
    /// Spaces of the map are registered (idempotent); spaces registered here that the map does not cover
    /// are removed. This keeps the registry a projection of the installed range map:
    /// <c>registry == {range-map key spaces}</c>.
    /// <para>
    /// <see cref="RangeMapStore"/> calls this under its install lock, <b>before</b> it publishes the map.
    /// A route reads the mode and then the map, so a reader that finds a space's descriptor in
    /// <see cref="RangeMapStore.Current"/> must already find the space key-range routed: read the other
    /// way round, the reader hash-routes a key of a seeded space to a partition the rest of the cluster
    /// does not use for it, and the write lands where no range read will find it. The reverse window,
    /// mode flipped and map not yet swapped, refuses the route retryably (no descriptor covers the key)
    /// and lasts only until the swap.
    /// </para>
    /// </summary>
    public void ReconcileTo(RangeMap map)
    {
        foreach (string keySpace in map.KeySpaces)
        {
            if (!string.IsNullOrEmpty(keySpace))
                modes[keySpace] = RoutingMode.KeyRange;
        }

        foreach (string keySpace in modes.Keys)
        {
            if (!map.ContainsKeySpace(keySpace))
                modes.TryRemove(keySpace, out _);
        }
    }

    /// <summary>
    /// Snapshot of every key space this node routes by key range. Unregistered spaces are absent
    /// (they are <see cref="RoutingMode.Hash"/> by omission, not by entry), so this is exactly the
    /// set a reader needs to spot a space that was registered here but has no descriptor yet.
    /// </summary>
    public IReadOnlyCollection<string> RegisteredKeySpaces =>
        modes.Where(static kv => kv.Value == RoutingMode.KeyRange).Select(static kv => kv.Key).ToArray();

    /// <summary>The routing mode for a key space, or <see cref="RoutingMode.Hash"/> if unregistered.</summary>
    public RoutingMode GetMode(string keySpace) =>
        modes.TryGetValue(keySpace, out RoutingMode mode) ? mode : RoutingMode.Hash;

    /// <summary>Span variant of <see cref="GetMode(string)"/> — no key-space substring needed.</summary>
    public RoutingMode GetMode(ReadOnlySpan<char> keySpace) =>
        modesBySpan.TryGetValue(keySpace, out RoutingMode mode) ? mode : RoutingMode.Hash;

    /// <summary>The routing mode for the key space that <paramref name="key"/> belongs to. Resolves
    /// without allocating the key-space substring — the common (unregistered ⇒ Hash) key touches no
    /// heap.</summary>
    public RoutingMode GetModeForKey(string key)
    {
        int separator = key.LastIndexOf('/');
        ReadOnlySpan<char> keySpace = separator < 0 ? key.AsSpan() : key.AsSpan(0, separator);
        return modesBySpan.TryGetValue(keySpace, out RoutingMode mode) ? mode : RoutingMode.Hash;
    }

    /// <summary>
    /// Extracts the key space (prefix before the last <c>'/'</c>) from a raw key. A key with no
    /// <c>'/'</c> is its own key space. Ordinal — no culture-sensitive handling.
    /// </summary>
    public static string ExtractKeySpace(string key)
    {
        int separator = key.LastIndexOf('/');
        return separator < 0 ? key : key[..separator];
    }
}

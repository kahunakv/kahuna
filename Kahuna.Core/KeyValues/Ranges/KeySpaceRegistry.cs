using System.Collections.Concurrent;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The per-node registry of which key spaces route by key-range vs hash (per-key-space opt-in). 
/// Default is <see cref="RoutingMode.Hash"/>; a key space becomes
/// <see cref="RoutingMode.KeyRange"/> only by explicit registration (opt-in registers
/// <c>{tableId}:r</c> / <c>{tableId}:i:{indexId}</c>; <c>{db}/meta</c> is never registered).
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

    /// <summary>Marks <paramref name="keySpace"/> as key-range routed (idempotent).</summary>
    /// <exception cref="ArgumentException">
    /// <paramref name="keySpace"/> ends with <c>/meta</c> — schema-log spaces must stay hash-routed.
    /// </exception>
    public void RegisterKeyRange(string keySpace)
    {
        if (string.IsNullOrEmpty(keySpace))
            throw new ArgumentException("Key space must be non-empty.", nameof(keySpace));

        if (keySpace.EndsWith("/meta", StringComparison.Ordinal))
            throw new ArgumentException(
                $"Key space '{keySpace}' is a schema-log space and must not be registered as key-range.",
                nameof(keySpace));

        modes[keySpace] = RoutingMode.KeyRange;
    }

    /// <summary>
    /// Reconciles the registry to exactly the set of key spaces in <paramref name="live"/>.
    /// Spaces in <paramref name="live"/> are registered (idempotent); spaces currently registered
    /// that are absent from <paramref name="live"/> are removed. This keeps the registry as a
    /// projection of the replicated range map: <c>registry == {range-map key spaces}</c>.
    /// </summary>
    public void ReconcileTo(IReadOnlySet<string> live)
    {
        foreach (string ks in live)
        {
            if (!string.IsNullOrEmpty(ks))
                modes[ks] = RoutingMode.KeyRange;
        }

        foreach (string ks in modes.Keys)
        {
            if (!live.Contains(ks))
                modes.TryRemove(ks, out _);
        }
    }

    /// <summary>The routing mode for a key space, or <see cref="RoutingMode.Hash"/> if unregistered.</summary>
    public RoutingMode GetMode(string keySpace) =>
        modes.TryGetValue(keySpace, out RoutingMode mode) ? mode : RoutingMode.Hash;

    /// <summary>The routing mode for the key space that <paramref name="key"/> belongs to.</summary>
    public RoutingMode GetModeForKey(string key) => GetMode(ExtractKeySpace(key));

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

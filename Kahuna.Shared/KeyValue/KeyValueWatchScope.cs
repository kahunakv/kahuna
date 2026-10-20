namespace Kahuna.Shared.KeyValue;

/// <summary>What a key-value watch covers.</summary>
public enum KeyValueWatchScopeKind
{
    /// <summary>One exact key.</summary>
    Key = 0,

    /// <summary>Every key that starts with a prefix.</summary>
    Prefix = 1,

    /// <summary>Every key of one key space in the half-open interval <c>[StartKey, EndKey)</c>.</summary>
    Range = 2
}

/// <summary>
/// The keys a watch covers: one exact key, a prefix, or a half-open key interval inside one key space. Every
/// comparison is ordinal, as every other key comparison in the store is.
/// </summary>
public sealed record KeyValueWatchScope
{
    public KeyValueWatchScopeKind Kind { get; }

    /// <summary>The exact key (<see cref="KeyValueWatchScopeKind.Key"/>), the prefix
    /// (<see cref="KeyValueWatchScopeKind.Prefix"/>), or the key space the interval lies in
    /// (<see cref="KeyValueWatchScopeKind.Range"/>).</summary>
    public string Value { get; }

    /// <summary>Inclusive lower bound of a range scope; null is unbounded.</summary>
    public string? StartKey { get; }

    /// <summary>Exclusive upper bound of a range scope; null is unbounded.</summary>
    public string? EndKey { get; }

    private KeyValueWatchScope(KeyValueWatchScopeKind kind, string value, string? startKey, string? endKey)
    {
        Kind = kind;
        Value = value;
        StartKey = startKey;
        EndKey = endKey;
    }

    public static KeyValueWatchScope ForKey(string key)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        return new(KeyValueWatchScopeKind.Key, key, null, null);
    }

    public static KeyValueWatchScope ForPrefix(string prefix)
    {
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        return new(KeyValueWatchScopeKind.Prefix, prefix, null, null);
    }

    /// <summary>Every key of <paramref name="keySpace"/> in <c>[startKey, endKey)</c>. The bounds are full keys,
    /// compared ordinally, as a range scan compares them.</summary>
    public static KeyValueWatchScope ForRange(string keySpace, string? startKey, string? endKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(keySpace);

        if (startKey is not null && endKey is not null && string.CompareOrdinal(startKey, endKey) >= 0)
            throw new ArgumentException("The start key must be below the end key.", nameof(startKey));

        // "t:r" and "t:r/" name the same key space, as they do for a range scan.
        if (keySpace[^1] == '/')
            keySpace = keySpace[..^1];

        return new(KeyValueWatchScopeKind.Range, keySpace, startKey, endKey);
    }

    /// <summary>Whether <paramref name="key"/> is inside this scope.</summary>
    public bool Matches(string key)
    {
        switch (Kind)
        {
            case KeyValueWatchScopeKind.Key:
                return string.Equals(key, Value, StringComparison.Ordinal);

            case KeyValueWatchScopeKind.Prefix:
                return key.StartsWith(Value, StringComparison.Ordinal);

            default:
                if (!InKeySpace(key))
                    return false;

                if (StartKey is not null && string.CompareOrdinal(key, StartKey) < 0)
                    return false;

                return EndKey is null || string.CompareOrdinal(key, EndKey) < 0;
        }
    }

    // A key belongs to a key space when the text before its last '/' is the key space.
    private bool InKeySpace(string key)
    {
        int separator = key.LastIndexOf('/');
        ReadOnlySpan<char> keySpace = separator < 0 ? key.AsSpan() : key.AsSpan(0, separator);
        return keySpace.SequenceEqual(Value.AsSpan());
    }
}

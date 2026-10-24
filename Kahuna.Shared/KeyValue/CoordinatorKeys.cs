using System.Globalization;

namespace Kahuna.Shared.KeyValue;

/// <summary>
/// The coordinator key forms that route an interactive session. A plain key is routed by the hash placement
/// rule. A pinned key names the partition the session was started on, so every call that carries the key
/// (start, commit, rollback, operation registration) is routed to that partition and never re-hashed.
///
/// <para>
/// A pinned key is <c>"\u0001p/{partitionId}/{key}"</c>. The leading control character cannot appear in a key
/// a caller composes from text, so no plain key is misread as pinned. The partition travels inside the key
/// because the key already travels on every message that routes to the session; a separate field would have
/// to be threaded through each of them.
/// </para>
///
/// <para>
/// A node that does not know the form hashes the whole string. Its commit and rollback still reach the
/// session through the owner check and the peer probe, but its operation registrations are refused, because
/// registration has no probe. A caller therefore pins sessions only once every node reads pinned keys.
/// </para>
/// </summary>
public static class CoordinatorKeys
{
    /// <summary>The prefix every pinned coordinator key starts with.</summary>
    public const string PinnedPrefix = "\u0001p/";

    /// <summary>Returns the pinned coordinator key that routes <paramref name="key"/> to <paramref name="partitionId"/>.</summary>
    public static string Pin(int partitionId, string key) =>
        string.Create(CultureInfo.InvariantCulture, $"{PinnedPrefix}{partitionId}/{key}");

    /// <summary>
    /// True when <paramref name="coordinatorKey"/> uses the reserved pinned form, well-formed or not. A key in
    /// the reserved form that does not parse must be refused, never hashed as if it were a plain key.
    /// </summary>
    public static bool IsReserved(string? coordinatorKey) =>
        coordinatorKey is { Length: > 0 } && coordinatorKey[0] == PinnedPrefix[0];

    /// <summary>
    /// Reads the partition from a well-formed pinned coordinator key: the reserved prefix, a positive decimal
    /// partition id, a <c>'/'</c>, and a non-empty key. False for a plain key and for a malformed pinned key.
    /// </summary>
    public static bool TryGetPinnedPartition(string? coordinatorKey, out int partitionId)
    {
        partitionId = 0;

        if (!IsReserved(coordinatorKey) || !coordinatorKey!.StartsWith(PinnedPrefix, StringComparison.Ordinal))
            return false;

        ReadOnlySpan<char> rest = coordinatorKey.AsSpan(PinnedPrefix.Length);

        int separator = rest.IndexOf('/');
        if (separator <= 0 || separator == rest.Length - 1)
            return false;

        if (!int.TryParse(rest[..separator], NumberStyles.None, CultureInfo.InvariantCulture, out int parsed) || parsed <= 0)
            return false;

        partitionId = parsed;
        return true;
    }
}

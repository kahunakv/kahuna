
namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Bound helpers for range operations that read the node-global, key-ordered transaction-state
/// stores (prepared intents, canonical records, completion receipts). A range descriptor's null
/// EndKey means "to the end of its key space", not "+infinity": the key-value copy already scopes
/// by key space because it reads through the range scan (whose prefix bounds a null end), but the
/// transaction-state stores are keyed by raw ordinal order across every key space, so a null end
/// there sweeps every key space that sorts above the start key. A split of a key space's tail
/// range then gathers other key spaces' live intents, its settle barrier can never observe an
/// empty range under sustained writes, and the split starves; a completed move would also copy
/// foreign key spaces' records and intents onto the destination partition.
/// </summary>
internal static class KeySpaceBounds
{
    /// <summary>
    /// The end bound a transaction-state read must use for a range of <paramref name="keySpace"/>:
    /// the descriptor's own end when it has one, otherwise the smallest string greater than every
    /// key of the key space — the same substitution the range scan applies to a null end, so the
    /// state gathered always matches the rows the copy moves. Null only for a key space made
    /// entirely of char.MaxValue characters, which cannot occur.
    /// </summary>
    internal static string? MovingEndKey(string keySpace, string? endKey)
    {
        if (endKey is not null)
            return endKey;

        for (int i = keySpace.Length - 1; i >= 0; i--)
        {
            if (keySpace[i] < char.MaxValue)
                return string.Concat(keySpace.AsSpan(0, i), ((char)(keySpace[i] + 1)).ToString());
        }

        return null;
    }
}

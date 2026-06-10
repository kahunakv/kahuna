namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Thread-safe set of key ranges that are currently quiesced for direct (non-2PC) writes during
/// a split window. When a direct <c>TrySet</c> arrives for a key in a quiesced range the locator
/// returns <c>MustRetry</c> before routing — the client retries after the cutover commits and
/// the locator then resolves the new partition.
///
/// <para>
/// The quiesce is range-scoped and short: <see cref="RangeSplitter"/> sets it immediately after
/// acquiring the exclusive range lock (which already quiesces 2PC commits) and releases it in the
/// same <c>finally</c> block that releases the range lock at cutover.
/// </para>
///
/// <para><b>Pre-G limitation.</b> The check is performed pre-route in the locator, so it only
/// intercepts direct writes that arrive on the node currently running the split. A write arriving
/// on a different node bypasses the check. This is an acknowledged limitation: until Phase G
/// (partition-scoped storage) lands, the check is a best-effort guard rather than a hard guarantee.
/// Fully closing the window requires replicating the quiesce state to the data-partition leader so
/// the check can be applied inside the proposal actor.</para>
/// </summary>
internal sealed class RangeQuiesceStore
{
    private volatile int count;
    private readonly List<(string KeySpace, string? StartKey, string? EndKey)> active = [];
    private readonly Lock sync = new();

    /// <summary>True when no ranges are currently quiesced (fast O(1) read without locking).</summary>
    public bool IsEmpty => Volatile.Read(ref count) == 0;

    /// <summary>Marks <c>[startKey, endKey)</c> within <paramref name="keySpace"/> as quiesced.</summary>
    public void Quiesce(string keySpace, string? startKey, string? endKey)
    {
        lock (sync)
        {
            active.Add((keySpace, startKey, endKey));
            Volatile.Write(ref count, active.Count);
        }
    }

    /// <summary>Removes the quiesce for <c>[startKey, endKey)</c> within <paramref name="keySpace"/>.</summary>
    public void Release(string keySpace, string? startKey, string? endKey)
    {
        lock (sync)
        {
            active.RemoveAll(q =>
                q.KeySpace == keySpace && q.StartKey == startKey && q.EndKey == endKey);
            Volatile.Write(ref count, active.Count);
        }
    }

    /// <summary>
    /// Returns true if <paramref name="key"/> falls within any currently quiesced range.
    /// O(1) fast path when no ranges are active; O(q) otherwise (q is typically 0 or 1).
    /// </summary>
    public bool IsQuiesced(string key)
    {
        if (IsEmpty) return false;

        string keySpace = KeySpaceRegistry.ExtractKeySpace(key);

        lock (sync)
        {
            foreach ((string ks, string? start, string? end) in active)
            {
                if (!string.Equals(ks, keySpace, StringComparison.Ordinal)) continue;
                if (start is not null && string.CompareOrdinal(key, start) < 0) continue;
                if (end   is not null && string.CompareOrdinal(key, end)   >= 0) continue;
                return true;
            }
        }

        return false;
    }
}

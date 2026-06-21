
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Per-partition, in-memory write-frequency tracker used exclusively to locate the
/// write centroid for a load-based range split (K1b).
///
/// <para>
/// Design properties:
/// <list type="bullet">
///   <item><b>Lock-free writes.</b> <see cref="RecordWrite"/> uses
///     <see cref="ConcurrentDictionary{TKey,TValue}"/> + <see cref="Interlocked.Add"/>
///     so the hot path on every proposal actor is contention-free.</item>
///   <item><b>Bounded.</b> Capped at <see cref="MaxEntries"/> keys; once full, the entry
///     with the lowest count is evicted to make room (prevents unbounded growth on high-cardinality
///     key spaces while preserving the most-frequently-written keys).</item>
///   <item><b>Decaying.</b> <see cref="Decay"/> halves all counts and removes zero entries;
///     called by the periodic split-checker pass so the histogram reflects recent load, not
///     lifetime cumulative counts.</item>
///   <item><b>Leadership-failover aware.</b> <see cref="Clear"/> wipes the histogram when the
///     range-partition leadership changes; the new leader falls back to the count-based median
///     split until the histogram re-warms (one half-life).</item>
///   <item><b>Transfer on split.</b> <see cref="FilterForChild"/> returns a new tracker seeded
///     with only the entries that fall within a child range's key bounds, so each child starts
///     with a warm histogram rather than rebuilding from zero.</item>
/// </list>
/// </para>
/// </summary>
internal sealed class KeyWriteFrequencyTracker
{
    /// <summary>Maximum distinct keys tracked. Entries beyond this cap are evicted (lowest count first).</summary>
    private const int MaxEntries = 4_096;

    private readonly ConcurrentDictionary<string, long> _counts = new(StringComparer.Ordinal);

    /// <summary>
    /// Records one write for <paramref name="key"/>. Lock-free; safe to call from any thread.
    /// </summary>
    public void RecordWrite(string key)
    {
        _counts.AddOrUpdate(key, 1L, static (_, v) => v + 1);

        if (_counts.Count <= MaxEntries)
            return;

        // Over budget: evict the entry with the lowest count.
        // Linear scan is acceptable — this path is taken at most once per write when near capacity.
        string? evict = null;
        long minCount = long.MaxValue;

        foreach (KeyValuePair<string, long> pair in _counts)
        {
            if (pair.Value < minCount)
            {
                minCount = pair.Value;
                evict = pair.Key;
            }
        }

        if (evict is not null)
            _counts.TryRemove(evict, out _);
    }

    /// <summary>
    /// Halves all counts and removes any entry that decays to zero or below.
    /// Called by the periodic split-checker so the histogram reflects recent load,
    /// not lifetime cumulative totals.
    /// </summary>
    public void Decay()
    {
        List<string>? toRemove = null;

        foreach (string key in _counts.Keys)
        {
            long next = _counts.AddOrUpdate(key, 0L, static (_, v) => v >> 1);
            if (next <= 0)
                (toRemove ??= []).Add(key);
        }

        if (toRemove is null)
            return;

        foreach (string key in toRemove)
            _counts.TryRemove(key, out _);
    }

    /// <summary>
    /// Wipes the histogram. Must be called when this node loses leadership of the partition so
    /// the stale in-memory counts are not used by the next leader after re-election.
    /// </summary>
    public void Clear() => _counts.Clear();

    /// <summary>Returns true when no write has been recorded (e.g. immediately after construction or <see cref="Clear"/>).</summary>
    public bool IsEmpty => _counts.IsEmpty;

    /// <summary>Returns the number of tracked keys.</summary>
    public int Count => _counts.Count;

    /// <summary>
    /// Returns a snapshot of the current write-frequency map. The snapshot is a point-in-time
    /// copy; concurrent <see cref="RecordWrite"/> calls after the snapshot are not reflected.
    /// Used by <see cref="RangeSplitPolicy"/> to compute the write centroid.
    /// </summary>
    public IReadOnlyDictionary<string, long> GetSnapshot() =>
        new Dictionary<string, long>(_counts, StringComparer.Ordinal);

    /// <summary>
    /// Returns a new <see cref="KeyWriteFrequencyTracker"/> seeded with only the entries whose
    /// key falls within <c>[<paramref name="startKey"/>, <paramref name="endKey"/>)</c>.
    /// Null bounds are treated as −∞ and +∞ respectively, matching <see cref="RangeDescriptor"/>
    /// conventions.
    ///
    /// <para>
    /// Called immediately after a split completes to seed the two child trackers from the parent,
    /// so each child has a warm histogram from the moment it starts accepting writes.
    /// Any bucket that straddles the split key is dropped; it re-warms in the next half-life.
    /// </para>
    /// </summary>
    public KeyWriteFrequencyTracker FilterForChild(string? startKey, string? endKey)
    {
        KeyWriteFrequencyTracker child = new();

        foreach (KeyValuePair<string, long> pair in _counts)
        {
            bool afterStart = startKey is null ||
                              string.Compare(pair.Key, startKey, StringComparison.Ordinal) >= 0;
            bool beforeEnd = endKey is null ||
                             string.Compare(pair.Key, endKey, StringComparison.Ordinal) < 0;

            if (afterStart && beforeEnd && pair.Value > 0)
                child._counts[pair.Key] = pair.Value;
        }

        return child;
    }
}

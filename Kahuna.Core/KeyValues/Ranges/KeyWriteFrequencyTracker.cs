
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
/// write centroid for a load-based range split.
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

    /// <summary>
    /// Number of entries inspected when choosing an eviction victim. Sampling replaces the former
    /// O(MaxEntries) full scan: we read a small window of consecutive entries and evict the minimum
    /// among them. Enumeration position in the dictionary is uncorrelated with write count, so a
    /// consecutive window is as representative as a strided one — and stays O(EvictionSampleSize).
    /// Hot keys survive because they are almost never the minimum of a small random sample.
    /// </summary>
    private const int EvictionSampleSize = 16;

    private readonly ConcurrentDictionary<string, long> _counts = new(StringComparer.Ordinal);

    /// <summary>Approximate live entry count; updated with Interlocked to avoid frequent ConcurrentDictionary.Count reads.</summary>
    private int _approxCount;

    /// <summary>Rolling counter used to vary the sample start position across successive evictions.</summary>
    private int _evictCounter;

    /// <summary>
    /// Records one write for <paramref name="key"/>. Lock-free; safe to call from any thread.
    /// </summary>
    public void RecordWrite(string key)
    {
        // TryAdd detects a genuine first insert without the per-call closure allocation an
        // AddOrUpdate add-factory would incur, and is precise under races: only the thread that
        // actually inserts takes the add path, so _approxCount cannot over-count.
        if (!_counts.TryAdd(key, 1L))
        {
            // Existing key (or a lost insert race): increment its count.
            _counts.AddOrUpdate(key, 1L, static (_, v) => v + 1);
            return;
        }

        if (Interlocked.Increment(ref _approxCount) <= MaxEntries)
            return;

        // Over budget: evict the lowest-count key from a small window of entries. Truly
        // O(EvictionSampleSize) — at most a bounded rotating offset plus the sample window are
        // enumerated, never the whole dictionary. The offset varies the region across successive
        // evictions; hot keys survive because they are almost never the minimum of the window.
        int counter = Interlocked.Increment(ref _evictCounter);
        int skip = (counter & int.MaxValue) % EvictionSampleSize;

        string? evict = null;
        long minCount = long.MaxValue;
        int seen = 0;

        foreach (KeyValuePair<string, long> pair in _counts)
        {
            if (skip > 0)
            {
                skip--;
                continue;
            }

            if (pair.Value < minCount)
            {
                minCount = pair.Value;
                evict = pair.Key;
            }

            if (++seen >= EvictionSampleSize)
                break;
        }

        if (evict is not null && _counts.TryRemove(evict, out _))
            Interlocked.Decrement(ref _approxCount);
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

        // Decay already enumerates the whole map, so reconcile the approximate counter with the exact
        // size here. This bounds any RecordWrite drift (concurrent first-insert races can over-count)
        // to a single decay interval.
        Interlocked.Exchange(ref _approxCount, _counts.Count);
    }

    /// <summary>
    /// Wipes the histogram. Must be called when this node loses leadership of the partition so
    /// the stale in-memory counts are not used by the next leader after re-election.
    /// </summary>
    public void Clear()
    {
        _counts.Clear();
        Interlocked.Exchange(ref _approxCount, 0);
    }

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
            {
                child._counts[pair.Key] = pair.Value;
                child._approxCount++;
            }
        }

        return child;
    }
}


/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// Node-wide registry that maps Raft partition IDs to their
/// <see cref="KeyWriteFrequencyTracker"/> instances (K1b).
///
/// <para>
/// One tracker per partition is created on first write and lives until the partition is
/// removed (split or merged). The registry is a shared singleton: proposal actors record
/// writes into it; the split trigger reads snapshots from it to locate the write centroid.
/// </para>
/// </summary>
internal sealed class KeyWriteFrequencyRegistry
{
    private readonly ConcurrentDictionary<int, KeyWriteFrequencyTracker> _trackers = new();

    /// <summary>
    /// Returns the tracker for <paramref name="partitionId"/>, creating it on first access.
    /// </summary>
    public KeyWriteFrequencyTracker GetOrCreate(int partitionId) =>
        _trackers.GetOrAdd(partitionId, static _ => new KeyWriteFrequencyTracker());

    /// <summary>
    /// Returns the tracker for <paramref name="partitionId"/>, or <c>null</c> if no writes
    /// have been recorded for that partition yet.
    /// </summary>
    public KeyWriteFrequencyTracker? TryGet(int partitionId) =>
        _trackers.TryGetValue(partitionId, out KeyWriteFrequencyTracker? t) ? t : null;

    /// <summary>
    /// Removes the tracker for <paramref name="partitionId"/>.
    /// Called after a split or merge so stale tracker memory is released.
    /// </summary>
    public void Remove(int partitionId) => _trackers.TryRemove(partitionId, out _);

    /// <summary>
    /// Replaces the tracker for <paramref name="partitionId"/> with <paramref name="tracker"/>.
    /// Used during split transfer to install a pre-filtered child tracker atomically.
    /// </summary>
    public void Replace(int partitionId, KeyWriteFrequencyTracker tracker) =>
        _trackers[partitionId] = tracker;

    /// <summary>
    /// Returns all (partitionId, tracker) pairs currently registered.
    /// Used by the periodic split checker to decay all trackers in one pass.
    /// </summary>
    public IEnumerable<KeyValuePair<int, KeyWriteFrequencyTracker>> All => _trackers;
}

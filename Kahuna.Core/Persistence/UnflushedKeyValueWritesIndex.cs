
using System.Collections.Concurrent;
using Kahuna.Server.KeyValues;
using Kommander.Time;

namespace Kahuna.Server.Persistence;

/// <summary>
/// A committed key-value write recorded at the moment it was queued for background persistence,
/// held until the flush that contains it (or a newer head for the same key) is confirmed.
/// </summary>
internal readonly record struct UnflushedKeyValueWrite(
    byte[]? Value,
    long Revision,
    HLCTimestamp Expires,
    HLCTimestamp LastUsed,
    HLCTimestamp LastModified,
    KeyValueState State,
    bool NoRevision);

/// <summary>
/// Node-local overlay of committed key-value writes that have been queued for the background writer
/// but not yet confirmed flushed to the persistence backend. Between the Raft apply and the periodic
/// flush, the backend is <b>behind the node's commit frontier</b>: a read that misses the actor cache
/// and consults only the backend would conclude a durably committed key does not exist — most visibly
/// on a freshly promoted partition leader, whose actor cache never materialised entries it applied as
/// a follower (a non-resident <c>InvalidateOrApply</c> is a deliberate no-op).
///
/// <para>
/// Producers record here <b>synchronously, before</b> sending the <c>QueueStoreKeyValue</c> message —
/// recording inside the writer actor would leave a mailbox-latency window (unbounded while a flush is
/// in progress) during which the committed write is invisible to both cache and backend. Entries are
/// removed by <see cref="UnflushedOverlayPersistenceBackend"/> when a confirmed flush carries a head
/// at or beyond the recorded one; a failed flush retains its items, so the overlay keeps covering
/// them. Memory is bounded by the flush cadence: the overlay never holds more than the writer's
/// dirty queue does.
/// </para>
/// </summary>
internal sealed class UnflushedKeyValueWritesIndex
{
    private readonly ConcurrentDictionary<string, UnflushedKeyValueWrite> entries = new(StringComparer.Ordinal);

    /// <summary>True when the overlay currently holds no unflushed writes (fast path for reads).</summary>
    public bool IsEmpty => entries.IsEmpty;

    /// <summary>
    /// Records a committed write queued for persistence. Keeps the newest head per key: same-revision
    /// records (delete/extend legitimately reuse a revision number) are ordered by commit HLC.
    /// </summary>
    public void Record(
        string key, byte[]? value, long revision,
        HLCTimestamp expires, HLCTimestamp lastUsed, HLCTimestamp lastModified,
        KeyValueState state, bool noRevision)
    {
        UnflushedKeyValueWrite incoming = new(value, revision, expires, lastUsed, lastModified, state, noRevision);

        entries.AddOrUpdate(
            key,
            incoming,
            (_, existing) => IsNewer(existing, incoming.Revision, incoming.LastModified) ? existing : incoming);
    }

    /// <summary>
    /// Removes the overlay entry for <paramref name="key"/> after a confirmed flush, unless a strictly
    /// newer head was queued meanwhile — that newer head is still unflushed and must stay covered.
    /// </summary>
    public void RemoveFlushed(string key, long flushedRevision, HLCTimestamp flushedLastModified)
    {
        while (entries.TryGetValue(key, out UnflushedKeyValueWrite current))
        {
            if (IsNewer(current, flushedRevision, flushedLastModified))
                return;

            // Atomic conditional removal: only removes when the stored value is still `current`,
            // so a concurrent Record of a newer head is never lost.
            if (entries.TryRemove(new KeyValuePair<string, UnflushedKeyValueWrite>(key, current)))
                return;
        }
    }

    public bool TryGet(string key, out UnflushedKeyValueWrite write) => entries.TryGetValue(key, out write);

    /// <summary>
    /// Snapshot of overlay entries whose key starts with <paramref name="prefix"/> and, when
    /// <paramref name="startKey"/> is given, sorts at or after it (ordinal — matching the backend
    /// scans' inclusive lower-bound seek).
    /// </summary>
    public List<KeyValuePair<string, UnflushedKeyValueWrite>> Collect(string prefix, string? startKey = null)
    {
        List<KeyValuePair<string, UnflushedKeyValueWrite>> matches = [];

        foreach (KeyValuePair<string, UnflushedKeyValueWrite> kv in entries)
        {
            if (!kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                continue;
            if (startKey is not null && string.CompareOrdinal(kv.Key, startKey) < 0)
                continue;
            matches.Add(kv);
        }

        return matches;
    }

    /// <summary>Newest-head ordering: revision first, commit HLC as the same-revision tiebreak.</summary>
    internal static bool IsNewer(in UnflushedKeyValueWrite candidate, long revision, HLCTimestamp lastModified) =>
        candidate.Revision > revision
        || (candidate.Revision == revision && candidate.LastModified > lastModified);
}

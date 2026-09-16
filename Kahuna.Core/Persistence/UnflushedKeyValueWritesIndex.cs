
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

    // Invoked with the key after a confirmed flush removed its overlay entry — i.e. once every queued head of
    // the key is durable. The prepared-intent store releases the settled intents it retains for that key's
    // unflushed materialization on this signal. Runs on the flush path; must be cheap and must not throw.
    private Action<string>? onKeyReleased;

    /// <summary>True when the overlay currently holds no unflushed writes (fast path for reads).</summary>
    public bool IsEmpty => entries.IsEmpty;

    /// <summary>Wires the observer notified whenever a confirmed flush removes a key's overlay entry (manager
    /// construction). One observer; a later attach replaces the earlier one.</summary>
    public void AttachReleaseObserver(Action<string> observer) => onKeyReleased = observer;

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
            {
                onKeyReleased?.Invoke(key);
                return;
            }
        }
    }

    public bool TryGet(string key, out UnflushedKeyValueWrite write) => entries.TryGetValue(key, out write);

    /// <summary>
    /// The overlay entries a scan page can contain, in ordinal key order: keys that start with
    /// <paramref name="prefix"/>, sort at or after <paramref name="startKey"/> when it is given
    /// (inclusive — matching the backend scans' lower-bound seek), and sort at or before
    /// <paramref name="ceilingKey"/> when it is given.
    ///
    /// <para>
    /// The selection is bounded to the <paramref name="limit"/> smallest matching keys. A page is the
    /// first <c>limit</c> keys of the ordinal union of the inner page and the overlay, so an overlay
    /// key outside its own <c>limit</c> smallest can never reach the page — the bound is exact, and it
    /// keeps the temporary storage proportional to the page rather than to the unflushed backlog.
    /// The ceiling is the same argument from the inner side: when the inner page is full, every key
    /// above its last row is displaced by the rows already below it. The enumeration itself still
    /// visits the whole overlay — dictionary order is arbitrary, so no entry can be skipped unseen.
    /// </para>
    /// </summary>
    /// <param name="limit">Page size; a negative value or <see cref="int.MaxValue"/> means unbounded.</param>
    /// <param name="ceilingKey">Largest key that can still enter the page, or null when the inner page
    /// was not full and every larger overlay key remains a candidate.</param>
    public List<KeyValuePair<string, UnflushedKeyValueWrite>> Collect(
        string prefix, string? startKey, int limit, string? ceilingKey)
    {
        bool bounded = limit >= 0 && limit < int.MaxValue;

        // Bounded selection keeps the `limit` smallest keys in a max-heap keyed by the key itself,
        // so each candidate beyond the bound costs one comparison against the current largest.
        PriorityQueue<KeyValuePair<string, UnflushedKeyValueWrite>, string>? heap = null;
        List<KeyValuePair<string, UnflushedKeyValueWrite>>? unbounded = null;

        foreach (KeyValuePair<string, UnflushedKeyValueWrite> kv in entries)
        {
            if (!kv.Key.StartsWith(prefix, StringComparison.Ordinal))
                continue;
            if (startKey is not null && string.CompareOrdinal(kv.Key, startKey) < 0)
                continue;
            if (ceilingKey is not null && string.CompareOrdinal(kv.Key, ceilingKey) > 0)
                continue;

            if (!bounded)
            {
                (unbounded ??= []).Add(kv);
                continue;
            }

            if (limit == 0)
                break;

            heap ??= new(Math.Min(limit, 64), DescendingOrdinal.Instance);

            if (heap.Count < limit)
                heap.Enqueue(kv, kv.Key);
            else if (string.CompareOrdinal(kv.Key, heap.Peek().Key) < 0)
                heap.EnqueueDequeue(kv, kv.Key);
        }

        if (!bounded)
        {
            if (unbounded is null)
                return [];

            unbounded.Sort(static (a, b) => string.CompareOrdinal(a.Key, b.Key));
            return unbounded;
        }

        if (heap is null)
            return [];

        // Drain largest-first into the tail of a pre-sized list, so the result is ascending.
        int count = heap.Count;
        KeyValuePair<string, UnflushedKeyValueWrite>[] ordered = new KeyValuePair<string, UnflushedKeyValueWrite>[count];
        for (int i = count - 1; i >= 0; i--)
            ordered[i] = heap.Dequeue();

        return new List<KeyValuePair<string, UnflushedKeyValueWrite>>(ordered);
    }

    /// <summary>Reverses ordinal order so the priority queue's root is the largest key of the selection.</summary>
    private sealed class DescendingOrdinal : IComparer<string>
    {
        public static readonly DescendingOrdinal Instance = new();

        public int Compare(string? x, string? y) => string.CompareOrdinal(y, x);
    }

    /// <summary>Newest-head ordering: revision first, commit HLC as the same-revision tiebreak.</summary>
    internal static bool IsNewer(in UnflushedKeyValueWrite candidate, long revision, HLCTimestamp lastModified) =>
        candidate.Revision > revision
        || (candidate.Revision == revision && candidate.LastModified > lastModified);
}


using System.Collections.Concurrent;
using Kahuna.Server.Locks.Data;
using Kommander.Time;

namespace Kahuna.Server.Persistence;

/// <summary>
/// A committed lock mutation recorded at the moment it was queued for background persistence, held
/// until the flush that contains it (or a newer head for the same resource) is confirmed.
/// </summary>
internal readonly record struct UnflushedLockWrite(
    byte[]? Owner,
    long FencingToken,
    HLCTimestamp Expires,
    HLCTimestamp LastUsed,
    HLCTimestamp LastModified,
    LockState State);

/// <summary>
/// Node-local overlay of committed lock mutations queued for the background writer but not yet
/// confirmed flushed — the lock analog of <see cref="UnflushedKeyValueWritesIndex"/>. The lock
/// replicator persists follower applies only through the asynchronous <c>QueueStoreLock</c> path
/// (there is no follower cache materialisation at all), so a freshly promoted lock leader that
/// consults its empty actor table and an unflushed backend would treat a held persistent lock as
/// free — and grant it to a second client. Producers record here synchronously before enqueueing;
/// a confirmed flush prunes; ordering is fencing token first (monotonic per acquisition), commit
/// HLC as the tiebreak for the unlock/extend records that reuse the acquire's token.
/// </summary>
internal sealed class UnflushedLockWritesIndex
{
    private readonly ConcurrentDictionary<string, UnflushedLockWrite> entries = new(StringComparer.Ordinal);

    /// <summary>True when the overlay currently holds no unflushed lock mutations.</summary>
    public bool IsEmpty => entries.IsEmpty;

    /// <summary>Records a committed lock mutation queued for persistence, keeping the newest head per resource.</summary>
    public void Record(
        string resource, byte[]? owner, long fencingToken,
        HLCTimestamp expires, HLCTimestamp lastUsed, HLCTimestamp lastModified, LockState state)
    {
        UnflushedLockWrite incoming = new(owner, fencingToken, expires, lastUsed, lastModified, state);

        entries.AddOrUpdate(
            resource,
            incoming,
            (_, existing) => IsNewer(existing, incoming.FencingToken, incoming.LastModified) ? existing : incoming);
    }

    /// <summary>
    /// Removes the overlay entry for <paramref name="resource"/> after a confirmed flush, unless a
    /// strictly newer head was queued meanwhile — that newer head is still unflushed and must stay
    /// covered.
    /// </summary>
    public void RemoveFlushed(string resource, long flushedFencingToken, HLCTimestamp flushedLastModified)
    {
        while (entries.TryGetValue(resource, out UnflushedLockWrite current))
        {
            if (IsNewer(current, flushedFencingToken, flushedLastModified))
                return;

            if (entries.TryRemove(new KeyValuePair<string, UnflushedLockWrite>(resource, current)))
                return;
        }
    }

    public bool TryGet(string resource, out UnflushedLockWrite write) => entries.TryGetValue(resource, out write);

    /// <summary>Newest-head ordering: fencing token first, commit HLC as the same-token tiebreak.</summary>
    internal static bool IsNewer(in UnflushedLockWrite candidate, long fencingToken, HLCTimestamp lastModified) =>
        candidate.FencingToken > fencingToken
        || (candidate.FencingToken == fencingToken && candidate.LastModified > lastModified);
}

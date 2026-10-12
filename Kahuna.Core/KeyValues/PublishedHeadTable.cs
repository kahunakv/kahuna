
using System.Collections.Concurrent;

using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The state one key-value actor shares with off-actor readers: the published head records of its resident keys,
/// the committed heads that are on their way to its mailbox but not yet applied, and whether it holds any prefix
/// lock. One table per actor, found by the same consistent hash that routes a request to the actor, so the actor
/// stays the only writer of the records and the readers never need a lock.
///
/// <para>The pending-apply map closes the ordering gap between Kommander's applied frontier and the actor: a
/// committed mutation is recorded here by the replicator <b>before</b> it is sent to the actor and before the
/// frontier moves, and removed by the actor after it processed that notification. A reader whose record is
/// behind a pending apply goes to the actor, where the notification is drained ahead of the read. The map keeps
/// the newest pending head per key, so a stale replay never hides a newer pending apply, and a pending head the
/// record already reached never blocks.</para>
/// </summary>
internal sealed class PublishedHeadTable
{
    /// <summary>The newest committed head sent to the actor for a key and not yet processed by it.</summary>
    internal readonly record struct PendingApply(long Revision, HLCTimestamp LastModified);

    private readonly ConcurrentDictionary<string, PublishedHead> heads = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, PendingApply> pendingApplies = new(StringComparer.Ordinal);

    // 1 while the actor's prefix-lock table is non-empty. Written by the actor at the end of every turn that
    // changed it; a live prefix lock answers a point read with MustRetry on the actor path, so the reader
    // must defer to the actor while one exists.
    private int prefixLocksHeld;

    /// <summary>Whether the actor held a prefix lock at the end of its last turn.</summary>
    public bool PrefixLocksHeld => Volatile.Read(ref prefixLocksHeld) != 0;

    /// <summary>Number of published records. Observability and tests.</summary>
    public int Count => heads.Count;

    /// <summary>Number of pending applies not yet processed. Observability and tests.</summary>
    public int PendingApplyCount => pendingApplies.Count;

    /// <summary>The live or retracted record for <paramref name="key"/>, when one is published.</summary>
    public bool TryGet(string key, out PublishedHead head) => heads.TryGetValue(key, out head!);

    /// <summary>Publishes or replaces the record for <paramref name="key"/>. Actor thread only.</summary>
    internal void Publish(string key, PublishedHead head) => heads[key] = head;

    /// <summary>Drops the record for <paramref name="key"/>. Actor thread only.</summary>
    internal void Remove(string key) => heads.TryRemove(key, out _);

    /// <summary>Records that the actor's prefix-lock table is non-empty (<paramref name="held"/>) or empty.
    /// Actor thread only; writes only on a change so a quiet turn costs one read.</summary>
    internal void SetPrefixLocksHeld(bool held)
    {
        int value = held ? 1 : 0;
        if (Volatile.Read(ref prefixLocksHeld) != value)
            Volatile.Write(ref prefixLocksHeld, value);
    }

    /// <summary>
    /// Records a committed head that is about to be sent to the actor. Called by the sender, on its own thread,
    /// before the send and before the applied frontier advances. Keeps the newest head per key.
    /// </summary>
    public void NotePendingApply(string key, long revision, HLCTimestamp lastModified)
    {
        PendingApply incoming = new(revision, lastModified);

        pendingApplies.AddOrUpdate(
            key,
            static (_, candidate) => candidate,
            static (_, existing, candidate) => IsNewer(existing, candidate.Revision, candidate.LastModified) ? existing : candidate,
            incoming);
    }

    /// <summary>
    /// Forgets the pending apply for <paramref name="key"/> once the actor processed the notification carrying
    /// exactly that head. A newer pending head noted meanwhile is kept. Actor thread only.
    /// </summary>
    internal void ClearPendingApply(string key, long revision, HLCTimestamp lastModified) =>
        pendingApplies.TryRemove(new KeyValuePair<string, PendingApply>(key, new PendingApply(revision, lastModified)));

    /// <summary>Whether a head newer than (<paramref name="revision"/>, <paramref name="lastModified"/>) is
    /// pending on the actor for <paramref name="key"/>.</summary>
    public bool HasNewerPendingApply(string key, long revision, HLCTimestamp lastModified) =>
        pendingApplies.TryGetValue(key, out PendingApply pending) && IsNewer(pending, revision, lastModified);

    private static bool IsNewer(in PendingApply pending, long revision, HLCTimestamp lastModified) =>
        pending.Revision > revision || (pending.Revision == revision && pending.LastModified > lastModified);
}


using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Answers a latest point get from the owning actor's published head record, on the caller's thread, when the
/// record proves the actor would answer the same: the key is resident and quiet, nothing newer is on its way to
/// the actor, and no overlay the actor consults holds a newer or undecided version of the key. Any doubt answers
/// false and the read takes the actor path, which is authoritative for every case.
///
/// <para>The guards, in order, and what each one stands in for on the actor path:</para>
/// <list type="bullet">
///   <item>Not a by-revision read, which needs the archive. Not inside an actor turn: a request rented there
///   is served inline, and a pin sent from inside the turn would land after the turn's own later requests for
///   the key.</item>
///   <item>A snapshot read only when its timestamp is at or above the head's commit time, and it is either
///   outside any transaction or a statement read. The actor answers such a read from the head itself, once the
///   same guards as below have passed, and changes nothing but the last-used stamp: it records no pin, and a
///   commit floor is recorded only on a staged intent, which a live record rules out. A snapshot below the
///   head needs the archive, and a transactional snapshot read in the default mode is left to the actor. So
    ///   is a timestamp at or ahead of this node's clock, which the actor folds into the clock before it serves
    ///   the read, and a timestamp that an installed partition or a moved range may not hold the history for.</item>
///   <item>A live record: the actor retracts it before it changes the head or plants an intent, so a live
///   record is a complete head with no replication intent, no write intent, and no parked committed head. The
///   actor's prefix-lock table must be empty, as a live prefix lock answers the read with MustRetry.</item>
///   <item>No committed head pending on the actor's mailbox for the key (the replicator notes one before the
///   applied frontier advances), and, for a persistent key, no newer head in the unflushed-writes overlay and no
///   durable prepared intent (a committed-but-unsettled value is served through the intent overlay).</item>
///   <item>A transactional read needs an entry with no MVCC dictionary, so this transaction cannot already hold
///   a pin or a staged write that its own read must see. The pin the actor path would record for a latest read
///   is sent as a control message that lands before the transaction's next request for the key. A statement
///   read leaves no pin on either path.</item>
///   <item>The head must not be expired: an expired head is a tombstone the actor converts and reports.</item>
/// </list>
/// </summary>
internal sealed class PublishedHeadReader
{
    private readonly KeyValueActorRing ephemeralRing;

    private readonly KeyValueActorRing persistentRing;

    // Committed writes queued for the background writer (persistent keys only; null when the backend carries no
    // overlay), and the durable prepared intents (the ephemeral actors' stores are always empty, so only the
    // persistent ring consults it).
    private readonly UnflushedKeyValueWritesIndex? unflushedWrites;

    private readonly PreparedIntentStore preparedIntentStore;

    private readonly IRaft raft;

    // What bounds the history this node can answer a snapshot from: the partitions whose state came from an install,
    // and the ranges moved by a split or merge. Null in a rig that has neither.
    private readonly InstalledHistoryFence? installedHistory;

    private readonly RangeMapStore? rangeMaps;

    private readonly bool enabled;

    internal PublishedHeadReader(
        KeyValueActorRing ephemeralRing,
        KeyValueActorRing persistentRing,
        UnflushedKeyValueWritesIndex? unflushedWrites,
        PreparedIntentStore preparedIntentStore,
        IRaft raft,
        bool enabled,
        InstalledHistoryFence? installedHistory = null,
        RangeMapStore? rangeMaps = null)
    {
        this.installedHistory = installedHistory;
        this.rangeMaps = rangeMaps;
        this.ephemeralRing = ephemeralRing;
        this.persistentRing = persistentRing;
        this.unflushedWrites = unflushedWrites;
        this.preparedIntentStore = preparedIntentStore;
        this.raft = raft;
        this.enabled = enabled;
    }

    /// <summary>
    /// Tries to answer the read from the published record. True with the entry when it did; false when the read
    /// must take the actor path.
    /// </summary>
    public bool TryRead(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        KeyValueReadMode readMode,
        out ReadOnlyKeyValueEntry? entry)
    {
        entry = null;

        if (!enabled || revision != -1 || KeyValueInlineScope.Current is not null)
            return false;

        bool transactional = transactionId != HLCTimestamp.Zero;
        bool snapshot = !readTimestamp.IsNull();

        if (snapshot && transactional && !KeyValueReadModeChecks.IsStatementRead(readMode, transactionId, readTimestamp))
            return false;

        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        if (snapshot)
        {
            // The actor serves a snapshot read only behind two checks of its own, made before any handler runs.
            // This node's clock must be past the timestamp, so that no commit staged here afterwards is stamped at
            // or below it: a timestamp at or ahead of the clock is left to the actor, which folds it into the
            // clock or refuses it. And the node must hold the history the timestamp needs: any timestamp an
            // install or a moved range could bound is left to the actor, which decides it per partition and range.
            if (readTimestamp.CompareTo(currentTime) >= 0)
                return false;

            if (durability == KeyValueDurability.Persistent && installedHistory is not null
                && (installedHistory.MayRefuse(readTimestamp)
                    || (rangeMaps is not null && readTimestamp.CompareTo(rangeMaps.Current.HighestHistoryBound) < 0)))
                return false;
        }

        KeyValueActorRing ring = durability == KeyValueDurability.Ephemeral ? ephemeralRing : persistentRing;
        PublishedHeadTable table = ring.PublishedHeadsFor(key);

        if (!table.TryGet(key, out PublishedHead head) || head.IsRetracted || table.PrefixLocksHeld)
            return false;

        if (table.HasNewerPendingApply(key, head.Revision, head.LastModified))
            return false;

        if (durability == KeyValueDurability.Persistent)
        {
            if (unflushedWrites is not null
                && unflushedWrites.TryGet(key, out UnflushedKeyValueWrite unflushed)
                && UnflushedKeyValueWritesIndex.IsNewer(unflushed, head.Revision, head.LastModified))
                return false;

            if (preparedIntentStore.Get(key) is not null)
                return false;
        }

        if (transactional && head.MvccHeld)
            return false;

        // The head is the revision at the snapshot only when it was committed at or before it.
        if (snapshot && head.LastModified > readTimestamp)
            return false;

        if (head.Expires != HLCTimestamp.Zero && head.Expires - currentTime < TimeSpan.Zero)
            return false;

        // The record was read live; a retraction from here on is a write concurrent with this read, and the read
        // is linearized before it. Stamp the use for the collector, pin the read for a transaction, answer.
        head.Touch(currentTime.L);

        if (transactional && !snapshot)
            ring.Send(KeyValueRequestPool.RentReadPin(key, transactionId, durability, head));

        entry = new ReadOnlyKeyValueEntry(head.Value, head.Revision, head.Expires, currentTime, head.LastModified, head.State);
        return true;
    }
}

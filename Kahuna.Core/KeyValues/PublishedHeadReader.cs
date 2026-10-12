
using Kommander;
using Kommander.Time;

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
///   <item>Only a latest read (no revision, no snapshot): by-revision and snapshot reads need the archive and
///   the clock fence. Not inside an actor turn: a request rented there is served inline, and a pin sent from
///   inside the turn would land after the turn's own later requests for the key.</item>
///   <item>A live record: the actor retracts it before it changes the head or plants an intent, so a live
///   record is a complete head with no replication intent, no write intent, and no parked committed head. The
///   actor's prefix-lock table must be empty, as a live prefix lock answers the read with MustRetry.</item>
///   <item>No committed head pending on the actor's mailbox for the key (the replicator notes one before the
///   applied frontier advances), and, for a persistent key, no newer head in the unflushed-writes overlay and no
///   durable prepared intent (a committed-but-unsettled value is served through the intent overlay).</item>
///   <item>A transactional read needs an entry with no MVCC dictionary, so this transaction cannot already hold
///   a pin or a staged write that its own read must see. The pin the actor path would record is sent as a
///   control message that lands before the transaction's next request for the key.</item>
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

    private readonly bool enabled;

    internal PublishedHeadReader(
        KeyValueActorRing ephemeralRing,
        KeyValueActorRing persistentRing,
        UnflushedKeyValueWritesIndex? unflushedWrites,
        PreparedIntentStore preparedIntentStore,
        IRaft raft,
        bool enabled)
    {
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
        out ReadOnlyKeyValueEntry? entry)
    {
        entry = null;

        if (!enabled || revision != -1 || !readTimestamp.IsNull() || KeyValueInlineScope.Current is not null)
            return false;

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

        bool transactional = transactionId != HLCTimestamp.Zero;
        if (transactional && head.MvccHeld)
            return false;

        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        if (head.Expires != HLCTimestamp.Zero && head.Expires - currentTime < TimeSpan.Zero)
            return false;

        // The record was read live; a retraction from here on is a write concurrent with this read, and the read
        // is linearized before it. Stamp the use for the collector, pin the read for a transaction, answer.
        head.Touch(currentTime.L);

        if (transactional)
            ring.Send(KeyValueRequestPool.RentReadPin(key, transactionId, durability, head));

        entry = new ReadOnlyKeyValueEntry(head.Value, head.Revision, head.Expires, currentTime, head.LastModified, head.State);
        return true;
    }
}

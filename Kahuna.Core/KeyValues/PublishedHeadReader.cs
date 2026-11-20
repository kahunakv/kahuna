
using Kommander;
using Kommander.Time;

using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Answers a point get from the owning actor's published head record, on the caller's thread, when the record
/// proves the actor would answer the same: the key is resident, nothing newer is on its way to the actor, and
/// no overlay the actor consults holds a newer or undecided version of the key. Any doubt answers false and the
/// read takes the actor path, which is authoritative for every case. The guard that refused the read is reported
/// as a <see cref="PublishedReadFallback"/>.
///
/// <para>The guards, in order, and what each one stands in for on the actor path:</para>
/// <list type="bullet">
///   <item>Not a by-revision read, which needs the archive. Not inside an actor turn: a request rented there
///   is served inline, and a pin sent from inside the turn would land after the turn's own later requests for
///   the key.</item>
///   <item>A snapshot read only when its timestamp is at or above the head's commit time, and it is either
///   outside any transaction or a statement read. The actor answers such a read from the head itself, once the
///   same guards as below have passed, and changes nothing but the last-used stamp. A snapshot below the head
///   needs the archive, and a transactional snapshot read in the default mode is left to the actor. So is a
///   timestamp at or ahead of this node's clock, which the actor folds into the clock before it serves the read,
///   and a timestamp that an installed partition or a moved range may not hold the history for.</item>
///   <item>A live record: the actor retracts it before it changes the head, an intent or the MVCC table, so a
///   live record is a complete head with no replication intent and no parked committed head. The actor's
///   prefix-lock table must be empty, as a live prefix lock answers the read with MustRetry.</item>
///   <item>No committed head pending on the actor's mailbox for the key (the replicator notes one before the
///   applied frontier advances), and, for a persistent key, no newer head in the unflushed-writes overlay and no
///   durable prepared intent that the head does not include. An undecided intent makes the actor wait, and a
///   committed one that is not installed yet is served from the intent. An intent whose write already reached
///   the head stays in the store until its settlement removes it, and the actor answers with the head.</item>
///   <item>A transactional read is decided by what the transaction holds on the key, which the record states.
///   Nothing: a latest read is the transaction's first read, and the pin the actor path would record is sent as
///   a control message that lands before the transaction's next request for the key. A read pin at the head of
///   the record: the actor answers the pinned bytes, which are the bytes of the head. A staged write, or a pin
///   at another revision: the actor answers. A statement read owns only its staged writes and leaves no pin on
///   either path.</item>
///   <item>A write intent on the key does not hold back a latest read, on the actor or here. Two reads are the
///   actor's to decide under one: a read at a timestamp, which waits for the intent or records a commit floor
///   on it, and the first read of the key by a transaction that asked to wait for the intent of another
///   transaction, which the actor parks.</item>
///   <item>The head must not be expired: an expired head is a tombstone the actor converts and reports.</item>
///   <item>The record must still be live after the guards were read. A head installed while they were read
///   retracted the record, and what announced that head can be gone again.</item>
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

    // The server's cap on an intent wait. Zero means the actor parks no read, so a read that asks to wait is
    // answered as one that does not.
    private readonly int maxIntentWaitMs;

    internal PublishedHeadReader(
        KeyValueActorRing ephemeralRing,
        KeyValueActorRing persistentRing,
        UnflushedKeyValueWritesIndex? unflushedWrites,
        PreparedIntentStore preparedIntentStore,
        IRaft raft,
        bool enabled,
        InstalledHistoryFence? installedHistory = null,
        RangeMapStore? rangeMaps = null,
        int maxIntentWaitMs = 0)
    {
        this.maxIntentWaitMs = maxIntentWaitMs;
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
        out ReadOnlyKeyValueEntry? entry) =>
        TryRead(transactionId, key, revision, readTimestamp, durability, readMode, default, out entry, out _);

    /// <summary>
    /// <see cref="TryRead(HLCTimestamp, string, long, HLCTimestamp, KeyValueDurability, KeyValueReadMode, out ReadOnlyKeyValueEntry?)"/>
    /// for a read that carries <paramref name="intentWait"/>, with the guard that refused the read in
    /// <paramref name="fallback"/> when the answer is false.
    /// </summary>
    public bool TryRead(
        HLCTimestamp transactionId,
        string key,
        long revision,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        KeyValueReadMode readMode,
        KeyValueIntentWait intentWait,
        out ReadOnlyKeyValueEntry? entry,
        out PublishedReadFallback fallback)
    {
        entry = null;

        if (!enabled)
            return Refuse(PublishedReadFallback.Disabled, out fallback);

        if (revision != -1 || KeyValueInlineScope.Current is not null)
            return Refuse(PublishedReadFallback.ReadKind, out fallback);

        bool transactional = transactionId != HLCTimestamp.Zero;
        bool snapshot = !readTimestamp.IsNull();

        if (snapshot && transactional && !KeyValueReadModeChecks.IsStatementRead(readMode, transactionId, readTimestamp))
            return Refuse(PublishedReadFallback.ReadKind, out fallback);

        HLCTimestamp currentTime = raft.HybridLogicalClock.TrySendOrLocalEvent(raft.GetLocalNodeId());

        if (snapshot)
        {
            // The actor serves a snapshot read only behind two checks of its own, made before any handler runs.
            // This node's clock must be past the timestamp, so that no commit staged here afterwards is stamped at
            // or below it: a timestamp at or ahead of the clock is left to the actor, which folds it into the
            // clock or refuses it. And the node must hold the history the timestamp needs: any timestamp an
            // install or a moved range could bound is left to the actor, which decides it per partition and range.
            if (readTimestamp.CompareTo(currentTime) >= 0)
                return Refuse(PublishedReadFallback.SnapshotBound, out fallback);

            if (durability == KeyValueDurability.Persistent && installedHistory is not null
                && (installedHistory.MayRefuse(readTimestamp)
                    || (rangeMaps is not null && readTimestamp.CompareTo(rangeMaps.Current.HighestHistoryBound) < 0)))
                return Refuse(PublishedReadFallback.SnapshotBound, out fallback);
        }

        KeyValueActorRing ring = durability == KeyValueDurability.Ephemeral ? ephemeralRing : persistentRing;
        PublishedHeadTable table = ring.PublishedHeadsFor(key);

        // For the read floor of a transaction: a time at which the record below was the committed head. Taken
        // before the record is looked up, so only a record installed after it needs the later of the two.
        long beforeLookupTicks = transactional && !snapshot ? System.Diagnostics.Stopwatch.GetTimestamp() : 0;

        if (!table.TryGet(key, out PublishedHead head))
            return Refuse(PublishedReadFallback.Absent, out fallback);

        if (head.IsRetracted)
            return Refuse(head.RetractCause, out fallback);

        return TryServe(ring, table, head, transactionId, key, readTimestamp, durability, intentWait, currentTime, beforeLookupTicks, out entry, out fallback);
    }

    /// <summary>
    /// Judges <paramref name="head"/>, a record the caller took from <paramref name="table"/> and saw live,
    /// against every guard, and answers the read from it when all of them pass and the record is still live
    /// after them. The caller made the checks that do not depend on the record. Apart from the lookup so that a
    /// test can judge a record that was replaced after it was taken.
    /// </summary>
    internal bool TryServe(
        KeyValueActorRing ring,
        PublishedHeadTable table,
        PublishedHead head,
        HLCTimestamp transactionId,
        string key,
        HLCTimestamp readTimestamp,
        KeyValueDurability durability,
        KeyValueIntentWait intentWait,
        HLCTimestamp currentTime,
        long beforeLookupTicks,
        out ReadOnlyKeyValueEntry? entry,
        out PublishedReadFallback fallback)
    {
        entry = null;

        bool transactional = transactionId != HLCTimestamp.Zero;
        bool snapshot = !readTimestamp.IsNull();

        if (table.PrefixLocksHeld)
            return Refuse(PublishedReadFallback.PrefixLock, out fallback);

        if (table.HasNewerPendingApply(key, head.Revision, head.LastModified))
            return Refuse(PublishedReadFallback.PendingApply, out fallback);

        if (durability == KeyValueDurability.Persistent)
        {
            if (unflushedWrites is not null
                && unflushedWrites.TryGet(key, out UnflushedKeyValueWrite unflushed)
                && UnflushedKeyValueWritesIndex.IsNewer(unflushed, head.Revision, head.LastModified))
                return Refuse(PublishedReadFallback.Unflushed, out fallback);

            // A durable prepared intent stays in the store after its commit reached the head, until its
            // settlement removes it. The actor answers a read of such a key with the bytes of the head, so the
            // record answers it too. An intent the head does not include is undecided, or committed and not
            // installed yet, and the actor decides: it waits for the decision or serves the intent's value. So
            // does it for an intent of the reading transaction.
            if (preparedIntentStore.Get(key) is { } intent
                && ((transactional && intent.TransactionId == transactionId) || !HeadIncludes(head, intent)))
                return Refuse(PublishedReadFallback.PreparedIntent, out fallback);
        }

        // What the reading transaction holds on the key. A staged write is its own view of the key, and the actor
        // answers it. So does a read pin at another revision than this head: the actor answers Aborted for a head
        // that moved past the pin, and the pinned value otherwise. A read pin at this head answers with the bytes
        // of the head, and no entry means this is the transaction's first read of the key. A statement read owns
        // only its staged writes, so a pin does not bind it.
        PublishedMvccState held = transactional ? head.MvccStateOf(transactionId) : PublishedMvccState.None;

        if (held == PublishedMvccState.Unknown)
            return Refuse(PublishedReadFallback.MvccHeld, out fallback);

        if (held == PublishedMvccState.Staged)
            return Refuse(PublishedReadFallback.MvccStaged, out fallback);

        if (held == PublishedMvccState.PinMoved && !snapshot)
            return Refuse(PublishedReadFallback.MvccPinMoved, out fallback);

        if (head.WriteIntentHeld)
        {
            // A snapshot read over a write intent is the actor's to decide: it waits for a commit that is fixed
            // at or below the timestamp, and it records the timestamp on the intent as the floor of a commit
            // that can still move.
            if (snapshot)
                return Refuse(PublishedReadFallback.WriteIntent, out fallback);

            // A latest read is answered with the committed head below the intent. The exception is the first
            // read of the key by a transaction that asked to wait for the intent of another transaction: the
            // actor parks that read. A transaction that already holds an entry on the key does not park.
            if (transactional
                && held == PublishedMvccState.None
                && head.WriteIntentOwner != transactionId
                && maxIntentWaitMs > 0
                && intentWait.LimitMs > 0)
                return Refuse(PublishedReadFallback.IntentWait, out fallback);
        }

        // The head is the revision at the snapshot only when it was committed at or before it.
        if (snapshot && head.LastModified > readTimestamp)
            return Refuse(PublishedReadFallback.SnapshotBelowHead, out fallback);

        if (head.Expires != HLCTimestamp.Zero && head.Expires - currentTime < TimeSpan.Zero)
            return Refuse(PublishedReadFallback.Expired, out fallback);

        // A transaction's first latest read of the key leaves a pin, sent to the actor as a control message.
        // Until the pin lands no record shows the transaction, so the pin is registered first. A second read of
        // the key by the transaction then finds the registration: it is answered from a record of the same head,
        // or by the actor when the head changed, and the actor processes the pin before it answers.
        bool sendPin = false;

        if (transactional && !snapshot && held == PublishedMvccState.None)
        {
            if (table.TryRegisterPin(transactionId, key, head, out PublishedHead inFlight))
                sendPin = true;
            else if (inFlight.Revision != head.Revision || inFlight.LastModified != head.LastModified)
                return Refuse(PublishedReadFallback.PinInFlight, out fallback);
        }

        // The guards ran after the first look at the record. A head installed in that time retracted the record,
        // and what announced that head (the pending apply, the prepared intent, an earlier pin of this
        // transaction) can be gone again. A record that is still live here was the committed head for the whole
        // time the guards ran. A pin that is already registered is sent in both cases: another read of the
        // transaction can rely on it, and the actor then answers this read after it processed the pin.
        if (head.IsRetracted)
        {
            if (sendPin)
                SendPin(ring, table, key, transactionId, durability, head);

            return Refuse(head.RetractCause, out fallback);
        }

        // A retraction from here on is a write concurrent with this read, and the read is linearized before it.
        // Stamp the use for the collector, pin the read for a transaction, answer.
        head.Touch(currentTime.L);

        if (sendPin)
            SendPin(ring, table, key, transactionId, durability, head);

        entry = new ReadOnlyKeyValueEntry(head.Value, head.Revision, head.Expires, currentTime, head.LastModified, head.State)
        {
            HeadSeenTicks = beforeLookupTicks != 0 ? head.HeadSinceTicks : 0,
            ServedTicks = beforeLookupTicks != 0 && head.HeadSinceTicks != 0 ? Math.Max(beforeLookupTicks, head.HeadSinceTicks) : 0
        };

        fallback = PublishedReadFallback.None;
        return true;
    }

    /// <summary>
    /// Whether the head of the record includes the write of <paramref name="intent"/>. A head at a later revision
    /// replaced it. A head at the intent's revision includes it when it carries the intent's commit timestamp,
    /// which makes it the installed intent itself, or a later stamp, which makes it an extend over the installed
    /// intent. A committed delete keeps the revision of the value it deletes, so at an equal revision only an
    /// intent that sets a value counts.
    /// </summary>
    private static bool HeadIncludes(PublishedHead head, PreparedIntent intent)
    {
        if (head.Revision > intent.Revision)
            return true;

        return head.Revision == intent.Revision
            && intent.State == KeyValueState.Set
            && !intent.CommitTimestamp.IsNull()
            && head.LastModified >= intent.CommitTimestamp;
    }

    /// <summary>Sends the registered read pin of a read served from <paramref name="head"/>. A pin that cannot
    /// be sent is taken out of the registry again, so no read waits on a pin that never lands.</summary>
    private static void SendPin(KeyValueActorRing ring, PublishedHeadTable table, string key, HLCTimestamp transactionId, KeyValueDurability durability, PublishedHead head)
    {
        try
        {
            ring.Send(KeyValueRequestPool.RentReadPin(key, transactionId, durability, head));
        }
        catch
        {
            table.ForgetPin(transactionId, key, head);
            throw;
        }
    }

    private static bool Refuse(PublishedReadFallback cause, out PublishedReadFallback fallback)
    {
        fallback = cause;
        return false;
    }
}

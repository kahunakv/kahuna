
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// An immutable copy of a resident key's committed head, published by the owning actor so a point read can be
/// answered on the caller's thread without a mailbox round trip. The actor is the only writer; readers only
/// compare, copy, and stamp the last-used time.
///
/// <para>A record exists only for a head in state <c>Set</c> with no replication intent and no parked committed
/// head when it was published. The actor <b>retracts</b> the record (one volatile field) the moment the head, an
/// intent or the MVCC table of the entry changes, before it touches the head fields, so a reader that observes
/// a live record observes a head that was complete and visible at the time it was read. The fields themselves
/// are never mutated: a changed head is a new record.</para>
///
/// <para>A write intent does not keep the head from being published: the actor answers a latest read with the
/// committed head below the intent, and the record holds that head. <see cref="WriteIntentHeld"/> and
/// <see cref="WriteIntentOwner"/> state the intent, for the reads the actor treats differently under one: a read
/// at a timestamp, and a read that asked to wait for the intent.</para>
///
/// <para><see cref="Pins"/> is a copy of the entry's MVCC table when the record was published: for each
/// transaction that holds an entry there, whether it is a read pin at this head, a read pin at another
/// revision, or a staged write. A transaction's own pin or staged write is authoritative for its reads, and the
/// reader cannot inspect the actor-confined table, so it decides from the copy (see <see cref="MvccStateOf"/>).
/// The copy is exact for a live record: every change of the table retracts the record.</para>
///
/// <para>The write intent and the MVCC copy live in <see cref="Holds"/>, which is null for a key that has
/// neither. Most resident keys have neither, and every resident key has a record.</para>
/// </summary>
internal sealed class PublishedHead
{
    /// <summary>The most MVCC entries a record carries. A key with more publishes none of them, and every
    /// transactional read of it takes the actor: the copy is made again at each change of the table, so its
    /// cost must stay bounded on a key that many transactions read at once.</summary>
    internal const int MaxPins = 16;

    public readonly byte[]? Value;

    public readonly long Revision;

    public readonly HLCTimestamp Expires;

    public readonly HLCTimestamp LastModified;

    public readonly KeyValueState State;

    /// <summary>The write intent and the MVCC entries of the key at publish time, or null when the key had no
    /// write intent and no MVCC table.</summary>
    public readonly PublishedHolds? Holds;

    /// <summary>The process clock when this head became the committed head in this process, or zero when the
    /// entry could not say (see <see cref="KeyValueEntry.HeadSeenTicks"/>).</summary>
    public readonly long HeadSinceTicks;

    // 0 while the record is live. Otherwise the PublishedReadFallback that says why it is retracted:
    // Retracted from the moment of the retraction, and the cause the actor recorded once it left the record in
    // the table. Written only by the owning actor; read by every off-actor reader.
    private int retracted;

    // Physical HLC milliseconds of the last off-actor read. Written by readers, read by the collector on the
    // actor thread so a key that is hot only through published reads is not treated as idle.
    private long lastUsedMs;

    public PublishedHead(byte[]? value, long revision, HLCTimestamp expires, HLCTimestamp lastModified, KeyValueState state, long lastUsedMs, long headSinceTicks = 0, PublishedHolds? holds = null)
    {
        Holds = holds;
        HeadSinceTicks = headSinceTicks;
        Value = value;
        Revision = revision;
        Expires = expires;
        LastModified = lastModified;
        State = state;
        this.lastUsedMs = lastUsedMs;
    }

    /// <summary>True when the entry held an MVCC table (any transaction's pin or staged write) at publish
    /// time. The table is created and dropped through the entry's setter, which retracts the record, so a
    /// live record with this flag clear proves no transaction holds a snapshot on the key.</summary>
    public bool MvccHeld => Holds is { MvccHeld: true };

    /// <summary>True when a write intent held the key at publish time: a point lock, a staged write, or a
    /// prepared write of a transaction. The head of the record is the committed head below it. The intent is
    /// set and cleared through the entry's setter, which retracts the record.</summary>
    public bool WriteIntentHeld => Holds is { WriteIntentHeld: true };

    /// <summary>The transaction that owns the write intent, when <see cref="WriteIntentHeld"/> is true.</summary>
    public HLCTimestamp WriteIntentOwner => Holds is { } holds ? holds.WriteIntentOwner : HLCTimestamp.Zero;

    /// <summary>The MVCC entries of the key at publish time, one per transaction, in no order. Null when the
    /// record does not carry them: the key had no MVCC table, or the table held more than
    /// <see cref="MaxPins"/> entries (<see cref="MvccHeld"/> tells the two apart).</summary>
    public PublishedPin[]? Pins => Holds?.Pins;

    /// <summary>Whether the owning actor retracted this record. A retracted record must not be served.</summary>
    public bool IsRetracted => Volatile.Read(ref retracted) != 0;

    /// <summary>Marks the record stale. Called by the owning actor before it changes the entry's head, its
    /// intents or its MVCC table.</summary>
    public void Retract() => Volatile.Write(ref retracted, (int)PublishedReadFallback.Retracted);

    /// <summary>
    /// Why the record is retracted: <see cref="PublishedReadFallback.Retracted"/> from the retraction on, then
    /// the state of the entry that kept the actor from publishing a new record at the end of the turn, when it
    /// recorded one. <see cref="PublishedReadFallback.None"/> for a live record. For observability only: a
    /// reader never serves a retracted record, whatever the cause.
    /// </summary>
    public PublishedReadFallback RetractCause => (PublishedReadFallback)Volatile.Read(ref retracted);

    /// <summary>Records why a retracted record was not replaced. Actor thread only, on a record it already
    /// retracted.</summary>
    public void SetRetractCause(PublishedReadFallback cause)
    {
        if (cause != PublishedReadFallback.None && IsRetracted)
            Volatile.Write(ref retracted, (int)cause);
    }

    /// <summary>Physical HLC milliseconds of the last off-actor read of this record.</summary>
    public long LastUsedMs => Volatile.Read(ref lastUsedMs);

    /// <summary>Stamps an off-actor read. Readers race on this; a slightly older stamp winning is harmless.</summary>
    public void Touch(long physicalMs) => Volatile.Write(ref lastUsedMs, physicalMs);

    /// <summary>
    /// What <paramref name="transactionId"/> holds on the key, as far as the record can say.
    /// <see cref="PublishedMvccState.Unknown"/> when the entry held MVCC entries the record does not carry.
    /// </summary>
    public PublishedMvccState MvccStateOf(HLCTimestamp transactionId)
    {
        if (Holds is not { MvccHeld: true } holds)
            return PublishedMvccState.None;

        PublishedPin[]? pins = holds.Pins;
        if (pins is null)
            return PublishedMvccState.Unknown;

        for (int i = 0; i < pins.Length; i++)
        {
            if (pins[i].TransactionId == transactionId)
                return pins[i].State;
        }

        return PublishedMvccState.None;
    }

    /// <summary>Whether the record is strictly behind the head identified by <paramref name="revision"/> and
    /// <paramref name="lastModified"/>: a higher revision, or the same revision with a later commit stamp (a
    /// delete or an extend reuses the revision it replaces).</summary>
    public bool IsBehind(long revision, HLCTimestamp lastModified) =>
        revision > Revision || (revision == Revision && lastModified > LastModified);
}

/// <summary>
/// What transactions held on a key when its head record was published: the write intent, and a copy of the MVCC
/// table. Immutable. One per record of a key that has a write intent or an MVCC table.
/// </summary>
internal sealed class PublishedHolds
{
    /// <summary>True when a write intent held the key.</summary>
    public readonly bool WriteIntentHeld;

    /// <summary>The transaction that owns the write intent, when <see cref="WriteIntentHeld"/> is true.</summary>
    public readonly HLCTimestamp WriteIntentOwner;

    /// <summary>True when the entry held an MVCC table.</summary>
    public readonly bool MvccHeld;

    /// <summary>The MVCC entries of the key, one per transaction, in no order. Null when the key had no MVCC
    /// table, or when the table held more than <see cref="PublishedHead.MaxPins"/> entries.</summary>
    public readonly PublishedPin[]? Pins;

    public PublishedHolds(bool writeIntentHeld, HLCTimestamp writeIntentOwner, bool mvccHeld, PublishedPin[]? pins)
    {
        WriteIntentHeld = writeIntentHeld;
        WriteIntentOwner = writeIntentOwner;
        MvccHeld = mvccHeld;
        Pins = pins;
    }
}

/// <summary>What one transaction holds in the MVCC table of a key, as a published head record states it.</summary>
internal enum PublishedMvccState : byte
{
    /// <summary>Nothing: the transaction has not read or written the key.</summary>
    None = 0,

    /// <summary>A read pin at the head of the record: the same revision and the same commit stamp. The actor
    /// answers the transaction's next latest read of the key with the bytes of the head.</summary>
    PinAtHead,

    /// <summary>A read pin at another revision or stamp than the head of the record: a head that moved since the
    /// pin, or a pin taken from a committed intent that the head does not include yet.</summary>
    PinMoved,

    /// <summary>A staged write of the transaction.</summary>
    Staged,

    /// <summary>The record does not carry the MVCC entries of the key.</summary>
    Unknown
}

/// <summary>One transaction's MVCC entry on a key, as copied into a published head record.</summary>
internal readonly record struct PublishedPin(HLCTimestamp TransactionId, PublishedMvccState State);

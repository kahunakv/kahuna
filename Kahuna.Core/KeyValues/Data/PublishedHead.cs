
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// An immutable copy of a resident key's committed head, published by the owning actor so a latest point
/// read can be answered on the caller's thread without a mailbox round trip. The actor is the only writer;
/// readers only compare, copy, and stamp the last-used time.
///
/// <para>A record exists only for a head that was <b>quiet</b> when it was published: state <c>Set</c>, no
/// replication intent, no write intent, no parked committed head. The actor <b>retracts</b> the record (one
/// volatile flag) the moment any of those change on the entry, before it touches the head fields, so a reader
/// that observes a live record observes a head that was complete and visible at the time it was read. The
/// fields themselves are never mutated: a changed head is a new record.</para>
///
/// <para><see cref="MvccHeld"/> says whether the entry carried an MVCC dictionary when the record was
/// published. A transactional read must not be answered from a record of an entry that may already hold a
/// snapshot for that transaction — its own earlier pin or staged write is authoritative — and the reader
/// cannot inspect the actor-confined dictionary, so the flag routes those reads to the actor.</para>
/// </summary>
internal sealed class PublishedHead
{
    public readonly byte[]? Value;

    public readonly long Revision;

    public readonly HLCTimestamp Expires;

    public readonly HLCTimestamp LastModified;

    public readonly KeyValueState State;

    /// <summary>True when the entry held an MVCC dictionary (any transaction's pin or staged write) at publish
    /// time. The dictionary is created and dropped through the entry's setter, which retracts the record, so a
    /// live record with this flag clear proves no transaction holds a snapshot on the key.</summary>
    public readonly bool MvccHeld;

    // 0 = live, 1 = retracted. Written only by the owning actor; read by every off-actor reader.
    private int retracted;

    // Physical HLC milliseconds of the last off-actor read. Written by readers, read by the collector on the
    // actor thread so a key that is hot only through published reads is not treated as idle.
    private long lastUsedMs;

    public PublishedHead(byte[]? value, long revision, HLCTimestamp expires, HLCTimestamp lastModified, KeyValueState state, bool mvccHeld, long lastUsedMs)
    {
        Value = value;
        Revision = revision;
        Expires = expires;
        LastModified = lastModified;
        State = state;
        MvccHeld = mvccHeld;
        this.lastUsedMs = lastUsedMs;
    }

    /// <summary>Whether the owning actor retracted this record. A retracted record must not be served.</summary>
    public bool IsRetracted => Volatile.Read(ref retracted) != 0;

    /// <summary>Marks the record stale. Called by the owning actor before it changes the entry's head or intents.</summary>
    public void Retract() => Volatile.Write(ref retracted, 1);

    /// <summary>Physical HLC milliseconds of the last off-actor read of this record.</summary>
    public long LastUsedMs => Volatile.Read(ref lastUsedMs);

    /// <summary>Stamps an off-actor read. Readers race on this; a slightly older stamp winning is harmless.</summary>
    public void Touch(long physicalMs) => Volatile.Write(ref lastUsedMs, physicalMs);

    /// <summary>Whether the record is strictly behind the head identified by <paramref name="revision"/> and
    /// <paramref name="lastModified"/>: a higher revision, or the same revision with a later commit stamp (a
    /// delete or an extend reuses the revision it replaces).</summary>
    public bool IsBehind(long revision, HLCTimestamp lastModified) =>
        revision > Revision || (revision == Revision && lastModified > LastModified);
}

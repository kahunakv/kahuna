
namespace Kahuna.Server.KeyValues;

/// <summary>
/// Why a point read was not answered from the owning actor's published head record and took the actor path.
/// The first guard that refuses the read names the reason. Recorded as the <c>reason</c> tag of
/// <c>kahuna.kv.read.fallback</c>.
/// </summary>
internal enum PublishedReadFallback : byte
{
    /// <summary>The record answered the read.</summary>
    None = 0,

    /// <summary>Published head reads are turned off on this node.</summary>
    Disabled,

    /// <summary>A read the record never answers: by revision, from inside an actor turn, or a transactional
    /// snapshot read in the default read mode.</summary>
    ReadKind,

    /// <summary>A snapshot timestamp at or ahead of this node's clock, or one that an installed partition or a
    /// moved range may not hold the history for.</summary>
    SnapshotBound,

    /// <summary>A snapshot timestamp below the commit time of the head: the archive holds the answer.</summary>
    SnapshotBelowHead,

    /// <summary>No record for the key: it is not resident, it is a tombstone, or it was never published.</summary>
    Absent,

    /// <summary>The record is retracted and the actor recorded no cause yet: it is changing the key in its
    /// current turn.</summary>
    Retracted,

    /// <summary>A direct write of the key is in flight (replication intent).</summary>
    ReplicationIntent,

    /// <summary>A committed head is parked on the entry behind an operation in flight.</summary>
    ParkedHead,

    /// <summary>The actor holds a prefix lock.</summary>
    PrefixLock,

    /// <summary>A committed head newer than the record waits on the actor's mailbox.</summary>
    PendingApply,

    /// <summary>The unflushed-writes overlay holds a head newer than the record.</summary>
    Unflushed,

    /// <summary>A durable prepared intent covers the key and the record does not include it.</summary>
    PreparedIntent,

    /// <summary>A write intent holds the key and the read is one the actor must decide under it.</summary>
    WriteIntent,

    /// <summary>A transactional read of a key whose MVCC state the record cannot answer for.</summary>
    MvccHeld,

    /// <summary>The reading transaction staged a write on the key.</summary>
    MvccStaged,

    /// <summary>The reading transaction holds a read pin that is not at the head of the record.</summary>
    MvccPinMoved,

    /// <summary>A read pin of the reading transaction, taken from another record, has not landed on the actor.</summary>
    PinInFlight,

    /// <summary>A transactional read that asked to wait behind a live write intent of another transaction.</summary>
    IntentWait,

    /// <summary>The head is past its expiry: the actor converts it to a tombstone and reports it.</summary>
    Expired
}

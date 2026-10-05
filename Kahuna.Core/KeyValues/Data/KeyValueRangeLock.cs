
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

public sealed class KeyValueRangeLock
{
    public HLCTimestamp  TransactionId  { get; set; }

    /// <summary>
    /// The lease's deadline as a hybrid logical clock timestamp, which is the form a lock travels in: a split
    /// or merge transfer, the wire, an operator's listing. Zero marks a session-owned lock, which has no
    /// deadline. While an actor holds the lock the lease is measured by <see cref="LeaseEndsAtTick"/>, and
    /// this value is only the deadline as it stood at the last grant or renewal.
    /// </summary>
    public HLCTimestamp  Expires        { get; set; }

    /// <summary>
    /// The lease's deadline on the monotonic clock of the node whose actor holds the lock
    /// (<see cref="Environment.TickCount64"/>). Zero for a lock no actor holds yet — a copy taken for a
    /// transfer, or one decoded from it — and for a session-owned lock.
    ///
    /// <para>A lease is a promise about elapsed time, and the hybrid logical clock does not measure it: its
    /// physical part follows the wall clock, and it adopts any larger timestamp a peer sends. A stepped wall
    /// clock anywhere in the cluster therefore moves it forward by the size of the step at once. Measured
    /// against it, every lease shorter than the step would end in that instant, under transactions that are
    /// still running.</para>
    /// </summary>
    public long          LeaseEndsAtTick { get; set; }

    /// <summary>True once the holder was recorded in the <see cref="LapsedRangeLockRegistry"/>, so a dead
    /// lock that stays in the table is recorded once and not on every write that steps over it.</summary>
    public bool          LapseRecorded  { get; set; }

    public string?       StartKey       { get; set; }
    public bool          StartInclusive { get; set; }
    public string?       EndKey         { get; set; }
    public bool          EndInclusive   { get; set; }
    public RangeLockMode Mode           { get; set; }
}

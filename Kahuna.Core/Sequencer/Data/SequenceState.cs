using Kahuna.Shared.Sequences;
using Kommander.Time;

namespace Kahuna.Server.Sequencer.Data;

/// <summary>
/// The durable record behind a sequence, stored as an ordinary key-value entry under
/// <c>__kahuna:sequences:{name}</c>.
///
/// <para><see cref="CurrentValue"/> is a <b>high-water mark</b>: the largest value that has been
/// <em>reserved</em>, not necessarily one that has been handed to a caller. A node reserves a whole
/// block by compare-and-swapping this field upwards once, then serves values from memory until the
/// block is drained. Anything left in an abandoned block becomes a gap.</para>
/// </summary>
internal sealed class SequenceState
{
    public string Name { get; set; } = "";

    /// <summary>Highest reserved value. Values above this have never been handed out by anyone.</summary>
    public long CurrentValue { get; set; }

    public long InitialValue { get; set; }

    public long Increment { get; set; }

    public long? MaxValue { get; set; }

    /// <summary>
    /// Values reserved per compare-and-swap for this one sequence. Null means the server-wide
    /// <c>SequencerBlockSize</c> applies, resolved at every reservation rather than frozen into the
    /// record — an operator who retunes the node must see old sequences follow the new setting.
    /// <c>1</c> is gap-free at one commit, with its fsync, per value.
    /// </summary>
    public int? BlockSize { get; set; }

    /// <summary>
    /// How many times this sequence's identity as a value stream has been deliberately broken. Bumped
    /// only by an update; never by an ordinary reservation, which is why a block holder can compare it
    /// to decide whether the window it reserved still belongs to the record it is looking at.
    /// </summary>
    public long Incarnation { get; set; }

    /// <summary>
    /// When the last break happened. <see cref="HLCTimestamp.Zero"/> on a record that has never been
    /// updated. No node may issue values from the sequence until <c>SequencerBlockLease</c> has passed
    /// since this instant: a block reserved from the previous incarnation, held on a node that has not
    /// yet revalidated, is still live until then. Used as a duration bound only, never to order events.
    /// </summary>
    public HLCTimestamp IncarnatedAt { get; set; }

    public HLCTimestamp CreatedAt { get; set; }

    public HLCTimestamp UpdatedAt { get; set; }

    /// <summary>
    /// Allocations replayable by idempotency key, keyed by <c>reserve:{key}</c>. Bounded by count and
    /// age so the record cannot grow without limit; see <see cref="SequenceStateCodec.Prune"/>.
    /// </summary>
    public Dictionary<string, SequenceIdempotencyEntry> Idempotency { get; set; } = [];
}

/// <summary>
/// One replayable allocation plus the timestamp used to age it out of the retention window. The
/// timestamp is kept outside <see cref="SequenceAllocation"/> so the client-facing wire type is
/// untouched.
/// </summary>
internal readonly record struct SequenceIdempotencyEntry(SequenceAllocation Allocation, HLCTimestamp CreatedAt);

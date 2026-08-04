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

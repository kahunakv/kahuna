using Kommander.Time;

namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// The source-of-truth routing record for a single contiguous key range (Spanner/CRDB-style).
/// A descriptor says: "every key in <see cref="KeySpace"/> that falls in
/// <c>[<see cref="StartKey"/>, <see cref="EndKey"/>)</c> is owned by Raft partition
/// <see cref="PartitionId"/> at routing generation <see cref="Generation"/>".
///
/// <para>
/// Bounds are <b>ordinal</b> (<see cref="string.CompareOrdinal(string,string)"/>) everywhere —
/// consistent with the rest of Kahuna and the documented culture-vs-ordinal hazard. The range is
/// half-open: <see cref="StartKey"/> is inclusive, <see cref="EndKey"/> is exclusive. A null bound
/// means ±infinity <i>within the key space</i>: null <see cref="StartKey"/> = −inf, null
/// <see cref="EndKey"/> = +inf.
/// </para>
///
/// </summary>
internal sealed record RangeDescriptor
{
    /// <summary>The prefix/key-space this range belongs to, e.g. <c>"{tableId}:r"</c>.</summary>
    public required string KeySpace { get; init; }

    /// <summary>Inclusive lower bound (ordinal). Null = −infinity within <see cref="KeySpace"/>.</summary>
    public string? StartKey { get; init; }

    /// <summary>Exclusive upper bound (ordinal). Null = +infinity within <see cref="KeySpace"/>.</summary>
    public string? EndKey { get; init; }

    /// <summary>The Raft group currently serving this range.</summary>
    public required int PartitionId { get; init; }

    /// <summary>Bumped on every split/merge/move — fences stale routing.</summary>
    public long Generation { get; init; }

    /// <summary>
    /// Deadline until which this range refuses writes because its data is being moved to another
    /// partition. <see cref="HLCTimestamp.Zero"/> means the range is open for writes.
    ///
    /// <para>
    /// The range mover holds an exclusive range lock for the same window, but that lock is in-memory
    /// and leader-local: a leadership change on the owning partition drops it, and it is invisible to
    /// nodes that merely route to that partition. This field rides the replicated range map instead,
    /// so every node sees the quiesce and a newly promoted leader still refuses writes into a range
    /// whose contents are mid-transfer.
    /// </para>
    ///
    /// <para>
    /// It is a deadline rather than a flag so that no live owner is required to end it: a mover that
    /// dies mid-window leaves a quiesce that lapses on its own instead of a range that refuses writes
    /// forever.
    /// </para>
    /// </summary>
    public HLCTimestamp QuiescedUntil { get; init; }

    /// <summary>
    /// The transaction id of the range move that opened the quiesce. A release clears the quiesce
    /// only when it matches, so a straggling release from a move that already gave up cannot open a
    /// later move's window.
    /// </summary>
    public HLCTimestamp QuiesceOwner { get; init; }

    /// <summary>
    /// The half-open sub-interval of this range that is quiesced, in the same ordinal, null-is-
    /// infinity convention as <see cref="StartKey"/>/<see cref="EndKey"/>. A split moves only part
    /// of a range, and only the part being moved must stop accepting writes: quiescing the whole
    /// descriptor would also stall the half that is staying put, for no reason. Ignored when
    /// <see cref="QuiescedUntil"/> is <see cref="HLCTimestamp.Zero"/>.
    /// </summary>
    public string? QuiesceStartKey { get; init; }

    /// <inheritdoc cref="QuiesceStartKey"/>
    public string? QuiesceEndKey { get; init; }

    /// <summary>
    /// True when any part of this range is still refusing writes at <paramref name="now"/>. A lapsed
    /// deadline reads as open — the quiesce needs no one to clear it.
    /// </summary>
    public bool IsQuiescedAt(HLCTimestamp now) =>
        QuiescedUntil != HLCTimestamp.Zero && QuiescedUntil - now > TimeSpan.Zero;

    /// <summary>
    /// True when <paramref name="key"/> specifically falls in the quiesced sub-interval and the
    /// deadline has not lapsed. The caller is responsible for matching <see cref="KeySpace"/>.
    /// </summary>
    public bool IsQuiescedAt(string key, HLCTimestamp now)
    {
        if (!IsQuiescedAt(now))
            return false;

        if (QuiesceStartKey is not null && string.CompareOrdinal(key, QuiesceStartKey) < 0)
            return false;

        if (QuiesceEndKey is not null && string.CompareOrdinal(key, QuiesceEndKey) >= 0)
            return false;

        return true;
    }

    /// <summary>
    /// True when <paramref name="key"/> falls in this range's half-open ordinal interval. The
    /// caller is responsible for matching <see cref="KeySpace"/>; this checks bounds only.
    /// </summary>
    public bool Contains(string key)
    {
        if (StartKey is not null && string.CompareOrdinal(key, StartKey) < 0)
            return false;

        if (EndKey is not null && string.CompareOrdinal(key, EndKey) >= 0)
            return false;

        return true;
    }

    public override string ToString() =>
        $"Range({KeySpace} [{StartKey ?? "-inf"}, {EndKey ?? "+inf"}) -> P{PartitionId} gen{Generation}"
        + (QuiescedUntil == HLCTimestamp.Zero
            ? ")"
            : $" quiescing [{QuiesceStartKey ?? "-inf"}, {QuiesceEndKey ?? "+inf"}) until {QuiescedUntil})");
}

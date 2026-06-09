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
/// See <c>docs/spec-range-splits.md</c> §4 and <c>docs/spec-range-splits-tasks.md</c> Task 1.
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

    /// <summary>Bumped on every split/merge/move — fences stale routing (design §4).</summary>
    public long Generation { get; init; }

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
        $"Range({KeySpace} [{StartKey ?? "-inf"}, {EndKey ?? "+inf"}) -> P{PartitionId} gen{Generation})";
}

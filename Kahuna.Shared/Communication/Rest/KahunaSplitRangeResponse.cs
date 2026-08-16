using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Outcome of a manual range split.
/// <para>
/// The field that matters to a fault-injection harness is <see cref="Determinate"/>. A split runs a
/// multi-step transaction — create the destination partition, quiesce the source, copy, commit the
/// cutover — and failures in the later steps leave the outcome genuinely unknown to the caller: the
/// map may still change moments after the call returns. Collapsing those into the same answer as
/// "refused, nothing happened" is how a checker ends up manufacturing findings, so the two groups
/// are reported apart and a caller must re-read <c>GET /v1/ranges</c> before concluding anything
/// about an indeterminate one.
/// </para>
/// <para><c>status</c> is one of:</para>
/// <list type="bullet">
///   <item><description><c>Succeeded</c> — the map now has two ranges; <c>newPartitionId</c> and
///     <c>newGeneration</c> describe the upper half. Determinate.</description></item>
///   <item><description><c>NotLeader</c> — this node does not lead the partition that owns the range
///     map, and never attempted the split. Determinate. Retry against the leader; <c>leaderHint</c>
///     names it when gossip knows.</description></item>
///   <item><description><c>NoRange</c> — no descriptor covers the key, so the space is unregistered or
///     unseeded. Determinate.</description></item>
///   <item><description><c>InvalidSplitKey</c> — the key is outside the covering range, or equal to its
///     start, which would produce an empty half. Determinate.</description></item>
///   <item><description><c>BelowMinRangeSize</c> — refused by policy: one of the halves holds no keys.
///     Determinate.</description></item>
///   <item><description><c>PartitionCreationFailed</c> — no descriptor changed, so determinate <i>for
///     the range map</i>; a destination partition may still have been created and left unused.</description></item>
///   <item><description><c>TransferFailed</c>, <c>QuiesceFailed</c>, <c>CutoverFailed</c>,
///     <c>ConcurrentSplit</c> — <b>indeterminate</b>. Re-read the map.</description></item>
///   <item><description><c>Indeterminate</c> — leadership or transport was lost mid-split.</description></item>
///   <item><description><c>InvalidInput</c> / <c>KeyRangeDisabled</c> — malformed request, or a cluster
///     with no data partition to split onto. Determinate.</description></item>
/// </list>
/// </summary>
public sealed class KahunaSplitRangeResponse
{
    /// <summary>True only for <c>Succeeded</c>.</summary>
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    /// <summary>The outcome name; see the type's remarks for the full set.</summary>
    [JsonPropertyName("status")]
    public string Status { get; set; } = "";

    /// <summary>
    /// True when the answer is final: the split either happened or definitely did not. False means
    /// the map may still change — treat the split as unknown and re-read <c>GET /v1/ranges</c>
    /// rather than as failed.
    /// </summary>
    [JsonPropertyName("determinate")]
    public bool Determinate { get; set; }

    /// <summary>The Raft partition now serving the upper half; 0 unless the split succeeded.</summary>
    [JsonPropertyName("newPartitionId")]
    public int NewPartitionId { get; set; }

    /// <summary>The routing generation the cutover committed; 0 unless the split succeeded.</summary>
    [JsonPropertyName("newGeneration")]
    public long NewGeneration { get; set; }

    /// <summary>
    /// Where to retry when this node refused for lack of leadership — best-effort, from the gossiped
    /// hint, and null when nothing is known. A hint, never an authority: the named node re-checks its
    /// own leadership and may refuse in turn.
    /// </summary>
    [JsonPropertyName("leaderHint")]
    public string? LeaderHint { get; set; }

    /// <summary>Why the split did not succeed, and what the caller should do about it.</summary>
    [JsonPropertyName("reason")]
    public string? Reason { get; set; }
}

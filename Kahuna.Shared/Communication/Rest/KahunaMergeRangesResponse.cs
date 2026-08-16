using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Outcome of running the merge pass on demand — the same work the periodic merge checker does,
/// which folds adjacent under-min ranges back into one.
/// <para>
/// The trap this shape exists to close: the underlying trigger answers <c>0</c> on a node that does
/// not lead the partition owning the range map, which is indistinguishable from a leader finding
/// nothing eligible. A bare count would report "0 merges" for both, so a non-leader is refused with
/// <c>NotLeader</c> instead and only a leader ever returns a count.
/// </para>
/// <para><c>status</c> is one of:</para>
/// <list type="bullet">
///   <item><description><c>Completed</c> — the pass ran to completion on this node; <c>merges</c> says
///     how many adjacent pairs it folded, and 0 means nothing was eligible.</description></item>
///   <item><description><c>NotLeader</c> — this node does not lead the partition that owns the range
///     map, so no pass ran. Retry against the leader; <c>leaderHint</c> names it when gossip knows.</description></item>
///   <item><description><c>Indeterminate</c> — the pass failed partway. Merges already committed stay
///     committed and <c>merges</c> is unknown, so re-read <c>GET /v1/ranges</c>.</description></item>
/// </list>
/// <para>
/// There is no per-key-space variant: the pass scans every key-range space, and the merge policy
/// (minimum size) is configuration, not a request parameter — a caller able to override it could
/// fold ranges the running policy considers too large, with nothing recording why.
/// </para>
/// </summary>
public sealed class KahunaMergeRangesResponse
{
    /// <summary>True when the pass ran here; a leader that merged nothing still succeeded.</summary>
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    /// <summary>The outcome name; see the type's remarks for the full set.</summary>
    [JsonPropertyName("status")]
    public string Status { get; set; } = "";

    /// <summary>
    /// True when the answer is final. False means the map may have changed in ways this response
    /// does not describe — re-read <c>GET /v1/ranges</c> rather than trusting <c>merges</c>.
    /// </summary>
    [JsonPropertyName("determinate")]
    public bool Determinate { get; set; }

    /// <summary>
    /// Adjacent pairs folded by this pass. Meaningful only when the pass ran: a refusal reports 0
    /// because nothing was attempted, not because nothing was eligible.
    /// </summary>
    [JsonPropertyName("merges")]
    public int Merges { get; set; }

    /// <summary>
    /// Where to retry when this node refused for lack of leadership — best-effort, from the gossiped
    /// hint, and null when nothing is known.
    /// </summary>
    [JsonPropertyName("leaderHint")]
    public string? LeaderHint { get; set; }

    /// <summary>Why the pass did not run, and what the caller should do about it.</summary>
    [JsonPropertyName("reason")]
    public string? Reason { get; set; }
}

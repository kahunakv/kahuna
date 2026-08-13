using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Result of <c>POST /v1/cluster/leave</c>, which decommissions a running node by committing its
/// removal from the cluster roster. The node keeps serving its port afterwards so the caller can
/// read this answer; stopping the process is the caller's next step.
/// <para>
/// HTTP status carries the same information for callers that only look at the code: 200 when the
/// node is out of the roster (committed, or already absent), 409 when the removal was permanently
/// refused, 503 when the node could not attempt it, and 504 when the attempt did not resolve in
/// time. Only 504 is worth retrying — see <see cref="Retryable"/>.
/// </para>
/// </summary>
public sealed class KahunaClusterLeaveResponse
{
    /// <summary>True when the node is no longer part of the committed roster.</summary>
    [JsonPropertyName("left")]
    public bool Left { get; set; }

    /// <summary>
    /// What happened, as the outcome name reported by consensus: <c>Committed</c>, <c>NotAMember</c>,
    /// <c>RefusedInsufficientVoters</c>, <c>NotInitialized</c>, <c>NoLeader</c> or <c>Timeout</c>.
    /// </summary>
    [JsonPropertyName("outcome")]
    public string Outcome { get; set; } = "";

    /// <summary>
    /// The roster version resulting from the request — for a committed removal, the version that no
    /// longer lists this node. 0 when no roster version is known locally.
    /// </summary>
    [JsonPropertyName("membershipVersion")]
    public long MembershipVersion { get; set; }

    /// <summary>
    /// Whether repeating the request could produce a different answer. False for a permanent refusal
    /// (removing the last voter would strand the cluster) and for outcomes that already succeeded.
    /// </summary>
    [JsonPropertyName("retryable")]
    public bool Retryable { get; set; }

    /// <summary>Human-readable explanation, for operator logs and CLI output.</summary>
    [JsonPropertyName("reason")]
    public string Reason { get; set; } = "";
}

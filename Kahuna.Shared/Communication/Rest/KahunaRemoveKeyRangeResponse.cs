using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Outcome of removing a key space's descriptors from the replicated range map — the inverse of
/// registration, used for teardown.
/// <para><c>status</c> is one of:</para>
/// <list type="bullet">
///   <item><description><c>Removed</c> — the space carries no descriptors here, whether this call
///     removed them or there were none (removal is idempotent).</description></item>
///   <item><description><c>Indeterminate</c> — the removal was accepted but descriptors are still
///     visible on this node. Committed-and-not-yet-applied looks exactly like this, so re-read
///     <c>GET /v1/ranges</c> rather than treating it as a failure.</description></item>
///   <item><description><c>QuiesceWindowOpen</c> — refused because a split is mid-cutover; the
///     window is short. Retry.</description></item>
///   <item><description><c>InvalidInput</c> — empty key space, or a <c>/meta</c> schema-log space
///     (which is never key-range routed, so there is nothing to remove).</description></item>
///   <item><description><c>KeyRangeDisabled</c> — the cluster has no data partition, so no space was
///     ever key-range routed. Permanent; do not retry.</description></item>
/// </list>
/// </summary>
public sealed class KahunaRemoveKeyRangeResponse
{
    /// <summary>True when the space carries no descriptors on the answering node after this call.</summary>
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    /// <summary>The outcome name; see the type's remarks for the full set.</summary>
    [JsonPropertyName("status")]
    public string Status { get; set; } = "";

    /// <summary>
    /// How the answering node routes the space after this call. Removal clears the mode through the
    /// normal replication path, so it can still read <c>"KeyRange"</c> for the moment between the
    /// commit and this node applying it.
    /// </summary>
    [JsonPropertyName("routingMode")]
    public string RoutingMode { get; set; } = "";

    /// <summary>Descriptors still visible for the space on the answering node; 0 on success.</summary>
    [JsonPropertyName("descriptorCount")]
    public int DescriptorCount { get; set; }

    /// <summary>Why the call did not fully succeed, and what the caller should do about it.</summary>
    [JsonPropertyName("reason")]
    public string? Reason { get; set; }
}

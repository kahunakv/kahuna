using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Outcome of registering a key space for key-range routing.
/// <para>
/// Registration has two halves that do not travel together, and the response reports both because a
/// caller cannot infer either one. The <b>routing-mode flip is node-local and unreplicated</b>, so
/// this call must be sent to <b>every</b> node; sending it to one leaves a cluster where that node
/// routes the space by key range and the rest still hash it. The <b>seed descriptor</b> is a single
/// replicated meta-partition write, forwarded to that partition's leader when the answering node is
/// not it — so a follower legitimately succeeds here.
/// </para>
/// <para><c>status</c> is one of:</para>
/// <list type="bullet">
///   <item><description><c>Seeded</c> — this call committed the whole-space descriptor.</description></item>
///   <item><description><c>AlreadySeeded</c> — a descriptor already existed (this call registered the
///     mode on this node, which is still work worth doing).</description></item>
///   <item><description><c>Indeterminate</c> — the mode was flipped but no descriptor is visible here
///     yet. The seed may have committed on the leader and not replicated within the wait, so this is
///     not a failure: re-read <c>GET /v1/ranges</c> before concluding anything.</description></item>
///   <item><description><c>InvalidInput</c> — empty key space, or a <c>/meta</c> schema-log space.</description></item>
///   <item><description><c>KeyRangeDisabled</c> — the cluster has no data partition to seed onto, so
///     key-range routing cannot be enabled at all. Permanent; do not retry.</description></item>
/// </list>
/// </summary>
public sealed class KahunaRegisterKeyRangeResponse
{
    /// <summary>True only when the space is registered here and covered by a descriptor.</summary>
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    /// <summary>The outcome name; see the type's remarks for the full set.</summary>
    [JsonPropertyName("status")]
    public string Status { get; set; } = "";

    /// <summary>
    /// True only when <b>this</b> call committed the seed descriptor. False when another caller (or
    /// another node) had already seeded it — which is a success, just not this call's doing.
    /// </summary>
    [JsonPropertyName("seeded")]
    public bool Seeded { get; set; }

    /// <summary>
    /// How the answering node routes the space after this call: <c>"KeyRange"</c> once registered,
    /// <c>"Hash"</c> if it was refused. Node-local — other nodes are unaffected by this call.
    /// </summary>
    [JsonPropertyName("routingMode")]
    public string RoutingMode { get; set; } = "";

    /// <summary>
    /// How many descriptors the answering node sees for the space. A registered space with zero
    /// descriptors routes by key range with nothing to route to, and every write to it throws.
    /// </summary>
    [JsonPropertyName("descriptorCount")]
    public int DescriptorCount { get; set; }

    /// <summary>Why the call did not fully succeed, and what the caller should do about it.</summary>
    [JsonPropertyName("reason")]
    public string? Reason { get; set; }
}

using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaClusterMembershipResponse
{
    [JsonPropertyName("membershipVersion")]
    public long MembershipVersion { get; set; }

    [JsonPropertyName("members")]
    public List<KahunaClusterMemberResponse> Members { get; set; } = [];

    [JsonPropertyName("localRole")]
    public string LocalRole { get; set; } = "";

    /// <summary>
    /// Whether the node has completed cluster initialization. Until this is true the node cannot
    /// resolve partition leaders and refuses every key/value request, even though it already
    /// answers membership queries (and may report a Voter role) — so callers routing by liveness
    /// must consult this flag, not the role.
    /// </summary>
    [JsonPropertyName("initialized")]
    public bool Initialized { get; set; }
}

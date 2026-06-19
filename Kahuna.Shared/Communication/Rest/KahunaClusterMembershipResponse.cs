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
}

using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaRenewSnapshotHoldRequest
{
    [JsonPropertyName("holdId")]
    public string HoldId { get; set; } = "";

    [JsonPropertyName("leaseMs")]
    public int LeaseMs { get; set; }
}

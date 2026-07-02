using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaReleaseSnapshotHoldRequest
{
    [JsonPropertyName("holdId")]
    public string HoldId { get; set; } = "";
}

using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaReleaseSnapshotHoldResponse
{
    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }
}

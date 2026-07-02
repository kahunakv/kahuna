using Kommander.Time;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaGetSnapshotFloorResponse
{
    [JsonPropertyName("effectiveFloor")]
    public HLCTimestamp EffectiveFloor { get; set; }

    [JsonPropertyName("liveHolds")]
    public int LiveHolds { get; set; }
}

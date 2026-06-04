using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaDeleteManyKeyValueRequest
{
    [JsonPropertyName("items")]
    public List<KahunaDeleteKeyValueRequestItem>? Items { get; set; }
}

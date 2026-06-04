using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaSetManyKeyValueRequest
{
    [JsonPropertyName("items")]
    public List<KahunaSetKeyValueRequestItem>? Items { get; set; }
}

using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

public sealed class KahunaDeleteManyKeyValueRequest
{
    [JsonPropertyName("items")]
    public List<KahunaDeleteKeyValueRequestItem>? Items { get; set; }

    /// <summary>
    /// The whole batch registers as one coordinator operation so its confirmed persistent keys anchor
    /// the transaction record deterministically. Absent for the non-transactional batch path.
    /// </summary>
    [JsonPropertyName("coordinatorKey")]
    public string? CoordinatorKey { get; set; }

    [JsonPropertyName("operationIdHigh")]
    public ulong OperationIdHigh { get; set; }

    [JsonPropertyName("operationIdLow")]
    public ulong OperationIdLow { get; set; }
}

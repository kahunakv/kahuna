
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Batched point-read request, shared by the get-many and exists-many endpoints. Both carry the same
/// shape: a transaction id, an optional snapshot timestamp, and the per-key items.
/// </summary>
public sealed class KahunaManyKeyValuesRequest
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }

    /// <summary>
    /// Snapshot to read as of. Zero reads the latest committed value.
    /// </summary>
    [JsonPropertyName("readTimestamp")]
    public HLCTimestamp ReadTimestamp { get; set; }

    [JsonPropertyName("items")]
    public List<KahunaGetManyKeyValuesRequestItem>? Items { get; set; }
}

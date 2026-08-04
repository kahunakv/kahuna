
using Kahuna.Shared.KeyValue;
using System.Text.Json.Serialization;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// One page of a range scan. A caller that received a <c>nextCursor</c> from a previous page sends it
/// back verbatim in <see cref="Cursor"/> to resume; the cursor is opaque to the client and is decoded
/// server-side, which also carries the snapshot forward so every page of a scan sees one view.
/// </summary>
public sealed class KahunaGetByRangeRequest
{
    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }

    [JsonPropertyName("prefix")]
    public string? Prefix { get; set; }

    [JsonPropertyName("startKey")]
    public string? StartKey { get; set; }

    [JsonPropertyName("startInclusive")]
    public bool StartInclusive { get; set; }

    [JsonPropertyName("endKey")]
    public string? EndKey { get; set; }

    [JsonPropertyName("endInclusive")]
    public bool EndInclusive { get; set; }

    [JsonPropertyName("limit")]
    public int Limit { get; set; }

    /// <summary>
    /// Snapshot to read as of. Zero reads the latest committed value.
    /// </summary>
    [JsonPropertyName("readTimestamp")]
    public HLCTimestamp ReadTimestamp { get; set; }

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; }

    /// <summary>
    /// Opaque resume token from a previous page's <c>nextCursor</c>. Null starts a fresh scan.
    /// </summary>
    [JsonPropertyName("cursor")]
    public string? Cursor { get; set; }

    [JsonPropertyName("coordinatorKey")]
    public string? CoordinatorKey { get; set; }

    [JsonPropertyName("operationIdHigh")]
    public ulong OperationIdHigh { get; set; }

    [JsonPropertyName("operationIdLow")]
    public ulong OperationIdLow { get; set; }
}

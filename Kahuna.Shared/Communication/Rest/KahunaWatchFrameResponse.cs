using System.Text.Json.Serialization;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// One frame of a watch on the REST surface, sent as the <c>data</c> line of a server-sent event.
/// <see cref="Frame"/> says which other fields carry data. <see cref="Type"/> is the opening's answer:
/// <see cref="KeyValueResponseType.Get"/> on an opened watch, the refusal on a refused one. The REST surface
/// answers a request it could not start with a plain JSON body that has only <c>type</c>, so a client reads the
/// content type before it reads frames.
/// </summary>
public sealed class KahunaWatchFrameResponse
{
    [JsonPropertyName("frame")]
    public KeyValueWatchFrameType Frame { get; set; }

    [JsonPropertyName("type")]
    public KeyValueResponseType Type { get; set; }

    [JsonPropertyName("partitionId")]
    public int PartitionId { get; set; }

    [JsonPropertyName("key")]
    public string? Key { get; set; }

    [JsonPropertyName("kind")]
    public KeyValueWatchEventKind Kind { get; set; }

    // A delete, an extend or a keys-only event has no value, which must stay apart from an empty value.
    [JsonPropertyName("value")]
    [JsonConverter(typeof(KeyValuePayloadJsonConverter))]
    public byte[]? Value { get; set; }

    [JsonPropertyName("revision")]
    public long Revision { get; set; }

    [JsonPropertyName("expires")]
    public HLCTimestamp Expires { get; set; }

    [JsonPropertyName("commitTimestamp")]
    public HLCTimestamp CommitTimestamp { get; set; }

    [JsonPropertyName("transactionId")]
    public HLCTimestamp TransactionId { get; set; }

    /// <summary>What produced the transaction that wrote the key; null for an ordinary write.</summary>
    [JsonPropertyName("origin")]
    public string? Origin { get; set; }

    [JsonPropertyName("timestamp")]
    public HLCTimestamp Timestamp { get; set; }

    [JsonPropertyName("lagReason")]
    public KeyValueWatchLagReason LagReason { get; set; }

    [JsonPropertyName("partitions")]
    public List<int>? Partitions { get; set; }

    [JsonPropertyName("routes")]
    public List<KahunaWatchRouteResponse>? Routes { get; set; }
}

/// <summary>Where a refused watch's partition is hosted (see <see cref="KeyValueWatchRoute"/>).</summary>
public sealed class KahunaWatchRouteResponse
{
    [JsonPropertyName("partitionId")]
    public int PartitionId { get; set; }

    [JsonPropertyName("hostedHere")]
    public bool HostedHere { get; set; }

    [JsonPropertyName("endpoints")]
    public List<string>? Endpoints { get; set; }
}

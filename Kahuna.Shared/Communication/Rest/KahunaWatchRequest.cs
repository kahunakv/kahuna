using System.Text.Json.Serialization;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// The body of <c>POST /v1/kv/watch</c>. The response is a server-sent-events stream of
/// <see cref="KahunaWatchFrameResponse"/>: first an <c>Opened</c> or a <c>Refused</c> frame, then the
/// watch's frames until a terminal frame or until the client closes the request.
/// </summary>
public sealed class KahunaWatchRequest
{
    [JsonPropertyName("scopeKind")]
    public KeyValueWatchScopeKind ScopeKind { get; set; }

    /// <summary>The key, the prefix, or the key space of a range.</summary>
    [JsonPropertyName("scopeValue")]
    public string? ScopeValue { get; set; }

    /// <summary>Inclusive lower bound of a range; null is unbounded.</summary>
    [JsonPropertyName("startKey")]
    public string? StartKey { get; set; }

    /// <summary>Exclusive upper bound of a range; null is unbounded.</summary>
    [JsonPropertyName("endKey")]
    public string? EndKey { get; set; }

    [JsonPropertyName("durability")]
    public KeyValueDurability Durability { get; set; } = KeyValueDurability.Persistent;

    [JsonPropertyName("keysOnly")]
    public bool KeysOnly { get; set; }

    [JsonPropertyName("capacity")]
    public int Capacity { get; set; }

    /// <summary>Limits the watch to these partitions of the scope; null watches every partition.</summary>
    [JsonPropertyName("partitions")]
    public List<int>? Partitions { get; set; }

    /// <summary>Replays every retained event above this timestamp before the live events, on every partition
    /// <see cref="ResumePartitions"/> does not name; zero is live only.</summary>
    [JsonPropertyName("resumeFrom")]
    public HLCTimestamp ResumeFrom { get; set; }

    /// <summary>A cursor per partition, overriding <see cref="ResumeFrom"/> for the partitions it names.</summary>
    [JsonPropertyName("resumePartitions")]
    public List<KahunaWatchCursor>? ResumePartitions { get; set; }
}

/// <summary>The resume cursor of one partition.</summary>
public sealed class KahunaWatchCursor
{
    [JsonPropertyName("partitionId")]
    public int PartitionId { get; set; }

    [JsonPropertyName("timestamp")]
    public HLCTimestamp Timestamp { get; set; }
}

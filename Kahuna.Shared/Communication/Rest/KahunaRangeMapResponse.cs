using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// The range-descriptor map as the answering node has applied it, served by <c>GET /v1/ranges</c>.
/// <para>
/// Two different things are reported side by side, and they do not travel together. The
/// <b>descriptors</b> are replicated on the meta partition, so every node converges on them; the
/// <b>routing mode</b> is node-local in-memory state set by registering a key space, and is never
/// replicated. Asking a second node can therefore change <c>routingMode</c> while leaving the
/// descriptors identical — that is the expected shape of a key space registered on some nodes and
/// not others, not an inconsistency.
/// </para>
/// </summary>
public sealed class KahunaRangeMapResponse
{
    /// <summary>
    /// Whether cluster initialization has completed. Until true this node has not necessarily
    /// applied the meta partition, so an empty key-space list means "not known yet" rather than
    /// "no ranges exist".
    /// </summary>
    [JsonPropertyName("initialized")]
    public bool Initialized { get; set; }

    /// <summary>The answering node's endpoint, so a node-local routing mode can be attributed.</summary>
    [JsonPropertyName("localEndpoint")]
    public string LocalEndpoint { get; set; } = "";

    /// <summary>
    /// One entry per key space, ordinal-ordered by name. Includes key spaces that are registered on
    /// this node but hold no descriptor yet, so "registered but unseeded" is distinguishable from
    /// "not registered".
    /// </summary>
    [JsonPropertyName("keySpaces")]
    public List<KahunaKeySpaceRangesResponse> KeySpaces { get; set; } = [];
}

/// <summary>One key space's descriptors, plus how the answering node routes it.</summary>
public sealed class KahunaKeySpaceRangesResponse
{
    /// <summary>The key space: a key's prefix up to (excluding) its last <c>'/'</c>.</summary>
    [JsonPropertyName("keySpace")]
    public string KeySpace { get; set; } = "";

    /// <summary>
    /// <c>"KeyRange"</c> when this node routes the space by key order, <c>"Hash"</c> when it hashes
    /// it. Node-local and unreplicated: a space can carry descriptors here and still be
    /// <c>"Hash"</c> on a node where it was never registered.
    /// </summary>
    [JsonPropertyName("routingMode")]
    public string RoutingMode { get; set; } = "";

    /// <summary>
    /// The descriptors covering this space, ordered by <c>startKey</c> (ordinal, null first). Empty
    /// when the space is registered but its whole-space descriptor has not been seeded yet.
    /// </summary>
    [JsonPropertyName("descriptors")]
    public List<KahunaRangeDescriptorResponse> Descriptors { get; set; } = [];
}

/// <summary>
/// One contiguous range: every key in <c>[startKey, endKey)</c> is owned by <c>partitionId</c> at
/// routing <c>generation</c>. Bounds are half-open and compared <b>ordinally</b> — a consumer that
/// checks coverage with a culture-aware comparison will read gaps that are not there.
/// </summary>
public sealed class KahunaRangeDescriptorResponse
{
    /// <summary>Inclusive lower bound. Null means −infinity within the key space.</summary>
    [JsonPropertyName("startKey")]
    public string? StartKey { get; set; }

    /// <summary>Exclusive upper bound. Null means +infinity within the key space.</summary>
    [JsonPropertyName("endKey")]
    public string? EndKey { get; set; }

    /// <summary>The Raft partition currently serving this range.</summary>
    [JsonPropertyName("partitionId")]
    public int PartitionId { get; set; }

    /// <summary>Bumped on every split/merge/move; fences stale routing.</summary>
    [JsonPropertyName("generation")]
    public long Generation { get; set; }
}

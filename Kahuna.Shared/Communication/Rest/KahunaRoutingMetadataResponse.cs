using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Scoped routing metadata served by <c>GET /v1/cluster/routing</c>: enough for a client to
/// resolve a resource it has never seen to its partition without a round trip, plus the advisory
/// leader of each partition.
///
/// <para>
/// Ownership and leadership are kept apart on purpose. A leader change replaces entries in
/// <see cref="Leaders"/> only; a split, merge or move replaces entries in <see cref="KeySpaces"/>
/// only. Mixing the two lets one kind of change invalidate the other kind of answer.
/// </para>
///
/// <para>
/// The whole payload is advisory. Public handlers re-resolve every key, re-apply live range
/// fences and re-check leadership, so metadata that is stale or wrong costs a forward and never
/// an incorrect result.
/// </para>
/// </summary>
public sealed class KahunaRoutingMetadataResponse
{
    /// <summary>
    /// Whether cluster initialization has completed. Until true the map is not usable and an empty
    /// key-space list means "not known yet" rather than "no ranges exist".
    /// </summary>
    [JsonPropertyName("initialized")]
    public bool Initialized { get; set; }

    /// <summary>Contract version of this payload. A client that does not recognise it falls back.</summary>
    [JsonPropertyName("schemaVersion")]
    public int SchemaVersion { get; set; }

    /// <summary>
    /// Identifier of the hash algorithm applied to hash-routed key spaces. A client that does not
    /// implement exactly this identifier must fall back rather than approximate it.
    /// </summary>
    [JsonPropertyName("hashAlgorithm")]
    public string HashAlgorithm { get; set; } = "";

    /// <summary>
    /// The character whose last occurrence in a key ends its key space. A key without it is its own
    /// key space.
    /// </summary>
    [JsonPropertyName("prefixSeparator")]
    public string PrefixSeparator { get; set; } = "";

    /// <summary>
    /// The character whose first occurrence in a key space ends its placement group — the string the
    /// hash actually runs over. A key space without it is its own group. Key spaces that name the same
    /// group hash to the same partition.
    /// </summary>
    [JsonPropertyName("groupSeparator")]
    public string GroupSeparator { get; set; } = "";

    /// <summary>Number of buckets the hash maps onto.</summary>
    [JsonPropertyName("hashPoolSize")]
    public int HashPoolSize { get; set; }

    /// <summary>Partition id of bucket 0; bucket <c>b</c> is partition <c>offset + b</c>.</summary>
    [JsonPropertyName("hashPartitionOffset")]
    public int HashPartitionOffset { get; set; }

    /// <summary>
    /// Format of a named sequence's storage key, with <c>{0}</c> standing for the name. Sequences
    /// route by the partition of this key, not by a hash of the bare name.
    /// </summary>
    [JsonPropertyName("sequenceStorageKeyFormat")]
    public string SequenceStorageKeyFormat { get; set; } = "";

    /// <summary>Key prefix reserved for server-managed records; a client routes nothing under it.</summary>
    [JsonPropertyName("reservedKeyPrefix")]
    public string ReservedKeyPrefix { get; set; } = "";

    /// <summary>The answering node's own advertised client endpoint.</summary>
    [JsonPropertyName("localEndpoint")]
    public string LocalEndpoint { get; set; } = "";

    /// <summary>
    /// Identity of the committed view <see cref="KeySpaces"/> was read from. Two responses with the
    /// same identity describe the same map.
    /// </summary>
    [JsonPropertyName("snapshotVersion")]
    public long SnapshotVersion { get; set; }

    /// <summary>
    /// False when the answering node could not produce one coherent view because a registration,
    /// split, merge or move ran across the read. The key spaces must then be discarded, never merged.
    /// </summary>
    [JsonPropertyName("coherent")]
    public bool Coherent { get; set; }

    [JsonPropertyName("keySpaces")]
    public List<KahunaRoutingKeySpaceResponse> KeySpaces { get; set; } = [];

    [JsonPropertyName("leaders")]
    public List<KahunaPartitionLeaderResponse> Leaders { get; set; } = [];
}

/// <summary>One key space's routing mode and, when it routes by key range, its descriptors.</summary>
public sealed class KahunaRoutingKeySpaceResponse
{
    [JsonPropertyName("keySpace")]
    public string KeySpace { get; set; } = "";

    /// <summary>
    /// <c>KeyRange</c> or <c>Hash</c>, as the answering node routes it. Node-local and unreplicated,
    /// so two nodes may legitimately disagree.
    /// </summary>
    [JsonPropertyName("routingMode")]
    public string RoutingMode { get; set; } = "";

    /// <summary>Ordered by start key, ordinally, the way the server's own router searches them.</summary>
    [JsonPropertyName("ranges")]
    public List<KahunaRoutingRangeResponse> Ranges { get; set; } = [];
}

/// <summary>One range descriptor. A null bound is an open end, not an empty-string bound.</summary>
public sealed class KahunaRoutingRangeResponse
{
    [JsonPropertyName("startKey")]
    public string? StartKey { get; set; }

    [JsonPropertyName("endKey")]
    public string? EndKey { get; set; }

    [JsonPropertyName("partitionId")]
    public int PartitionId { get; set; }

    [JsonPropertyName("generation")]
    public long Generation { get; set; }
}

/// <summary>One partition's advisory leader. An empty endpoint means the leader is unknown.</summary>
public sealed class KahunaPartitionLeaderResponse
{
    [JsonPropertyName("partitionId")]
    public int PartitionId { get; set; }

    [JsonPropertyName("endpoint")]
    public string Endpoint { get; set; } = "";
}

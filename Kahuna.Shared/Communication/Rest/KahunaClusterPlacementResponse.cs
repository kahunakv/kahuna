using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Per-partition replica placement served by <c>GET /v1/cluster/placement</c>: which nodes host
/// each partition and in what role, the effective replication factor, and which partitions the
/// answering node hosts locally. Under full replication (replication factor 0) every replica set
/// is empty and every partition is hosted locally.
/// </summary>
public sealed class KahunaClusterPlacementResponse
{
    /// <summary>The node's globally configured replication factor. 0 means full replication.</summary>
    [JsonPropertyName("replicationFactor")]
    public int ReplicationFactor { get; set; }

    /// <summary>Whether the placement rebalancer moves replicas automatically on this node's configuration.</summary>
    [JsonPropertyName("rebalancerEnabled")]
    public bool RebalancerEnabled { get; set; }

    /// <summary>Whether cluster initialization has completed. Until true the partition list may be empty.</summary>
    [JsonPropertyName("initialized")]
    public bool Initialized { get; set; }

    /// <summary>The answering node's endpoint, so hosted flags can be attributed.</summary>
    [JsonPropertyName("localEndpoint")]
    public string LocalEndpoint { get; set; } = "";

    /// <summary>How many of the listed partitions the answering node hosts locally.</summary>
    [JsonPropertyName("hostedPartitionCount")]
    public int HostedPartitionCount { get; set; }

    /// <summary>One entry per data partition in the committed map.</summary>
    [JsonPropertyName("partitions")]
    public List<KahunaPartitionPlacementResponse> Partitions { get; set; } = [];
}

/// <summary>One data partition's placement in the committed map.</summary>
public sealed class KahunaPartitionPlacementResponse
{
    [JsonPropertyName("partitionId")]
    public int PartitionId { get; set; }

    /// <summary>Lifecycle state of the range (Active, Draining, Removed, …).</summary>
    [JsonPropertyName("state")]
    public string State { get; set; } = "";

    /// <summary>
    /// The range's committed generation; bumps on splits/merges and replica changes, so two
    /// snapshots with the same generation describe the same placement.
    /// </summary>
    [JsonPropertyName("generation")]
    public long Generation { get; set; }

    /// <summary>
    /// The replication factor in effect for this partition: its per-range override when one is
    /// set, otherwise the global factor. 0 means full replication.
    /// </summary>
    [JsonPropertyName("effectiveReplicationFactor")]
    public int EffectiveReplicationFactor { get; set; }

    /// <summary>Whether the answering node hosts (materializes) this partition locally.</summary>
    [JsonPropertyName("hostedLocally")]
    public bool HostedLocally { get; set; }

    /// <summary>
    /// The committed replica set. Empty means legacy full replication: every roster voter hosts
    /// the range.
    /// </summary>
    [JsonPropertyName("replicas")]
    public List<KahunaPartitionReplicaResponse> Replicas { get; set; } = [];
}

/// <summary>One replica of a partition: the hosting node and its role in the range's consensus group.</summary>
public sealed class KahunaPartitionReplicaResponse
{
    [JsonPropertyName("endpoint")]
    public string Endpoint { get; set; } = "";

    /// <summary>Voter (counts toward quorum), Learner (catching up), or Removing (on its way out).</summary>
    [JsonPropertyName("role")]
    public string Role { get; set; } = "";
}

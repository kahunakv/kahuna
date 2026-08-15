using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Request body for <c>POST /v1/cluster/replication-factor</c>: commits a per-partition
/// replication-factor override. 0 clears the override so the partition inherits the global
/// configuration. Leader-only: the receiving node must lead the meta partition; a follower
/// refuses and the caller retries against the leader.
/// </summary>
public sealed class KahunaSetReplicationFactorRequest
{
    [JsonPropertyName("partitionId")]
    public int PartitionId { get; set; }

    /// <summary>The new per-partition target. 0 clears the override (inherit the global factor).</summary>
    [JsonPropertyName("replicationFactor")]
    public int ReplicationFactor { get; set; }
}

/// <summary>
/// Outcome of a replication-factor override. The change adjusts the placement target only — the
/// rebalancer moves replicas toward it on later passes — so success means the override is
/// committed, not that replicas have moved yet.
/// </summary>
public sealed class KahunaSetReplicationFactorResponse
{
    /// <summary>True when the override was committed to the partition map.</summary>
    [JsonPropertyName("success")]
    public bool Success { get; set; }

    /// <summary>The commit outcome (Success, or the refusing status).</summary>
    [JsonPropertyName("status")]
    public string Status { get; set; } = "";

    /// <summary>The range's committed generation after the change; 0 when refused.</summary>
    [JsonPropertyName("generation")]
    public long Generation { get; set; }

    /// <summary>Why the override was refused; null on success.</summary>
    [JsonPropertyName("reason")]
    public string? Reason { get; set; }
}

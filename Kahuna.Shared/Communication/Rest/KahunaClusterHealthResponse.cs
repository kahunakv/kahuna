using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Readiness report served by <c>GET /v1/cluster/health</c>. A node is ready only when it has
/// completed cluster initialization and holds a serving role in the roster; until then it will
/// refuse every key/value request, so load balancers and orchestrator probes should route
/// traffic elsewhere. The endpoint answers HTTP 200 when ready and 503 when not.
/// </summary>
public sealed class KahunaClusterHealthResponse
{
    /// <summary>True when the node can serve requests. Mirrors the HTTP status (200 vs 503).</summary>
    [JsonPropertyName("ready")]
    public bool Ready { get; set; }

    /// <summary>Whether cluster initialization has completed (partition map received and applied).</summary>
    [JsonPropertyName("initialized")]
    public bool Initialized { get; set; }

    /// <summary>The node's role in the cluster roster (Voter, Learner, Leaving, NotMember).</summary>
    [JsonPropertyName("localRole")]
    public string LocalRole { get; set; } = "";

    /// <summary>
    /// How many data partitions this node hosts locally. Informational only — it never gates
    /// readiness: under per-partition replica placement a node hosting zero partitions still
    /// serves every key by forwarding to the hosting nodes. 0 while uninitialized.
    /// </summary>
    [JsonPropertyName("hostedPartitions")]
    public int HostedPartitions { get; set; }

    /// <summary>
    /// Set when the process observed a fault it cannot vouch for surviving (an out-of-memory in the
    /// WAL/apply pipeline) and was configured not to fail fast on it. A node in this state answers 503 for
    /// the rest of its life even though it stays reachable: its replication pipeline may be dead, and the
    /// orchestrator should replace it. Null on a healthy node.
    /// </summary>
    [JsonPropertyName("fatalFault")]
    public string? FatalFault { get; set; }

    /// <summary>
    /// Set when the node's key-value and lock store is fail-stopped on storage damage (the engine reported a
    /// <c>Corruption</c> status: a block or file whose checksum did not match), or when the store was found
    /// empty at start while the node's Raft log holds history (the store directory was lost or replaced). In
    /// the first case the node answers <c>StorageCorrupt</c> to every read and write of the partitions it
    /// hosts and has withheld its candidacy for them; in the second its partitions are rebuilt from their
    /// peers (or stay quarantined where none exist). A node in either state answers 503 until an operator
    /// repairs or replaces the store and restarts it. Null on a healthy node.
    /// </summary>
    [JsonPropertyName("storageFault")]
    public string? StorageFault { get; set; }

    /// <summary>
    /// Hosted partitions whose Raft storage verdict is anything but healthy or restoring, one line per
    /// partition naming the verdict and its evidence: a log that lost acknowledged entries, an application
    /// floor below the WAL's compaction floor (the store is missing a prefix the WAL no longer holds; the
    /// partition is being rebuilt by snapshot), a fail-stopped WAL engine, or a failed restore. Such a
    /// partition is not served from this node, so the node answers 503 while the list is non-empty.
    /// Null or empty on a healthy node.
    /// </summary>
    [JsonPropertyName("partitionStorageFaults")]
    public List<string>? PartitionStorageFaults { get; set; }
}

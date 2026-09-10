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
}

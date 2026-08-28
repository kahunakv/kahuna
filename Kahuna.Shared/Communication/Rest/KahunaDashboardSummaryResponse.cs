using System.Text.Json.Serialization;

namespace Kahuna.Shared.Communication.Rest;

/// <summary>
/// Node identity, readiness and storage, in the one payload the operator dashboard's top band
/// renders. Served by <c>GET /v1/dashboard/summary</c>.
///
/// <para>Every field describes <b>this node</b>. Membership version and replication factor come
/// from replicated state and therefore agree across the cluster; everything else is node-local, so
/// a fleet is read by opening the dashboard on each node.</para>
/// </summary>
public sealed class KahunaDashboardSummaryResponse
{
    /// <summary>The node's own Raft endpoint. Empty while the node is too early in boot to have one.</summary>
    [JsonPropertyName("localEndpoint")]
    public string LocalEndpoint { get; set; } = "";

    /// <summary>The configured node name.</summary>
    [JsonPropertyName("nodeName")]
    public string NodeName { get; set; } = "";

    /// <summary>The node's role in the cluster roster (Voter, Learner, Leaving, NotMember).</summary>
    [JsonPropertyName("localRole")]
    public string LocalRole { get; set; } = "";

    /// <summary>Whether cluster initialization has completed (partition map received and applied).</summary>
    [JsonPropertyName("initialized")]
    public bool Initialized { get; set; }

    /// <summary>True when the node can serve requests. Same condition the readiness probe reports.</summary>
    [JsonPropertyName("ready")]
    public bool Ready { get; set; }

    /// <summary>How many data partitions this node hosts locally.</summary>
    [JsonPropertyName("hostedPartitions")]
    public int HostedPartitions { get; set; }

    /// <summary>Partitions in the committed map, hosted here or not.</summary>
    [JsonPropertyName("totalPartitions")]
    public int TotalPartitions { get; set; }

    /// <summary>False when the node runs standalone on phantom witnesses rather than real peers.</summary>
    [JsonPropertyName("clusterMode")]
    public bool ClusterMode { get; set; }

    /// <summary>Members in the roster, including this node.</summary>
    [JsonPropertyName("memberCount")]
    public int MemberCount { get; set; }

    /// <summary>Roster version, so a stale membership view is visible rather than silent.</summary>
    [JsonPropertyName("membershipVersion")]
    public long MembershipVersion { get; set; }

    /// <summary>Cluster-wide replication factor. 0 means full replication — every node hosts every partition.</summary>
    [JsonPropertyName("replicationFactor")]
    public int ReplicationFactor { get; set; }

    /// <summary>The key-value and locks storage backend (rocksdb, sqlite, memory).</summary>
    [JsonPropertyName("storage")]
    public string Storage { get; set; } = "";

    /// <summary>Resolved storage directory, or an empty string when the backend holds no files.</summary>
    [JsonPropertyName("storagePath")]
    public string StoragePath { get; set; } = "";

    /// <summary>The Raft write-ahead-log backend (rocksdb, sqlite, memory).</summary>
    [JsonPropertyName("walStorage")]
    public string WalStorage { get; set; } = "";

    /// <summary>Resolved write-ahead-log directory, or an empty string when the backend holds no files.</summary>
    [JsonPropertyName("walPath")]
    public string WalPath { get; set; } = "";

    /// <summary>Whether this node has a backup root configured. The backups panel is empty without one.</summary>
    [JsonPropertyName("backupConfigured")]
    public bool BackupConfigured { get; set; }

    /// <summary>Server assembly version.</summary>
    [JsonPropertyName("version")]
    public string Version { get; set; } = "";

    /// <summary>Seconds since process start. A restart resets every counter the engine panel shows.</summary>
    [JsonPropertyName("uptimeSeconds")]
    public long UptimeSeconds { get; set; }

    /// <summary>Managed heap in bytes, as the runtime last measured it.</summary>
    [JsonPropertyName("heapBytes")]
    public long HeapBytes { get; set; }

    /// <summary>Threads in the process. A number that climbs with load and never falls is worth chasing.</summary>
    [JsonPropertyName("threadCount")]
    public int ThreadCount { get; set; }

    /// <summary>How often the page should poll, in seconds.</summary>
    [JsonPropertyName("refreshSeconds")]
    public int RefreshSeconds { get; set; }
}

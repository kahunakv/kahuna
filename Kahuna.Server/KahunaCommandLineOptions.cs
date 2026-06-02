
using CommandLine;

namespace Kahuna;

/// <summary>
/// Represents the available command-line options for configuring the Kahuna service.
/// This class provides a range of configuration parameters, such as host and port bindings,
/// storage options, and cluster settings, all of which can be used to dictate how the service operates.
/// </summary>
public sealed class KahunaCommandLineOptions
{
    [Option('h', "host", Required = false, HelpText = "Host to bind incoming connections to", Default = "*")]
    public string Host { get; set; } = "*";

    [Option('p', "http-ports", Required = false, HelpText = "Ports to bind incoming HTTP connections to")]
    public IEnumerable<string>? HttpPorts { get; set; }
    
    [Option("https-ports", Required = false, HelpText = "Ports to bind incoming HTTPs connections to")]
    public IEnumerable<string>? HttpsPorts { get; set; }
    
    [Option("https-certificate", Required = false, HelpText = "Path to the HTTPs certificate")]
    public string HttpsCertificate { get; set; } = "";

    [Option("https-certificate-password", Required = false, HelpText = "Password of the HTTPs certificate", Default = "")]
    public string HttpsCertificatePassword { get; set; } = "";
    
    [Option("storage", Required = false, HelpText = "Storage (rocksdb, sqlite)", Default = "rocksdb")]
    public string Storage { get; set; } = "";
    
    [Option("storage-path", Required = false, HelpText = "Storage path")]
    public string StoragePath { get; set; } = "";
    
    [Option("storage-revision", Required = false, HelpText = "Storage revision")]
    public string StorageRevision{ get; set; } = "";
    
    [Option("wal-storage", Required = false, HelpText = "WAL storage (rocksdb, sqlite)", Default = "rocksdb")]
    public string WalStorage { get; set; } = "";
    
    [Option("wal-path", Required = false, HelpText = "WAL path")]
    public string WalPath { get; set; } = "";
    
    [Option("wal-revision", Required = false, HelpText = "WAL revision", Default = "v1")]
    public string WalRevision{ get; set; } = "";

    [Option("initial-cluster", Required = false, HelpText = "Initial cluster configuration for static discovery")]
    public IEnumerable<string>? InitialCluster { get; set; }

    [Option("initial-cluster-partitions", Required = false, HelpText = "Initial cluster number of partitions", Default = 128)] // 32
    public int InitialClusterPartitions { get; set; } = 128;
    
    [Option("raft-nodename", Required = false, HelpText = "Unique name to identify the node in the cluster")]
    public string RaftNodeName { get; set; } = "";
    
    [Option("raft-nodeid", Required = false, HelpText = "Unique id to identify the node in the cluster")]
    public int RaftNodeId { get; set; } = 0;
    
    [Option("raft-host", Required = false, HelpText = "Host to listen for Raft consensus and replication requests", Default = "localhost")]
    public string RaftHost { get; set; } = "localhost";

    [Option("raft-port", Required = false, HelpText = "Port to bind incoming Raft consensus and replication requests", Default = 2070)]
    public int RaftPort { get; set; } = 2070;
    
    [Option("locks-workers", Required = false, HelpText = "Number of lock ephemeral/consistent workers", Default = 128)]
    public int LocksWorkers { get; set; } = 128;
    
    [Option("keyvalue-workers", Required = false, HelpText = "Number of key/value ephemeral/consistent workers", Default = 128)]
    public int KeyValueWorkers { get; set; } = 128;
    
    [Option("background-writer-workers", Required = false, HelpText = "Number of background writers workers", Default = 1)]
    public int BackgroundWritersWorkers { get; set; } = 1;
    
    [Option("default-transaction-timeout", Required = false, HelpText = "Default transaction timeout (in milliseconds)", Default = 5000)]
    public int DefaultTransactionTimeout { get; set; } = 5000;
    
    [Option("read-io-threads", Required = false, HelpText = "Read I/O threads", Default = 8)]
    public int ReadIOThreads { get; set; } = 8;
    
    [Option("write-io-threads", Required = false, HelpText = "Write I/O threads", Default = 16)]
    public int WriteIOThreads { get; set; } = 16;

    [Option("raft-http-scheme", Required = false, HelpText = "Raft HTTP scheme used by REST communication", Default = "https://")]
    public string RaftHttpScheme { get; set; } = "https://";

    [Option("raft-http-auth-bearer-token", Required = false, HelpText = "Raft HTTP bearer token used by REST communication", Default = "")]
    public string RaftHttpAuthBearerToken { get; set; } = "";

    [Option("raft-http-timeout", Required = false, HelpText = "Raft HTTP request timeout in seconds", Default = 5)]
    public int RaftHttpTimeout { get; set; } = 5;

    [Option("raft-http-version", Required = false, HelpText = "Raft HTTP version used by REST communication", Default = "2.0")]
    public string RaftHttpVersion { get; set; } = "2.0";

    [Option("raft-heartbeat-interval", Required = false, HelpText = "Raft leader heartbeat interval in milliseconds", Default = 500)]
    public int RaftHeartbeatInterval { get; set; } = 500;

    [Option("raft-recent-heartbeat", Required = false, HelpText = "Raft recent heartbeat window in milliseconds", Default = 100)]
    public int RaftRecentHeartbeat { get; set; } = 100;

    [Option("raft-voting-timeout", Required = false, HelpText = "Raft vote wait timeout in milliseconds", Default = 1500)]
    public int RaftVotingTimeout { get; set; } = 1500;

    [Option("raft-check-leader-interval", Required = false, HelpText = "Raft leader check interval in milliseconds", Default = 250)]
    public int RaftCheckLeaderInterval { get; set; } = 250;

    [Option("raft-timer-initial-delay", Required = false, HelpText = "Initial delay before Raft timers start in milliseconds", Default = 2500)]
    public int RaftTimerInitialDelay { get; set; } = 2500;

    [Option("raft-update-nodes-interval", Required = false, HelpText = "Raft node registry update interval in milliseconds", Default = 5000)]
    public int RaftUpdateNodesInterval { get; set; } = 5000;

    [Option("raft-start-election-timeout", Required = false, HelpText = "Raft minimum election timeout in milliseconds", Default = 2000)]
    public int RaftStartElectionTimeout { get; set; } = 2000;

    [Option("raft-end-election-timeout", Required = false, HelpText = "Raft maximum election timeout in milliseconds", Default = 4000)]
    public int RaftEndElectionTimeout { get; set; } = 4000;

    [Option("raft-start-election-timeout-increment", Required = false, HelpText = "Raft minimum election timeout increment in milliseconds", Default = 100)]
    public int RaftStartElectionTimeoutIncrement { get; set; } = 100;

    [Option("raft-end-election-timeout-increment", Required = false, HelpText = "Raft maximum election timeout increment in milliseconds", Default = 200)]
    public int RaftEndElectionTimeoutIncrement { get; set; } = 200;

    [Option("raft-slow-state-machine-log", Required = false, HelpText = "Raft state-machine slow operation log threshold in milliseconds", Default = 50)]
    public int RaftSlowStateMachineLog { get; set; } = 50;

    [Option("raft-slow-wal-machine-log", Required = false, HelpText = "Raft WAL state-machine slow operation log threshold in milliseconds", Default = 25)]
    public int RaftSlowWalMachineLog { get; set; } = 25;

    [Option("raft-compact-every-operations", Required = false, HelpText = "Committed operations between automatic Raft WAL compactions", Default = 10000)]
    public int RaftCompactEveryOperations { get; set; } = 10000;

    [Option("raft-compact-number-entries", Required = false, HelpText = "Raft WAL entries removed per compaction batch", Default = 100)]
    public int RaftCompactNumberEntries { get; set; } = 100;

    [Option("raft-max-entries-per-compaction", Required = false, HelpText = "Maximum Raft WAL entries to process per compaction run", Default = 5000)]
    public int RaftMaxEntriesPerCompaction { get; set; } = 5000;

    [Option("script-cache-expiration", Required = false, HelpText = "Script cache expiration (in seconds)", Default = 600)]
    public int ScriptCacheExpiration { get; set; } = 600;

    [Option("revisions-to-cache", Required = false, HelpText = "Number of revisions to keep cached in memory", Default = 4)]
    public int RevisionsToKeepCached { get; set; } = 4;

    [Option("cache-entry-ttl", Required = false, HelpText = "Maximum age of cache entries before eviction (in seconds)", Default = 1800)]
    public int CacheEntryTtl { get; set; } = 1800; // 30 minutes
    
    [Option("cache-entries-to-remove", Required = false, HelpText = "Maximum number of cache entries to remove per eviction process", Default = 100)]
    public int CacheEntriesToRemove { get; set; } = 100;
    
    [Option("dirty-objects-writer-delay", Required = false, HelpText = "Specifies how often the dirty object writer flushes ti disk (in milliseconds)", Default = 200)]
    public int DirtyObjectsWriterDelay { get; set; } = 200;
}

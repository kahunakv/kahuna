
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
    
    [Option("storage", Required = false, HelpText = "Storage (rocksdb, sqlite, memory)", Default = "rocksdb")]
    public string Storage { get; set; } = "";
    
    [Option("storage-path", Required = false, HelpText = "Storage path")]
    public string StoragePath { get; set; } = "";
    
    [Option("storage-revision", Required = false, HelpText = "Storage revision")]
    public string StorageRevision{ get; set; } = "";
    
    [Option("wal-storage", Required = false, HelpText = "WAL storage (rocksdb, sqlite, memory)", Default = "rocksdb")]
    public string WalStorage { get; set; } = "";
    
    [Option("wal-path", Required = false, HelpText = "WAL path")]
    public string WalPath { get; set; } = "";
    
    [Option("wal-revision", Required = false, HelpText = "WAL revision", Default = "v1")]
    public string WalRevision{ get; set; } = "";

    [Option("wal-sync-writes", Required = false, HelpText = "Use synchronous durable WAL writes (default: enabled)")]
    public bool WalSyncWrites { get; set; }

    [Option("disable-wal-sync-writes", Required = false, HelpText = "Disable synchronous durable WAL writes for faster non-critical test/local runs")]
    public bool DisableWalSyncWrites { get; set; }

    [Option("rocksdb-shared-memory", Required = false, HelpText = "Share one RocksDB block cache + write-buffer manager between the KV/locks backend and the Raft WAL (requires both storage and wal-storage = rocksdb). Default: disabled")]
    public bool RocksDbSharedMemory { get; set; }

    [Option("rocksdb-shared-memory-budget-mb", Required = false, HelpText = "Total shared RocksDB block-cache budget in MiB (memtable sub-budget lives inside it)", Default = 320)]
    public int RocksDbSharedMemoryBudgetMb { get; set; } = 320;

    [Option("rocksdb-shared-memtable-budget-mb", Required = false, HelpText = "Shared RocksDB memtable sub-budget in MiB, cost-charged into the shared cache (must be <= budget)", Default = 128)]
    public int RocksDbSharedMemtableBudgetMb { get; set; } = 128;

    [Option("disable-rocksdb-direct-reads", Required = false, HelpText = "Disable direct-I/O reads (default: enabled) and use buffered reads backed by the OS page cache")]
    public bool DisableRocksDbDirectReads { get; set; }

    [Option("rocksdb-statistics", Required = false, HelpText = "Enable RocksDB statistics collection and periodic LOG dumps (adds per-operation overhead; for tuning/diagnosis). Default: disabled")]
    public bool RocksDbStatistics { get; set; }

    [Option("initial-cluster", Required = false, HelpText = "Initial cluster configuration for static discovery")]
    public IEnumerable<string>? InitialCluster { get; set; }

    [Option("join-existing", Required = false, HelpText = "Join a running cluster as a new node using --initial-cluster as the seed list. When false (default) the node boots via static discovery.", Default = false)]
    public bool RaftJoinExisting { get; set; }

    [Option("graceful-leave-on-shutdown", Required = false, HelpText = "On planned shutdown, commit a RemoveMember so the roster shrinks immediately rather than waiting for SWIM eviction. Default false; do not enable on rolling restarts.", Default = false)]
    public bool RaftGracefulLeaveOnShutdown { get; set; }

    [Option("initial-cluster-partitions", Required = false, HelpText = "Initial cluster number of partitions", Default = 3)] // 32
    public int InitialClusterPartitions { get; set; } = 3;
    
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

    [Option("sequencer-workers", Required = false, HelpText = "Number of sequence workers", Default = 128)]
    public int SequencerWorkers { get; set; } = 128;

    [Option("sequencer-block-size", Required = false, HelpText = "Values reserved per sequence compare-and-swap; one Raft commit is amortized over this many values. 1 = gap-free (one commit per value)", Default = 1000)]
    public int SequencerBlockSize { get; set; } = 1000;

    [Option("sequencer-idempotency-retention-max", Required = false, HelpText = "Maximum idempotency entries retained per sequence record (0 = no cap)", Default = 256)]
    public int SequencerIdempotencyRetentionMax { get; set; } = 256;

    [Option("sequencer-idempotency-retention-ttl", Required = false, HelpText = "Window (in seconds) within which retrying a keyed sequence reserve replays the identical allocation (0 = no age pruning)", Default = 600)]
    public int SequencerIdempotencyRetentionTtl { get; set; } = 600;

    [Option("sequencer-max-sequences-per-actor", Required = false, HelpText = "Maximum sequences one actor keeps resident before evicting the least recently used (0 = unbounded)", Default = 10000)]
    public int SequencerMaxSequencesPerActor { get; set; } = 10000;

    [Option("sequencer-block-lease", Required = false, HelpText = "Seconds a reserved sequence block may be served from memory before it is revalidated against the durable record (0 = never revalidate)", Default = 5)]
    public int SequencerBlockLease { get; set; } = 5;
    
    [Option("default-transaction-timeout", Required = false, HelpText = "Default transaction timeout (in milliseconds)", Default = 5000)]
    public int DefaultTransactionTimeout { get; set; } = 5000;

    [Option("default-admission-wait", Required = false, HelpText = "Milliseconds a caller queues for an admission slot when it does not request its own budget; this is the wait to start a transaction, not the transaction's lifetime", Default = 5000)]
    public int DefaultAdmissionWaitMs { get; set; } = 5000;

    [Option("max-admission-wait", Required = false, HelpText = "Hard upper bound (in milliseconds) on any admission wait, so no caller can hold a queue slot longer than this", Default = 30000)]
    public int MaxAdmissionWaitMs { get; set; } = 30000;

    [Option("max-concurrent-transactions", Required = false, HelpText = "Script transactions that may execute concurrently before further ones queue and start in priority order (0 = no limit)", Default = 0)]
    public int MaxConcurrentTransactions { get; set; }

    [Option("max-concurrent-sessions", Required = false, HelpText = "Interactive transaction sessions that may be open concurrently (0 = no limit)", Default = 0)]
    public int MaxConcurrentSessions { get; set; }

    [Option("transaction-priority-reserved-slots", Required = false, HelpText = "Slots out of each concurrency ceiling that only high/critical priority transactions may occupy", Default = 0)]
    public int TransactionPriorityReservedSlots { get; set; }

    [Option("transaction-priority-aging-threshold", Required = false, HelpText = "Milliseconds a queued transaction waits to gain one effective priority level (0 = no aging)", Default = 1000)]
    public int TransactionPriorityAgingThreshold { get; set; } = 1000;

    [Option("transaction-priority-max-queued", Required = false, HelpText = "Callers that may wait for an admission slot per gate before further ones are refused retryably (0 = unbounded)", Default = 4096)]
    public int TransactionPriorityMaxQueued { get; set; } = 4096;
    
    [Option("read-io-threads", Required = false, HelpText = "Read I/O threads", Default = 4)]
    public int ReadIOThreads { get; set; } = 4;
    
    [Option("write-io-threads", Required = false, HelpText = "Write I/O threads", Default = 16)]
    public int WriteIOThreads { get; set; } = 16;

    [Option("backend-read-io-threads", Required = false, HelpText = "Dedicated Kahuna backend read pool threads (point gets, scans), separate from the Kommander WAL read pool", Default = 8)]
    public int BackendReadIOThreads { get; set; } = 8;

    [Option("backend-write-io-threads", Required = false, HelpText = "Dedicated Kahuna background-writer pool threads; the writer uses a single queue so >1 only adds idle threads", Default = 1)]
    public int BackendWriteIOThreads { get; set; } = 1;

    [Option("backend-read-queue-depth", Required = false, HelpText = "Per-partition pending-queue depth for the Kahuna backend read scheduler", Default = 4096)]
    public int BackendReadQueueDepth { get; set; } = 4096;

    [Option("raft-enable-shared-executor-pool", Required = false, HelpText = "Share a bounded pool of worker threads across all Raft partitions instead of one OS thread per partition. Required to run thousands of partitions (e.g. after range splits). Disable only to isolate scheduler issues.", Default = true)]
    public bool RaftEnableSharedExecutorPool { get; set; } = true;

    [Option("raft-executor-pool-size", Required = false, HelpText = "Number of shared Raft executor worker threads. 0 auto-sizes to the processor count. Raise above the core count only if ops queue behind busy partitions while CPU is not saturated.", Default = 0)]
    public int RaftExecutorPoolSize { get; set; }

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

    [Option("raft-leadership-barrier-timeout", Required = false, HelpText = "How long a newly elected leader waits for its promotion barrier entry to commit before reverting to follower, in milliseconds. Until that entry commits the node heartbeats but does not serve, so raising this trades failover latency for tolerance of a slow quorum", Default = 10000)]
    public int RaftLeadershipBarrierTimeout { get; set; } = 10000;

    [Option("raft-leadership-confirmation-timeout", Required = false, HelpText = "Maximum time a read-index leadership confirmation may wait for the quorum ack round plus applied-frontier catch-up before failing, in milliseconds", Default = 2000)]
    public int RaftLeadershipConfirmationTimeout { get; set; } = 2000;

    [Option("raft-enable-check-quorum", Required = false, HelpText = "When set, a leader that has not heard a same-term append/heartbeat ack from a majority of voters for the check-quorum window steps down to follower", Default = false)]
    public bool RaftEnableCheckQuorum { get; set; }

    [Option("raft-check-quorum-interval-multiplier", Required = false, HelpText = "Number of heartbeat intervals without a majority of same-term acks after which a leader with check-quorum enabled steps down", Default = 8)]
    public int RaftCheckQuorumIntervalMultiplier { get; set; } = 8;

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

    [Option("raft-election-timeout-seed", Required = false, HelpText = "Seed for deterministic Raft election timeouts (0 = random, non-zero = deterministic; for testing only)", Default = 0)]
    public int RaftElectionTimeoutSeed { get; set; }

    [Option("raft-max-queued-client-proposals", Required = false, HelpText = "Maximum client proposals queued per partition before backpressure kicks in", Default = 2048)]
    public int RaftMaxQueuedClientProposals { get; set; } = 2048;

    [Option("raft-max-wal-queue-depth-per-partition", Required = false, HelpText = "Per-partition WAL write queue depth limit", Default = 4096)]
    public int RaftMaxWalQueueDepthPerPartition { get; set; } = 4096;

    [Option("raft-max-global-wal-queue-depth", Required = false, HelpText = "Global WAL write queue depth limit across all partitions (0 = unlimited)", Default = 0)]
    public int RaftMaxGlobalWalQueueDepth { get; set; }

    [Option("raft-max-wal-batch-size", Required = false, HelpText = "Maximum WAL writes batched per storage flush", Default = 256)]
    public int RaftMaxWalBatchSize { get; set; } = 256;

    [Option("raft-max-wal-group-batch-partitions", Required = false, HelpText = "Maximum partitions coalesced into a single WAL cross-partition group-commit batch", Default = 64)]
    public int RaftMaxWalGroupBatchPartitions { get; set; } = 64;

    [Option("raft-wal-group-commit-linger-ms", Required = false, HelpText = "Group-commit linger window in milliseconds (0 = disabled; raise on networks where fsync rate is the bottleneck)", Default = 0)]
    public int RaftWalGroupCommitLingerMs { get; set; }

    [Option("raft-wal-single-fsync-commit", Required = false, HelpText = "Enable single-fsync fast path: ack on propose-quorum-durable and demote the commit marker to a lazy write", Default = true)]
    public bool RaftWalSingleFsyncCommit { get; set; } = true;

    [Option("raft-sqlite-wal-shard-count", Required = false, HelpText = "Number of SQLite shard databases across which partitions are distributed (0 = resolved to processor count on first initialisation)", Default = 0)]
    public int RaftSqliteWalShardCount { get; set; }

    [Option("raft-max-drain-quantum-control", Required = false, HelpText = "Max control-plane operations drained per executor wake cycle", Default = 8)]
    public int RaftMaxDrainQuantumControl { get; set; } = 8;

    [Option("raft-max-drain-quantum-replication", Required = false, HelpText = "Max replication operations drained per executor wake cycle", Default = 4)]
    public int RaftMaxDrainQuantumReplication { get; set; } = 4;

    [Option("raft-max-drain-quantum-client", Required = false, HelpText = "Max client operations drained per executor wake cycle", Default = 2)]
    public int RaftMaxDrainQuantumClient { get; set; } = 2;

    [Option("raft-max-drain-quantum-maintenance", Required = false, HelpText = "Max maintenance operations drained per executor wake cycle", Default = 1)]
    public int RaftMaxDrainQuantumMaintenance { get; set; } = 1;

    [Option("raft-grpc-scheme", Required = false, HelpText = "URL scheme prepended to peer endpoints when opening gRPC channels", Default = "https://")]
    public string RaftGrpcScheme { get; set; } = "https://";

    [Option("raft-grpc-channels-per-node", Required = false, HelpText = "Number of pooled gRPC channels opened to each peer node (clamped to [1, 64]). Each channel is a permanently-held connection and handler.", Default = 4)]
    public int RaftGrpcChannelsPerNode { get; set; } = 4;

    [Option("raft-grpc-enable-multiple-http2-connections", Required = false, HelpText = "Allow each pooled gRPC channel to open multiple HTTP/2 connections to spread concurrent streams", Default = false)]
    public bool RaftGrpcEnableMultipleHttp2Connections { get; set; }

    [Option("raft-grpc-enable-snapshot-compression", Required = false, HelpText = "Compress Raft snapshot transfers sent over gRPC", Default = false)]
    public bool RaftGrpcEnableSnapshotCompression { get; set; }

    [Option("raft-snapshot-receive-session-ttl", Required = false, HelpText = "How long an idle snapshot-receive session is kept before the receiver discards it and releases its buffered bytes, in milliseconds. Bounds the memory a leader that vanishes mid-transfer can strand.", Default = 30000)]
    public int RaftSnapshotReceiveSessionTtl { get; set; } = 30000;

    [Option("raft-snapshot-max-pending-sessions", Required = false, HelpText = "Cap on concurrent snapshot-receive sessions across all partitions. Beyond the cap the receiver evicts its least-recently-active sessions.", Default = 8)]
    public int RaftSnapshotMaxPendingSessions { get; set; } = 8;

    [Option("raft-snapshot-max-pending-bytes", Required = false, HelpText = "Cap on total buffered bytes across all in-progress snapshot-receive sessions. The primary memory bound on the receive path.", Default = 512L * 1024 * 1024)]
    public long RaftSnapshotMaxPendingBytes { get; set; } = 512L * 1024 * 1024;

    [Option("raft-allow-legacy-snapshot-senders", Required = false, HelpText = "Accept snapshot chunks that leave the sender's session-metadata fields empty, treating them as a sender that pre-dates those fields. Temporary compatibility switch for a mixed-version cluster.", Default = false)]
    public bool RaftAllowLegacySnapshotSenders { get; set; }

    [Option("raft-backfill-threshold", Required = false, HelpText = "Committed entries a follower may trail the leader before active backfill kicks in", Default = 10)]
    public int RaftBackfillThreshold { get; set; } = 10;

    [Option("raft-max-backfill-entries-per-round", Required = false, HelpText = "Maximum committed entries shipped to a stale follower per heartbeat interval", Default = 128)]
    public int RaftMaxBackfillEntriesPerRound { get; set; } = 128;

    [Option("raft-follower-saturation-backoff", Required = false, HelpText = "How long a leader stops sending backfill batches to a peer after that peer reports its WAL queue saturated, in milliseconds", Default = 1000)]
    public int RaftFollowerSaturationBackoff { get; set; } = 1000;

    [Option("raft-learner-promotion-lag", Required = false, HelpText = "Maximum entries a learner may trail the leader and still be eligible for promotion", Default = 10)]
    public int RaftLearnerPromotionLag { get; set; } = 10;

    [Option("raft-learner-promotion-stable-window", Required = false, HelpText = "Milliseconds a learner must stay within promotion-lag before being promoted to Voter", Default = 3000)]
    public int RaftLearnerPromotionStableWindow { get; set; } = 3000;

    [Option("raft-gossip-interval", Required = false, HelpText = "Interval between gossip anti-entropy rounds in milliseconds", Default = 5000)]
    public int RaftGossipInterval { get; set; } = 5000;

    [Option("raft-gossip-fanout", Required = false, HelpText = "Number of random peers contacted per gossip round (0 disables gossip)", Default = 2)]
    public int RaftGossipFanout { get; set; } = 2;

    [Option("raft-ping-timeout", Required = false, HelpText = "SWIM failure-detector ping timeout in milliseconds", Default = 500)]
    public int RaftPingTimeout { get; set; } = 500;

    [Option("raft-indirect-ping-fanout", Required = false, HelpText = "Number of intermediary nodes used for indirect SWIM probing", Default = 2)]
    public int RaftIndirectPingFanout { get; set; } = 2;

    [Option("raft-suspicion-timeout", Required = false, HelpText = "How long a node may remain Suspect before being declared Dead in milliseconds", Default = 5000)]
    public int RaftSuspicionTimeout { get; set; } = 5000;

    [Option("raft-dead-member-eviction-grace", Required = false, HelpText = "Grace period before a Dead node is committed as removed from the roster in milliseconds", Default = 30000)]
    public int RaftDeadMemberEvictionGrace { get; set; } = 30000;

    [Option("raft-ping-interval", Required = false, HelpText = "Interval between SWIM ping rounds in milliseconds (0 disables the failure detector). Must be > 0 and < raft-start-election-timeout when quiescence is enabled.", Default = 1000)]
    public int RaftPingInterval { get; set; } = 1000;

    [Option("raft-enable-auto-rejoin", Required = false, HelpText = "A node that finds itself removed from the committed roster (dead-member eviction firing across a slow restart) re-runs the join flow against the remaining members instead of staying NotMember. Disable only when an operator workflow removes live nodes and expects them to stay out.", Default = true)]
    public bool RaftEnableAutoRejoin { get; set; } = true;

    [Option("raft-enable-quiescence", Required = false, HelpText = "Quiesce idle partitions: a leader stops per-partition heartbeats after a partition is idle, relying on the SWIM failure detector for node liveness. Cuts O(NxM) idle heartbeat traffic across many partitions. Requires SWIM (raft-ping-interval > 0 and < raft-start-election-timeout).", Default = true)]
    public bool RaftEnableQuiescence { get; set; } = true;

    [Option("raft-quiesce-after", Required = false, HelpText = "How long a partition must be idle (no proposals, no in-flight replication) before its leader quiesces it, in milliseconds", Default = 1500)]
    public int RaftQuiesceAfter { get; set; } = 1500;

    [Option("raft-enable-leader-balancer", Required = false, HelpText = "Master switch for advisory leader-balancing. When false (default) no load reports are built or gossiped and the balancer loop never runs. Enable only after all cluster nodes support the feature.", Default = false)]
    public bool RaftEnableLeaderBalancer { get; set; } = false;

    [Option("raft-leader-balancer-report-interval", Required = false, HelpText = "How often each node emits a load report on the gossip path in milliseconds. Shorter intervals give fresher data at the cost of more gossip traffic.", Default = 5000)]
    public int RaftLeaderBalancerReportInterval { get; set; } = 5000;

    [Option("raft-leader-balancer-interval", Required = false, HelpText = "How often the P0 leader runs a balancer planning pass and dispatches leader-move suggestions in milliseconds.", Default = 30000)]
    public int RaftLeaderBalancerInterval { get; set; } = 30000;

    [Option("raft-leader-balancer-report-ttl", Required = false, HelpText = "Maximum age of a node load report before it is considered stale and excluded from planning in milliseconds. Must be greater than raft-leader-balancer-report-interval.", Default = 20000)]
    public int RaftLeaderBalancerReportTtl { get; set; } = 20000;

    [Option("raft-count-deadband", Required = false, HelpText = "Minimum leader-count imbalance above the ideal per-node count before the balancer emits any moves. Prevents flip-flopping around balanced distributions.", Default = 1)]
    public int RaftCountDeadband { get; set; } = 1;

    [Option("raft-load-imbalance-threshold", Required = false, HelpText = "Fractional load skew threshold (maxLoad-minLoad)/maxLoad between the busiest and quietest node that triggers load-tier swaps when count is already balanced.", Default = 0.25)]
    public double RaftLoadImbalanceThreshold { get; set; } = 0.25;

    [Option("raft-min-leader-stability-ms", Required = false, HelpText = "How long a partition must remain on the same leader (ms) before it is eligible to be moved. Prevents immediate reshuffling of a partition after an election.", Default = 5000)]
    public long RaftMinLeaderStabilityMs { get; set; } = 5000;

    [Option("raft-move-cooldown", Required = false, HelpText = "How long a partition is excluded from further moves after a transfer suggestion in milliseconds. Prevents rapid oscillation on hot partitions.", Default = 60000)]
    public int RaftMoveCooldown { get; set; } = 60000;

    [Option("raft-max-moves-per-pass", Required = false, HelpText = "Maximum number of leader-move suggestions emitted in a single planner pass. Limits blast radius of a bad planning decision.", Default = 4)]
    public int RaftMaxMovesPerPass { get; set; } = 4;

    [Option("raft-max-concurrent-transfers", Required = false, HelpText = "Maximum number of in-flight transfer suggestions the P0 leader tracks simultaneously. No new moves are dispatched until outstanding ones confirm or time out.", Default = 2)]
    public int RaftMaxConcurrentTransfers { get; set; } = 2;

    [Option("raft-leader-balancer-ops-weight", Required = false, HelpText = "Weight of the EWMA ops/sec term in the composite per-partition load score. Higher values make the balancer more sensitive to throughput differences.", Default = 1.0)]
    public double RaftLeaderBalancerOpsWeight { get; set; } = 1.0;

    [Option("raft-leader-balancer-queue-weight", Required = false, HelpText = "Weight of the instantaneous queue-depth term in the composite per-partition load score. Higher values make the balancer more sensitive to pending-work backlog.", Default = 0.5)]
    public double RaftLeaderBalancerQueueWeight { get; set; } = 0.5;

    [Option("raft-suggestion-timeout", Required = false, HelpText = "How long the P0 leader waits for a suggested move to be confirmed before declaring it dropped in milliseconds. After timeout the partition is eligible again subject to raft-move-cooldown.", Default = 15000)]
    public int RaftSuggestionTimeout { get; set; } = 15000;

    [Option("raft-replication-factor", Required = false, HelpText = "Desired number of voter replicas per partition range. 0 (the default) means full replication: every roster voter hosts every range. When > 0, each range gets a replica set of this size and quorum is computed per range over its voter replicas only. Prefer odd values.", Default = 0)]
    public int RaftReplicationFactor { get; set; }

    [Option("raft-enable-placement-rebalancer", Required = false, HelpText = "Master switch for the continual replica-placement rebalancer on the P0 leader. When false (default) no rebalancing moves are planned; in-flight replica transitions are still driven to completion. Initial placement at raft-replication-factor is applied regardless.", Default = false)]
    public bool RaftEnablePlacementRebalancer { get; set; }

    [Option("raft-max-replica-moves-per-pass", Required = false, HelpText = "Maximum number of new replica moves (add or remove sequences) initiated per placement-controller pass. Bounds the blast radius of a bad plan.", Default = 2)]
    public int RaftMaxReplicaMovesPerPass { get; set; } = 2;

    [Option("raft-max-concurrent-replica-transfers", Required = false, HelpText = "Maximum number of ranges with an in-flight transitional replica (learner catching up or removal awaiting final drop) at any time. Caps concurrent backfill/snapshot transfers so rebalancing never starves client traffic.", Default = 1)]
    public int RaftMaxConcurrentReplicaTransfers { get; set; } = 1;

    [Option("raft-replica-count-deadband", Required = false, HelpText = "Minimum per-node replica-count imbalance above the even-spread ceiling before the placement planner emits balancing moves. Under-replicated ranges bypass the deadband.", Default = 1)]
    public int RaftReplicaCountDeadband { get; set; } = 1;

    [Option("raft-zone", Required = false, HelpText = "Optional locality hint (zone/rack) for the local node. When set, the placement planner prefers spreading a range's replicas across distinct zones.", Default = null)]
    public string? RaftZone { get; set; }

    [Option("raft-grpc-enable-append-logs-coalescing", Required = false, HelpText = "Enable coalescing of multiple AppendLogs calls into a single gRPC frame per write cycle. Useful for write-heavy multi-partition workloads where per-peer send concurrency is the bottleneck.", Default = false)]
    public bool RaftGrpcEnableAppendLogsCoalescing { get; set; }

    [Option("raft-grpc-append-logs-max-coalesce-batch", Required = false, HelpText = "Maximum number of AppendLogs items drained into a single gRPC frame per write cycle when coalescing is enabled.", Default = 256)]
    public int RaftGrpcAppendLogsMaxCoalesceBatch { get; set; } = 256;

    [Option("raft-transport-security", Required = false, HelpText = "Transport security and node authentication settings (JSON; prefer --raft-allow-insecure-certificate-validation for simple dev overrides)", Default = "")]
    public string RaftTransportSecurity { get; set; } = "";

    [Option("script-cache-expiration", Required = false, HelpText = "Script cache expiration (in seconds)", Default = 600)]
    public int ScriptCacheExpiration { get; set; } = 600;

    [Option("revisions-to-cache", Required = false, HelpText = "Number of revisions to keep cached in memory", Default = 4)]
    public int RevisionsToKeepCached { get; set; } = 4;

    [Option("cache-entry-ttl", Required = false, HelpText = "Maximum age of cache entries before eviction (in seconds)", Default = 1800)]
    public int CacheEntryTtl { get; set; } = 1800; // 30 minutes
    
    [Option("cache-entries-to-remove", Required = false, HelpText = "Maximum number of cache entries to remove per eviction process", Default = 100)]
    public int CacheEntriesToRemove { get; set; } = 100;

    [Option("kv-write-linger-ms", Required = false, HelpText = "Delay from the oldest queued direct write before its partition batch is proposed; 0 = immediate (low-latency escape hatch)", Default = 1)]
    public int KeyValueWriteLingerMs { get; set; } = 1;

    [Option("kv-write-max-batch-items", Required = false, HelpText = "Maximum log entries per aggregator Raft call", Default = 512)]
    public int KeyValueWriteMaxBatchItems { get; set; } = 512;

    [Option("kv-write-max-batch-bytes", Required = false, HelpText = "Target serialized bytes per aggregator Raft call", Default = 4 * 1024 * 1024)]
    public int KeyValueWriteMaxBatchBytes { get; set; } = 4 * 1024 * 1024;

    [Option("kv-write-max-queued-items", Required = false, HelpText = "Maximum admitted direct writes per partition (incl. in flight)", Default = 8192)]
    public int KeyValueWriteMaxQueuedItemsPerPartition { get; set; } = 8192;

    [Option("kv-write-max-queued-bytes", Required = false, HelpText = "Maximum admitted serialized bytes per partition (incl. in flight)", Default = 32L * 1024 * 1024)]
    public long KeyValueWriteMaxQueuedBytesPerPartition { get; set; } = 32L * 1024 * 1024;

    [Option("kv-write-max-queue-delay-ms", Required = false, HelpText = "Maximum pre-dispatch residence for a direct write before it is released as MustRetry", Default = 1000)]
    public int KeyValueWriteMaxQueueDelayMs { get; set; } = 1000;

    [Option("kv-write-aggregator-inbox-size", Required = false, HelpText = "Ordinary-submission inbox bound per aggregator lane (control messages exempt); <=0 disables", Default = 16384)]
    public int MaxKeyValueWriteAggregatorInboxSize { get; set; } = 16384;

    [Option("dirty-objects-writer-delay", Required = false, HelpText = "Specifies how often the dirty object writer flushes ti disk (in milliseconds)", Default = 200)]
    public int DirtyObjectsWriterDelay { get; set; } = 200;

    [Option("persistent-revision-retention-count", Required = false, HelpText = "Maximum persisted key/value revisions to keep per key (0 = keep forever)", Default = 0)]
    public int PersistentRevisionRetentionCount { get; set; }

    [Option("persistent-revision-retention-age", Required = false, HelpText = "Maximum age of persisted key/value revisions in seconds (0 = disabled)", Default = 0)]
    public int PersistentRevisionRetentionAge { get; set; }

    [Option("persistent-revision-cleanup-interval", Required = false, HelpText = "Minimum interval between full persistent revision cleanup sweeps in seconds", Default = 300)]
    public int PersistentRevisionCleanupInterval { get; set; } = 300;

    [Option("persistent-revision-cleanup-batch-size", Required = false, HelpText = "Maximum revision records deleted per cleanup pass", Default = 1000)]
    public int PersistentRevisionCleanupBatchSize { get; set; } = 1000;

    [Option("persistent-revision-cleanup-on-write", Required = false, HelpText = "Run targeted persistent revision cleanup after key/value writes (default: enabled)")]
    public bool PersistentRevisionCleanupOnWrite { get; set; }

    [Option("disable-persistent-revision-cleanup-on-write", Required = false, HelpText = "Disable targeted persistent revision cleanup after key/value writes")]
    public bool DisablePersistentRevisionCleanupOnWrite { get; set; }

    [Option("pitr-window", Required = false, HelpText = "Point-in-time recovery window in seconds; WAL entries older than now-window may be compacted. Valid range: (0, 21600]. Default 3600 (1 hour).", Default = 3600)]
    public int PitrWindowSeconds { get; set; } = 3600;

    [Option("base-snapshot-interval", Required = false, HelpText = "Interval between base checkpoints per shard in seconds. Must be positive and no greater than pitr-window. Default 1800 (30 minutes).", Default = 1800)]
    public int BaseSnapshotIntervalSeconds { get; set; } = 1800;

    [Option("pitr-backup-dir", Required = false, HelpText = "Root directory for PITR backup artifacts and catalog manifests. Required when --pitr-bootstrap-from is set; also used by the backup service to store new backups.", Default = "")]
    public string PitrBackupDir { get; set; } = "";

    [Option("pitr-backup-target", Required = false, HelpText = "Which storage target holds backups. 'local' (the default) keeps them under --pitr-backup-dir on this host. Any other value requires the host to register a backup storage provider, since object-storage targets ship as separate packages.", Default = "local")]
    public string PitrBackupTarget { get; set; } = "local";

    [Option("pitr-backup-scratch-dir", Required = false, HelpText = "Local directory backup bytes transit when the target cannot be written to directly by the storage engine. Mandatory for such a target: a checkpoint is written through the filesystem before upload. Size it for one whole full backup, which transits it in its entirety.", Default = "")]
    public string PitrBackupScratchDir { get; set; } = "";

    [Option("pitr-backup-cluster-id", Required = false, HelpText = "Operator-assigned cluster identity stamped into every backup manifest; set the SAME value on every node. Gates backup-chain resolution so a foreign cluster's artifacts cannot be chained or restored here. Empty = unset (guard dormant).", Default = "")]
    public string PitrBackupClusterId { get; set; } = "";

    [Option("pitr-backup-mac-key-file", Required = false, HelpText = "Path to a file holding the secret key that authenticates backup manifests (HMAC-SHA-256); set the same key file on every node and keep it outside the backup directory, readable only by the server user. When set, manifests are signed on write and verified before restore. Empty disables authentication. Enabling it means backups taken earlier are unsigned and cannot be restored until re-taken.", Default = "")]
    public string PitrBackupMacKeyFile { get; set; } = "";

    [Option("backup-retention-max-chains", Required = false, HelpText = "Backup retention: keep at most this many most-recent backup chains, deleting older chains whole. 0 = unbounded. Retention is off unless at least one backup-retention-* bound is set.", Default = 0)]
    public int BackupRetentionMaxChains { get; set; }

    [Option("backup-retention-max-age", Required = false, HelpText = "Backup retention: delete any chain whose newest backup is older than this many seconds. 0 = unbounded.", Default = 0)]
    public long BackupRetentionMaxAgeSeconds { get; set; }

    [Option("backup-retention-max-bytes", Required = false, HelpText = "Backup retention: keep the most-recent chains whose combined artifact bytes stay within this budget (the newest chain is always kept). 0 = unbounded.", Default = 0)]
    public long BackupRetentionMaxBytes { get; set; }

    [Option("backup-gc-interval", Required = false, HelpText = "Cadence in seconds of the periodic backup GC pass (orphan/leftover sweep, plus retention when configured); a sweep also runs shortly after startup. 0 disables the periodic pass (GC still runs inline after each backup). Default 3600 (1 hour).", Default = 3600)]
    public long BackupGcIntervalSeconds { get; set; } = 3600;

    [Option("backup-restore-throttle-mbps", Required = false, HelpText = "Throughput budget in MB/s for a restore's checkpoint copy, so a restore does not saturate the disk and starve foreground traffic. 0 = unlimited.", Default = 0)]
    public int BackupRestoreThrottleMbps { get; set; }

    [Option("pitr-restore-root", Required = false, HelpText = "Server-owned root directory that restore destinations must be contained within. Setting this enables remote (REST/gRPC) restore, confining all targets under this path.", Default = "")]
    public string PitrRestoreRoot { get; set; } = "";

    [Option("pitr-allow-unconfined-remote-restore", Required = false, HelpText = "Allow remote restore requests even without a configured restore root. Insecure; disabled by default.", Default = false)]
    public bool PitrAllowUnconfinedRemoteRestore { get; set; }

    [Option("pitr-bootstrap-from", Required = false, HelpText = "Backup ID (GUID) of the leaf backup to restore from before joining the cluster. Must be combined with --join-existing and --pitr-backup-dir. When omitted the node starts normally without seeding.")]
    public Guid? PitrBootstrapFrom { get; set; }

    [Option("pitr-target-time-ms", Required = false, HelpText = "HLC target time for PITR restore expressed as milliseconds since Unix epoch (the L/physical component). 0 or omitted means restore to the chain's natural maximum (i.e. the full backup chain).", Default = 0L)]
    public long PitrTargetTimeMs { get; set; }

    [Option("raft-allow-insecure-certificate-validation", Required = false, HelpText = "Skip TLS certificate validation for inter-node Raft gRPC connections (use only in dev/test environments)")]
    public bool RaftAllowInsecureCertificateValidation { get; set; }

    /// <summary>
    /// Resolves cleanup-on-write for server startup. Enabled by default; use
    /// <see cref="DisablePersistentRevisionCleanupOnWrite"/> to turn it off from the CLI.
    /// </summary>
    public bool GetPersistentRevisionCleanupOnWrite() =>
        DisablePersistentRevisionCleanupOnWrite ? false : true;

    public bool GetWalSyncWrites() =>
        DisableWalSyncWrites ? false : true;

    public bool GetRocksDbDirectReads() =>
        DisableRocksDbDirectReads ? false : true;
}

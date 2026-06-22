namespace Kahuna;

/// <summary>
/// Options used to run a single-node Kahuna engine in-process.
/// </summary>
public sealed class EmbeddedKahunaOptions
{
    public string NodeName { get; set; } = "embedded-1";

    public int NodeId { get; set; } = 1;

    public string Host { get; set; } = "localhost";

    public int Port { get; set; }

    public int InitialPartitions { get; set; } = 1;

    /// <summary>
    /// Persistence backend for locks, key-values, and sequences. Supported values: memory, sqlite, rocksdb.
    /// </summary>
    public string Storage { get; set; } = "memory";

    public string StoragePath { get; set; } = "";

    public string StorageRevision { get; set; } = "";

    /// <summary>
    /// Raft WAL backend. Supported values: memory, sqlite, rocksdb.
    /// </summary>
    public string WalStorage { get; set; } = "memory";

    public string WalPath { get; set; } = "";

    public string WalRevision { get; set; } = "";

    public bool WalSyncWrites { get; set; } = true;

    public int LocksWorkers { get; set; } = Environment.ProcessorCount;

    public int KeyValueWorkers { get; set; } = Environment.ProcessorCount;

    public int BackgroundWriterWorkers { get; set; } = 1;

    public int DefaultTransactionTimeout { get; set; } = 5000;

    public TimeSpan ScriptCacheExpiration { get; set; } = TimeSpan.FromMinutes(1);

    public int RevisionsToKeepCached { get; set; } = 100;

    public TimeSpan CacheEntryTtl { get; set; } = TimeSpan.FromMinutes(5);

    public int CacheEntriesToRemove { get; set; } = 1000;

    public TimeSpan CollectionInterval { get; set; } = TimeSpan.FromSeconds(60);

    public int MaxEntriesPerActor { get; set; } = 50_000;

    public long MaxBytesPerActor { get; set; } = 256L * 1024 * 1024;

    public int CollectBatchMax { get; set; } = 1_000;

    public int RevisionRetention { get; set; } = 16;

    public int DirtyObjectsWriterDelay { get; set; } = 1000;

    public int ReadIOThreads { get; set; } = 8;

    public int WriteIOThreads { get; set; } = 8;

    public string HttpScheme { get; set; } = "https://";

    public string HttpAuthBearerToken { get; set; } = "";

    public int HttpTimeout { get; set; } = 5;

    public string HttpVersion { get; set; } = "2.0";

    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    public TimeSpan RecentHeartbeat { get; set; } = TimeSpan.FromMilliseconds(100);

    public TimeSpan VotingTimeout { get; set; } = TimeSpan.FromMilliseconds(1500);

    public TimeSpan CheckLeaderInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    public TimeSpan TimerInitialDelay { get; set; } = TimeSpan.FromMilliseconds(2500);

    public TimeSpan UpdateNodesInterval { get; set; } = TimeSpan.FromMilliseconds(5000);

    public int StartElectionTimeout { get; set; } = 500;

    public int EndElectionTimeout { get; set; } = 1500;

    public int StartElectionTimeoutIncrement { get; set; } = 100;

    public int EndElectionTimeoutIncrement { get; set; } = 200;

    public int SlowRaftStateMachineLog { get; set; } = 50;

    public int SlowRaftWALMachineLog { get; set; } = 25;

    public int CompactEveryOperations { get; set; } = 1000;

    public int CompactNumberEntries { get; set; } = 50;

    public int MaxEntriesPerCompaction { get; set; } = 5000;

    public int PersistentRevisionRetentionCount { get; set; }

    public TimeSpan PersistentRevisionRetentionAge { get; set; }

    public TimeSpan PersistentRevisionCleanupInterval { get; set; } = TimeSpan.FromMinutes(5);

    public int PersistentRevisionCleanupBatchSize { get; set; } = 1000;

    public bool PersistentRevisionCleanupOnWrite { get; set; } = true;

    /// <summary>
    /// Length of the point-in-time recovery window. Valid range: (0, 6h].
    /// </summary>
    public TimeSpan PitrWindow { get; set; } = TimeSpan.FromHours(1);

    /// <summary>
    /// How often a new base checkpoint is taken per shard. Must be positive and no greater than <see cref="PitrWindow"/>.
    /// </summary>
    public TimeSpan BaseSnapshotInterval { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>
    /// Root directory for PITR backup artifacts and catalog manifests.
    /// When empty, backup operations are disabled.
    /// </summary>
    public string BackupDir { get; set; } = "";

    // ── Range-split knobs (K3) ────────────────────────────────────────────────

    /// <summary>
    /// Minimum sampled key count before a range is split. 0 disables count-based auto-split.
    /// </summary>
    public int RangeSplitThreshold { get; set; } = 1_000;

    /// <summary>
    /// Minimum number of keys each child range must have after a split.
    /// </summary>
    public int RangeSplitMinRangeSize { get; set; } = 10;

    /// <summary>
    /// Minimum log-ops/sec a partition must sustain before the load branch considers splitting.
    /// 0 disables load-based auto-split.
    /// <para>
    /// <b>Important:</b> cross-node load signals (ops/sec, queue depth, commit-wait) are only
    /// gossiped when <see cref="EnableLeaderBalancer"/> is <c>true</c>. With it off, the split
    /// trigger reads 0 for every partition led on a remote node and load-based splitting is
    /// silently inert for those partitions. Set <see cref="EnableLeaderBalancer"/> = <c>true</c>
    /// alongside this knob to enable load splitting across all nodes.
    /// </para>
    /// </summary>
    public double RangeSplitLoadThreshold { get; set; }

    /// <summary>
    /// Minimum WAL queue depth (pending writes) required alongside <see cref="RangeSplitLoadThreshold"/>.
    /// </summary>
    public int RangeSplitLoadMinQueueDepth { get; set; } = 8;

    /// <summary>
    /// Optional secondary saturation gate: minimum commit-wait latency (ms) the partition must
    /// reach (alongside the rate and queue-depth gates) before a load split fires. 0 disables.
    /// </summary>
    public double RangeSplitLoadMinCommitWaitMs { get; set; }

    /// <summary>
    /// How long the load predicate must hold continuously before a split is triggered.
    /// </summary>
    public TimeSpan RangeSplitLoadWindow { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Cadence at which the load branch polls partition signals. Should be less than
    /// <see cref="RangeSplitLoadWindow"/> so the debounce can be measured accurately.
    /// </summary>
    public TimeSpan RangeSplitLoadPollInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Maximum acceptable write-imbalance fraction after a split (K2.3 indivisibility guard).
    /// A range whose best achievable imbalance meets or exceeds this value is refused.
    /// </summary>
    public double RangeSplitLoadImbalanceMax { get; set; } = 0.8;

    /// <summary>
    /// Per-descriptor cooldown after a split during which the descriptor is not re-evaluated.
    /// Prevents rapid re-splitting of a still-hot child before its new leader stabilises (K6).
    /// </summary>
    public TimeSpan RangeSplitSettleWindow { get; set; } = TimeSpan.FromSeconds(10);

    // ── Leader-balancer knobs (K5) ────────────────────────────────────────────
    // The Kommander leader balancer redistributes partition leadership across cluster nodes.
    // A freshly split partition starts with LeaderSinceMs = 0 and becomes a balancer candidate
    // automatically once MinLeaderStabilityMs elapses — no special registration needed.
    // Off by default; set EnableLeaderBalancer = true to activate.

    /// <summary>
    /// Enables automatic leader rebalancing across nodes. When on, the balancer periodically
    /// transfers partition leaders from overloaded nodes to underloaded ones, ensuring split
    /// partitions land on different nodes (K5).
    /// <para>
    /// <b>Important:</b> this flag also gates gossip of per-partition load reports
    /// (ops/sec, WAL queue depth, commit-wait). With it off, those signals are never
    /// published, so <see cref="RangeSplitLoadThreshold"/>-based splitting is silently
    /// inert for any partition whose leader lives on a remote node. Enable this alongside
    /// <see cref="RangeSplitLoadThreshold"/> whenever cross-node load-based splitting is
    /// desired.
    /// </para>
    /// </summary>
    public bool EnableLeaderBalancer { get; set; }

    /// <summary>
    /// How often each node gossips its per-partition load report. Must be less than
    /// <see cref="LeaderBalancerReportTtl"/>.
    /// </summary>
    public TimeSpan LeaderBalancerReportInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How often the balancer runs a rebalance pass. Longer values reduce churn.
    /// </summary>
    public TimeSpan LeaderBalancerInterval { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Maximum age of a gossiped load report before the balancer treats the node as silent.
    /// Must be greater than <see cref="LeaderBalancerReportInterval"/>.
    /// </summary>
    public TimeSpan LeaderBalancerReportTtl { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Minimum time a leader must have been stable before the balancer may transfer it.
    /// Newly split partitions are excluded until this elapses, giving them time to elect a
    /// stable leader before being considered for redistribution.
    /// </summary>
    public TimeSpan MinLeaderStability { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Weight applied to ops/sec in the balancer's load score.
    /// </summary>
    public double LeaderBalancerOpsWeight { get; set; } = 1.0;

    /// <summary>
    /// Weight applied to WAL queue depth in the balancer's load score.
    /// </summary>
    public double LeaderBalancerQueueWeight { get; set; } = 0.5;
}

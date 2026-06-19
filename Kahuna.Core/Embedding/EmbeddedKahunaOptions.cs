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
}

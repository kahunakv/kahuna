
using Kahuna.Server.Configuration;

namespace Kahuna.Server;

/// <summary>
/// Translates parsed command-line options into the embedded node's options object.
/// <para>
/// This lives in a class rather than as a local function in the entry point so the mapping can be
/// asserted by tests: the failure it guards against is a flag that parses correctly and is then
/// never assigned, which leaves the server running a default while the operator believes the flag
/// took effect — silent, and invisible to any test that only checks parsing.
/// </para>
/// </summary>
public static class EmbeddedOptionsFactory
{
    public static EmbeddedKahunaOptions CreateEmbeddedOptions(KahunaCommandLineOptions opts) => new()
    {
        NodeName = opts.RaftNodeName,
        NodeId = opts.RaftNodeId,
        Host = opts.RaftHost,
        Port = opts.RaftPort,
        InitialPartitions = opts.InitialClusterPartitions,
        EnableSharedExecutorPool = opts.RaftEnableSharedExecutorPool,
        PartitionExecutorPoolSize = opts.RaftExecutorPoolSize,
        ReplicationFactor = opts.RaftReplicationFactor,
        EnablePlacementRebalancer = opts.RaftEnablePlacementRebalancer,
        PlacementPassInterval = TimeSpan.FromMilliseconds(opts.RaftPlacementPassInterval),
        MaxReplicaMovesPerPass = opts.RaftMaxReplicaMovesPerPass,
        MaxConcurrentReplicaTransfers = opts.RaftMaxConcurrentReplicaTransfers,
        MaxConcurrentReplicaRepairs = opts.RaftMaxConcurrentReplicaRepairs,
        DecommissionDrainTimeout = TimeSpan.FromMilliseconds(opts.RaftDecommissionDrainTimeout),
        ReplicaCountDeadband = opts.RaftReplicaCountDeadband,
        Zone = opts.RaftZone,
        Storage = opts.Storage,
        StoragePath = opts.StoragePath,
        StorageRevision = opts.StorageRevision,
        WalStorage = opts.WalStorage,
        WalPath = opts.WalPath,
        WalRevision = opts.WalRevision,
        WalSyncWrites = opts.GetWalSyncWrites(),
        RocksDbSharedMemoryEnabled = opts.RocksDbSharedMemory,
        RocksDbSharedMemoryBudgetMb = opts.RocksDbSharedMemoryBudgetMb,
        RocksDbSharedMemtableBudgetMb = opts.RocksDbSharedMemtableBudgetMb,
        RocksDbDirectReads = opts.GetRocksDbDirectReads(),
        RocksDbStatistics = opts.RocksDbStatistics,
        LocksWorkers = opts.LocksWorkers,
        KeyValueWorkers = opts.KeyValueWorkers,
        BackgroundWriterWorkers = opts.BackgroundWritersWorkers,
        SequencerWorkers = opts.SequencerWorkers,
        SequencerBlockSize = opts.SequencerBlockSize,
        SequencerIdempotencyRetentionMax = opts.SequencerIdempotencyRetentionMax,
        SequencerIdempotencyRetentionTtl = TimeSpan.FromSeconds(opts.SequencerIdempotencyRetentionTtl),
        SequencerMaxSequencesPerActor = opts.SequencerMaxSequencesPerActor,
        SequencerBlockLease = TimeSpan.FromSeconds(opts.SequencerBlockLease),
        BackendReadIOThreads = opts.BackendReadIOThreads,
        BackendWriteIOThreads = opts.BackendWriteIOThreads,
        BackendReadQueueDepth = opts.BackendReadQueueDepth,
        DefaultTransactionTimeout = opts.DefaultTransactionTimeout,
        DefaultAdmissionWaitMs = opts.DefaultAdmissionWaitMs,
        MaxAdmissionWaitMs = opts.MaxAdmissionWaitMs,
        MaxConcurrentTransactions = opts.MaxConcurrentTransactions,
        MaxConcurrentSessions = opts.MaxConcurrentSessions,
        TransactionPriorityReservedSlots = opts.TransactionPriorityReservedSlots,
        TransactionPriorityAgingThreshold = opts.TransactionPriorityAgingThreshold,
        TransactionPriorityMaxQueued = opts.TransactionPriorityMaxQueued,
        ScriptCacheExpiration = TimeSpan.FromSeconds(opts.ScriptCacheExpiration),
        CacheEntryTtl = TimeSpan.FromSeconds(opts.CacheEntryTtl),
        CacheEntriesToRemove = opts.CacheEntriesToRemove,
        KeyValueWriteLingerMs = opts.KeyValueWriteLingerMs,
        KeyValueWriteMaxBatchItems = opts.KeyValueWriteMaxBatchItems,
        KeyValueWriteMaxBatchBytes = opts.KeyValueWriteMaxBatchBytes,
        KeyValueWriteMaxQueuedItemsPerPartition = opts.KeyValueWriteMaxQueuedItemsPerPartition,
        KeyValueWriteMaxQueuedBytesPerPartition = opts.KeyValueWriteMaxQueuedBytesPerPartition,
        KeyValueWriteMaxQueueDelayMs = opts.KeyValueWriteMaxQueueDelayMs,
        MaxKeyValueWriteAggregatorInboxSize = opts.MaxKeyValueWriteAggregatorInboxSize,
        DirtyObjectsWriterDelay = opts.DirtyObjectsWriterDelay,
        PersistentRevisionRetentionCount = opts.PersistentRevisionRetentionCount,
        PersistentRevisionRetentionAge = TimeSpan.FromSeconds(opts.PersistentRevisionRetentionAge),
        PersistentRevisionCleanupInterval = TimeSpan.FromSeconds(opts.PersistentRevisionCleanupInterval),
        PersistentRevisionCleanupBatchSize = opts.PersistentRevisionCleanupBatchSize,
        PersistentRevisionCleanupOnWrite = opts.GetPersistentRevisionCleanupOnWrite(),
        PitrWindow = TimeSpan.FromSeconds(opts.PitrWindowSeconds),
        BaseSnapshotInterval = TimeSpan.FromSeconds(opts.BaseSnapshotIntervalSeconds),
        BackupDir = opts.PitrBackupDir,
        BackupTarget = opts.PitrBackupTarget,
        BackupScratchDir = opts.PitrBackupScratchDir,
        BackupClusterId = opts.PitrBackupClusterId,
        BackupMacKeyFile = opts.PitrBackupMacKeyFile,
        RestoreRoot = opts.PitrRestoreRoot,
        AllowUnconfinedRemoteRestore = opts.PitrAllowUnconfinedRemoteRestore,
        BackupRetentionMaxChains = opts.BackupRetentionMaxChains,
        BackupRetentionMaxAge = TimeSpan.FromSeconds(opts.BackupRetentionMaxAgeSeconds),
        BackupRetentionMaxBytes = opts.BackupRetentionMaxBytes,
        BackupGcInterval = TimeSpan.FromSeconds(opts.BackupGcIntervalSeconds),
        BackupRestoreThrottleBytesPerSec = (long)opts.BackupRestoreThrottleMbps * 1_000_000,
        RangeSplitThreshold = opts.RangeSplitThreshold,
        RangeSplitMinRangeSize = opts.RangeSplitMinRangeSize,
        RangeSplitSettleWindow = TimeSpan.FromSeconds(opts.RangeSplitSettleWindowSeconds),
        RangeMoveSettleTimeout = TimeSpan.FromSeconds(opts.RangeMoveSettleTimeoutSeconds),
        RangeMergeMinSize = opts.RangeMergeMinSize,
        CollectionInterval = TimeSpan.FromSeconds(opts.RangeCollectionIntervalSeconds),
        RangeSplitLoadThreshold = opts.RangeSplitLoadThreshold,
        RangeSplitLoadMinQueueDepth = opts.RangeSplitLoadMinQueueDepth,
        RangeSplitLoadWindow = TimeSpan.FromSeconds(opts.RangeSplitLoadWindowSeconds),
        RangeSplitLoadPollInterval = TimeSpan.FromSeconds(opts.RangeSplitLoadPollIntervalSeconds),
        // The embedded node validates the settle window against its own stability value, so feed the
        // CLI's here too: left on the default, that check would compare the operator's settle window
        // against a stability figure the operator never set.
        MinLeaderStability = TimeSpan.FromMilliseconds(opts.RaftMinLeaderStabilityMs)
    };
}

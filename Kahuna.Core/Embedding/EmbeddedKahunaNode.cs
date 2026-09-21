using Kommander;
using Kommander.Communication;
using Kommander.Communication.Memory;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna;

/// <summary>
/// Boots a Kahuna engine in-process without the ASP.NET host: a single node with its own quorum, or
/// one member of a cluster whose transports the caller supplies.
/// </summary>
public sealed class EmbeddedKahunaNode : IAsyncDisposable
{
    private readonly ActorSystem actorSystem;

    private readonly MemoryInterNodeCommmunication? standaloneComm;

#if !KAHUNA_THREAD_FREE
    /// <summary>
    /// Shared RocksDB memory bundle (block cache + WriteBufferManager) when both the backend and WAL are
    /// RocksDB and sharing is enabled; otherwise null. This node <b>owns</b> it: it is injected into both
    /// the WAL and the persistence backend and disposed last in <see cref="DisposeAsync"/>, after both
    /// databases are closed.
    /// </summary>
    private readonly RocksDbSharedResources? sharedResources;
#endif

    private bool started;

    private bool disposed;

    /// <summary>
    /// Seed endpoints of a running cluster to join at <see cref="StartAsync"/> instead of
    /// bootstrapping; null for the ordinary static-roster boot. Cluster constructor only.
    /// </summary>
    private readonly List<string>? joinExistingSeeds;

    public IKahuna Kahuna { get; }

    public IRaft Raft { get; }

    public EmbeddedKahunaNode(EmbeddedKahunaOptions options, ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        ValidateOptions(options);
        EnsureStorageDirectories(options);

        // The phantom-witness quorum below IS the whole cluster; there is nothing to join, and
        // silently ignoring the seeds would boot a node the caller believes is a member of an
        // existing cluster as an isolated single-node one.
        if (options.JoinExistingSeeds is { Count: > 0 })
            throw new ArgumentException(
                $"{nameof(options.JoinExistingSeeds)} requires the cluster constructor (external communication and discovery); " +
                "the standalone constructor bootstraps its own single-node quorum",
                nameof(options));

        loggerFactory ??= NullLoggerFactory.Instance;

        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        ILogger<IKahuna> kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        // This constructor forms its quorum from phantom witnesses; the only real node is this one.
        ConfigurationValidator.ValidateReplicaPlacement(options.ReplicationFactor, seedNodeCount: 1, raftLogger);

        actorSystem = new(logger: raftLogger);
        EmbeddedRaftCommunication raftCommunication = new();

        InstallProcessFaultPolicy(options);
#if !KAHUNA_THREAD_FREE
        this.sharedResources = CreateSharedResources(options);
#endif

        RaftConfiguration raftConfiguration = CreateRaftConfiguration(options);

        // The standalone quorum is formed by phantom witnesses that persist nothing and auto-ACK
        // every AppendLogs — they exist only to grant a ceremonial majority so the sole real node
        // can elect itself leader. Nothing here ever needs catching up: the witnesses discard every
        // entry, no read is served from one, and the leader's own commit progress does not depend
        // on their catch-up. Left enabled, every apparent gap costs a bounded WAL read per witness
        // per heartbeat, and — once the readable floor has moved above the anchor — a full
        // partition-state export shipped into a transport that rejects it. The gap is easy to
        // manufacture: after a cold restart the leader restores a high committed index from its own
        // WAL while the witnesses are seeded at the election-time log id, so a startup's worth of
        // history is streamed out and thrown away.
        //
        // This must be BackfillEnabled and not a large BackfillThreshold: the threshold gates only
        // the actively-behind trigger, leaving the idle-tail and crash-restart-regression triggers
        // to fire the moment writes pause — the exact state an embedded node spends its life in.
        raftConfiguration.BackfillEnabled = false;

        this.Raft = new RaftManager(
            raftConfiguration,
            new StaticDiscovery(EmbeddedRaftCommunication.Witnesses),
#if KAHUNA_THREAD_FREE
            CreateWal(options, raftLogger),
#else
            CreateWal(options, raftLogger, sharedResources),
#endif
            raftCommunication,
            new HybridLogicalClock(),
            raftLogger
        );

        KahunaConfiguration kahunaConfiguration = CreateKahunaConfiguration(options, singleProcessRaftGroup: true);

        this.standaloneComm = new();
#if KAHUNA_THREAD_FREE
        this.Kahuna = new KahunaManager(actorSystem, Raft, kahunaConfiguration, standaloneComm, CreateBackend(options, kahunaConfiguration), kahunaLogger, raftLogger, options.WriteBatchExecutorDecorator);
#else
        this.Kahuna = new KahunaManager(actorSystem, Raft, kahunaConfiguration, standaloneComm, CreateBackend(options, kahunaConfiguration, kahunaLogger, sharedResources), kahunaLogger, raftLogger, options.WriteBatchExecutorDecorator);
#endif

        // Restart replay and WAL compaction consult Kahuna's application-durability floor; wired
        // before StartAsync joins the cluster, so the first partition restore already sees it.
        raftConfiguration.ApplicationDurabilityProvider = ((KahunaManager)Kahuna).DurabilityProvider;
    }

    /// <summary>
    /// Boots one member of a cluster with externally supplied transports and discovery. A networked
    /// cluster passes the gRPC inter-node and Raft transports; an in-process cluster passes
    /// <see cref="MemoryInterNodeCommmunication"/> and Kommander's <see cref="InMemoryCommunication"/>,
    /// shared by every member (see <see cref="EmbeddedKahunaCluster"/>).
    /// <para>
    /// The thread-free (browser) build accepts only the in-memory transports: it has no sockets, so the
    /// gRPC client (which needs SocketsHttpHandler) is not in that build.
    /// </para>
    /// </summary>
    public EmbeddedKahunaNode(
        EmbeddedKahunaOptions options,
        IInterNodeCommunication interNode,
        ICommunication raftComm,
        IDiscovery discovery,
        ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(interNode);
        ArgumentNullException.ThrowIfNull(raftComm);
        ArgumentNullException.ThrowIfNull(discovery);

#if KAHUNA_THREAD_FREE
        if (interNode is not MemoryInterNodeCommmunication)
            throw new ArgumentException(
                $"The thread-free (browser) build supports only {nameof(MemoryInterNodeCommmunication)} as the inter-node transport; got {interNode.GetType().Name}.",
                nameof(interNode));

        if (raftComm is not InMemoryCommunication)
            throw new ArgumentException(
                $"The thread-free (browser) build supports only {nameof(InMemoryCommunication)} as the Raft transport; got {raftComm.GetType().Name}.",
                nameof(raftComm));
#endif

        ValidateOptions(options);
        EnsureStorageDirectories(options);

        loggerFactory ??= NullLoggerFactory.Instance;

        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        ILogger<IKahuna> kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        // The caller owns discovery here, so the seed node count is unknown at this layer.
        ConfigurationValidator.ValidateReplicaPlacement(options.ReplicationFactor, seedNodeCount: null, raftLogger);

        joinExistingSeeds = options.JoinExistingSeeds is { Count: > 0 } seeds ? seeds : null;

        actorSystem = new(logger: raftLogger);

        InstallProcessFaultPolicy(options);
#if !KAHUNA_THREAD_FREE
        this.sharedResources = CreateSharedResources(options);
#endif

        RaftConfiguration raftConfiguration = CreateRaftConfiguration(options);

        this.Raft = new RaftManager(
            raftConfiguration,
            discovery,
#if KAHUNA_THREAD_FREE
            CreateWal(options, raftLogger),
#else
            CreateWal(options, raftLogger, sharedResources),
#endif
            raftComm,
            new HybridLogicalClock(),
            raftLogger
        );

        KahunaConfiguration kahunaConfiguration = CreateKahunaConfiguration(options, singleProcessRaftGroup: false);

        this.standaloneComm = null;
#if KAHUNA_THREAD_FREE
        this.Kahuna = new KahunaManager(actorSystem, Raft, kahunaConfiguration, interNode, CreateBackend(options, kahunaConfiguration), kahunaLogger, raftLogger, options.WriteBatchExecutorDecorator);
#else
        this.Kahuna = new KahunaManager(actorSystem, Raft, kahunaConfiguration, interNode, CreateBackend(options, kahunaConfiguration, kahunaLogger, sharedResources), kahunaLogger, raftLogger, options.WriteBatchExecutorDecorator);
#endif

        // Restart replay and WAL compaction consult Kahuna's application-durability floor; wired
        // before StartAsync joins the cluster, so the first partition restore already sees it.
        raftConfiguration.ApplicationDurabilityProvider = ((KahunaManager)Kahuna).DurabilityProvider;
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (started)
            return;

        Raft.OnLogRestored += Kahuna.OnLogRestored;
        Raft.OnReplicationReceived += Kahuna.OnReplicationReceived;
        Raft.OnReplicationError += Kahuna.OnReplicationError;
        Raft.OnLeaderChanged += Kahuna.OnLeaderChanged;
        Raft.OnLeadershipLost += Kahuna.OnLeadershipLost;

        if (standaloneComm is not null)
        {
            string localEndpoint = Raft.GetLocalEndpoint();
            standaloneComm.SetNodes(new() { { localEndpoint, Kahuna } });
        }

        // Joining a running cluster is an explicit choice, never inferred: with seeds the node
        // enters the existing roster (as a learner first, promoted once caught up); without them
        // it boots via its discovery's static roster.
        if (joinExistingSeeds is not null)
            await Raft.JoinCluster(joinExistingSeeds, cancellationToken).ConfigureAwait(false);
        else
            await Raft.JoinCluster().ConfigureAwait(false);

        started = true;

        await Raft.WaitForLeader(0, cancellationToken).ConfigureAwait(false);

        // Wait for a leader only on the partitions this node hosts. JoinCluster returns after the
        // first committed partition map has been applied, so the hosted set is known here — but a
        // later map application (the initial replica seeding, or a rebalance) can stop hosting a
        // partition at any point during this loop, so each id is re-checked and a partition that
        // becomes non-hosted mid-wait is simply skipped: a range hosted elsewhere needs no local
        // leader, and requests for it are served by forwarding. With the replication factor off,
        // every partition is hosted and this is exactly the historical wait.
        for (int partitionId = 1; partitionId <= Raft.Configuration.InitialPartitions; partitionId++)
        {
            if (!Raft.HostsPartition(partitionId))
                continue;

            try
            {
                await Raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
            }
            catch (PartitionNotHostedException)
            {
                // The placement map moved this range off the node between the check and the wait.
            }
        }
    }

    public async Task<string> WaitForLeaderForKeyAsync(string key, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        int partitionId = Raft.GetPartitionKey(key);
        return await Raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Flushes all pending dirty writes to the persistence backend and waits for completion.
    /// Call this after WAL restore (after <see cref="WaitForLeaderForKeyAsync"/>) to ensure
    /// restored entries are written to SQLite before reading schema or row data.
    /// </summary>
    public Task FlushAsync()
    {
        ObjectDisposedException.ThrowIf(disposed, this);
        return Kahuna.FlushPersistenceAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (disposed)
            return;

        disposed = true;

        if (started)
        {
            Raft.OnLogRestored -= Kahuna.OnLogRestored;
            Raft.OnReplicationReceived -= Kahuna.OnReplicationReceived;
            Raft.OnReplicationError -= Kahuna.OnReplicationError;
            Raft.OnLeadershipLost -= Kahuna.OnLeadershipLost;

            // Drain the direct-write aggregator FIRST, while its lane actors and Raft are still alive: it
            // releases queued writes retryably and awaits in-flight batches settling their Raft round trip.
            // Disposing the actor system or Raft before this would strand queued items and drop in-flight
            // completions on now-dead lanes.
            if (Kahuna is KahunaManager kahunaManager)
                await kahunaManager.DrainKeyValueWritesAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            // Skip the graceful-leave commit (CommitGracefulLeaveAsync) — in a
            // single-node embedded cluster there are no peers to notify, and the
            // 10-second retry loop inside LeaveCluster always times out during
            // shutdown because the system-partition actors are already draining.
            // Dispose() performs the same orderly shutdown (drain queues, stop
            // schedulers, stop actors) without the membership-change round-trip.
            if (Raft is IDisposable disposableRaft)
                disposableRaft.Dispose();

            // Drain all actor inboxes before disposing, so that background tasks
            // from this instance do not race with the next instance's actors on the
            // shared .NET thread pool.
            await actorSystem.GracefulShutdownAll(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

            actorSystem.Dispose();

            // Brief pause for dispatcher drain tasks and partition stop tasks that complete
            // asynchronously after Stop()/Dispose() return.
            await Task.Delay(50).ConfigureAwait(false);
        }

        if (Kahuna is IDisposable disposable)
            disposable.Dispose();

#if !KAHUNA_THREAD_FREE
        // Dispose the shared bundle LAST — only after both the Raft/WAL and the Kahuna backend above are
        // closed. This node owns it; the WAL and backend only borrow it.
        sharedResources?.Dispose();
#endif
    }

    /// <summary>
    /// The raw persistence backend the configuration selects, wrapped by the test-only decorator when the
    /// options carry one. The manager's composer puts the unflushed-write overlay over whatever is returned.
    /// </summary>
#if KAHUNA_THREAD_FREE
    private static Server.Persistence.Backend.IPersistenceBackend CreateBackend(
        EmbeddedKahunaOptions options, KahunaConfiguration configuration)
    {
        Server.Persistence.Backend.IPersistenceBackend backend = Server.Composition.KahunaNodeComposer.CreateBackend(configuration);

        return options.PersistenceBackendDecorator?.Invoke(backend) ?? backend;
    }
#else
    private static Server.Persistence.Backend.IPersistenceBackend CreateBackend(
        EmbeddedKahunaOptions options, KahunaConfiguration configuration, ILogger<IKahuna> logger, RocksDbSharedResources? sharedResources)
    {
        Server.Persistence.Backend.IPersistenceBackend backend = Server.Composition.KahunaNodeComposer.CreateBackend(configuration, logger, sharedResources);

        return options.PersistenceBackendDecorator?.Invoke(backend) ?? backend;
    }
#endif

#if KAHUNA_THREAD_FREE
    // The thread-free (browser) build has only the in-memory WAL: Kommander's browser assembly has no
    // RocksDB or SQLite WAL.
    private static IWAL CreateWal(EmbeddedKahunaOptions options, ILogger<IRaft> logger)
    {
        return options.WalStorage switch
        {
            "memory" => new InMemoryWAL(logger),
            _ => throw new KahunaServerException("Invalid WAL storage type for the thread-free build (only 'memory' is supported): " + options.WalStorage)
        };
    }
#else
    private static IWAL CreateWal(EmbeddedKahunaOptions options, ILogger<IRaft> logger, RocksDbSharedResources? sharedResources)
    {
        string revision = string.IsNullOrWhiteSpace(options.WalRevision) ? Guid.NewGuid().ToString() : options.WalRevision;

        return options.WalStorage switch
        {
            "memory" => new InMemoryWAL(logger),
            "sqlite" => new SqliteWAL(options.WalPath, revision, logger, syncWrites: options.WalSyncWrites),
            "rocksdb" => new RocksDbWAL(
                options.WalPath,
                revision,
                logger,
                syncWrites: options.WalSyncWrites,
                sharedResources: sharedResources,
                tuning: RaftWalTuningFactory.Build(options)),
            _ => throw new KahunaServerException("Invalid WAL storage type: " + options.WalStorage)
        };
    }
#endif

    /// <summary>
    /// Builds the shared RocksDB memory bundle when sharing is enabled and both the backend and WAL are
    /// RocksDB (the only case where there is anything to share). Returns null otherwise, which routes both
    /// databases down their byte-for-byte default paths.
    /// </summary>
    // Process-wide, installed by the first node constructed in the process: an out-of-memory anywhere in the
    // WAL/apply pipeline must terminate the process (or at least mark it unhealthy), never leave a reachable
    // replica that appends nothing. See ProcessFaults for the two layers and why first-chance observation is
    // needed to see a fault raised inside the WAL writer's own catch-all.
    private static void InstallProcessFaultPolicy(EmbeddedKahunaOptions options)
    {
        global::Kahuna.Server.Diagnostics.ProcessFaults.FailFastEnabled = options.FailFastOnOutOfMemory;
        global::Kahuna.Server.Diagnostics.ProcessFaults.InstallFirstChancePolicy();
    }

#if !KAHUNA_THREAD_FREE
    private static RocksDbSharedResources? CreateSharedResources(EmbeddedKahunaOptions options)
    {
        if (!options.RocksDbSharedMemoryEnabled)
            return null;

        if (options.Storage != "rocksdb" || options.WalStorage != "rocksdb")
            return null;

        long totalBytes = (long)options.RocksDbSharedMemoryBudgetMb * 1024 * 1024;
        long memtableBytes = (long)options.RocksDbSharedMemtableBudgetMb * 1024 * 1024;

        return RocksDbSharedResources.CreateWithUnifiedBudget(totalBytes, memtableBytes);
    }
#endif


    /// <summary>
    /// Builds the Kahuna configuration both constructors run on. Kept in one place because the two
    /// paths differ by a single flag: duplicating ~85 assignments is how a knob ends up wired on the
    /// standalone path and silently left on its default in a cluster.
    /// </summary>
    /// <param name="singleProcessRaftGroup">
    /// True for the in-process constructor, which builds the whole Raft group locally (phantom
    /// witnesses over an in-process transport, no inter-node listener), so no proposal can outlive
    /// the process and no remote replica can ever join — the topological guarantee the one-phase
    /// read-carrying fast path relies on. Always false for a real cluster.
    /// </param>
    internal static KahunaConfiguration CreateKahunaConfiguration(
        EmbeddedKahunaOptions options, bool singleProcessRaftGroup)
    {
        return ConfigurationValidator.Validate(new()
        {
            HttpsCertificate = "",
            HttpsCertificatePassword = "",
            LocksWorkers = options.LocksWorkers,
            KeyValueWorkers = options.KeyValueWorkers,
            BackgroundWriterWorkers = options.BackgroundWriterWorkers,
            SequencerWorkers = options.SequencerWorkers,
            SequencerBlockSize = options.SequencerBlockSize,
            SequencerIdempotencyRetentionMax = options.SequencerIdempotencyRetentionMax,
            SequencerIdempotencyRetentionTtl = options.SequencerIdempotencyRetentionTtl,
            SequencerMaxSequencesPerActor = options.SequencerMaxSequencesPerActor,
            SequencerBlockLease = options.SequencerBlockLease,
            BackendReadIOThreads = options.BackendReadIOThreads,
            BackendWriteIOThreads = options.BackendWriteIOThreads,
            BackendReadQueueDepth = options.BackendReadQueueDepth,
            Storage = options.Storage,
            StoragePath = options.StoragePath,
            StorageRevision = string.IsNullOrWhiteSpace(options.StorageRevision) ? Guid.NewGuid().ToString() : options.StorageRevision,
            RocksDbDirectReads = options.RocksDbDirectReads,
            RocksDbStatistics = options.RocksDbStatistics,
            DefaultTransactionTimeout = options.DefaultTransactionTimeout,
            MaxTransactionTimeout = options.MaxTransactionTimeout,
            DefaultAdmissionWaitMs = options.DefaultAdmissionWaitMs,
            MaxAdmissionWaitMs = options.MaxAdmissionWaitMs,
            MaxConcurrentTransactions = options.MaxConcurrentTransactions,
            MaxConcurrentSessions = options.MaxConcurrentSessions,
            TransactionPriorityReservedSlots = options.TransactionPriorityReservedSlots,
            TransactionPriorityAgingThreshold = options.TransactionPriorityAgingThreshold,
            TransactionPriorityMaxQueued = options.TransactionPriorityMaxQueued,
            ScriptCacheExpiration = options.ScriptCacheExpiration,
            MaxScriptLength = options.MaxScriptLength,
            MaxScriptDepth = options.MaxScriptDepth,
            Functions = options.Functions,
            FunctionSlowWarnMs = options.FunctionSlowWarnMs,
            RevisionsToKeepCached = options.RevisionsToKeepCached,
            CacheEntryTtl = options.CacheEntryTtl,
            CacheEntriesToRemove = options.CacheEntriesToRemove,
            CollectionInterval = options.CollectionInterval,
            TransactionOutcomeRetentionMax = options.TransactionOutcomeRetentionMax,
            TransactionOutcomeRetentionTtl = options.TransactionOutcomeRetentionTtl,
            CompletionReceiptRetentionTtl = options.CompletionReceiptRetentionTtl,
            DurableRecordGcMaxPerPass = options.DurableRecordGcMaxPerPass,
            DurableRecordRetentionMax = options.DurableRecordRetentionMax,
            DurableRecordRetentionMaxBytes = options.DurableRecordRetentionMaxBytes,
            DurableRecordRetentionHeapPressure = options.DurableRecordRetentionHeapPressure,
            DurableRecordRetentionFloor = options.DurableRecordRetentionFloor,
            DurableMaintenanceInterval = options.DurableMaintenanceInterval,
            DurableDeferredSettlement = options.DurableDeferredSettlement,
            DurableMaterializeByReference = options.DurableMaterializeByReference,
            DurableDecisionOutstandingMax = options.DurableDecisionOutstandingMax, DurablePreparedIntentMaxCount = options.DurablePreparedIntentMaxCount, DurablePreparedIntentMaxBytes = options.DurablePreparedIntentMaxBytes,
            DurableDecisionDeadlineFloorMs = options.DurableDecisionDeadlineFloorMs,
            DurableDecisionDeadlineCeilingMs = options.DurableDecisionDeadlineCeilingMs,
            MaxEntriesPerActor = options.MaxEntriesPerActor,
            MaxBytesPerActor = options.MaxBytesPerActor,
            CollectBatchMax = options.CollectBatchMax,
            KeyValueWriteLingerMs = options.KeyValueWriteLingerMs,
            KeyValueWritePostCompletionHoldMs = options.KeyValueWritePostCompletionHoldMs,
            KeyValueWriteMaxBatchItems = options.KeyValueWriteMaxBatchItems,
            KeyValueWriteMaxInFlightBatchesPerPartition = options.KeyValueWriteMaxInFlightBatchesPerPartition,
            KeyValueWriteMaxBatchBytes = options.KeyValueWriteMaxBatchBytes,
            KeyValueWriteMaxQueuedItemsPerPartition = options.KeyValueWriteMaxQueuedItemsPerPartition,
            KeyValueWriteMaxQueuedBytesPerPartition = options.KeyValueWriteMaxQueuedBytesPerPartition,
            KeyValueWriteMaxQueueDelayMs = options.KeyValueWriteMaxQueueDelayMs,
            MaxKeyValueWriteAggregatorInboxSize = options.MaxKeyValueWriteAggregatorInboxSize,
            RevisionRetention = options.RevisionRetention,
            DirtyObjectsWriterDelay = options.DirtyObjectsWriterDelay,
            PersistentRevisionRetentionCount = options.PersistentRevisionRetentionCount,
            PersistentRevisionRetentionAge = options.PersistentRevisionRetentionAge,
            PersistentRevisionCleanupInterval = options.PersistentRevisionCleanupInterval,
            PersistentRevisionCleanupBatchSize = options.PersistentRevisionCleanupBatchSize,
            PersistentRevisionCleanupOnWrite = options.PersistentRevisionCleanupOnWrite,
            PersistentRevisionCleanupTimeBudget = options.PersistentRevisionCleanupTimeBudget,
            PersistenceMaxUnflushedItems = options.PersistenceMaxUnflushedItems,
            PersistenceMaxUnflushedBytes = options.PersistenceMaxUnflushedBytes,
            PersistenceWriteStallWarnMs = options.PersistenceWriteStallWarnMs,
            PitrWindow = options.PitrWindow,
            BaseSnapshotInterval = options.BaseSnapshotInterval,
            CheckpointInterval = options.CheckpointInterval,
            BackupDir = options.BackupDir,
            BackupTarget = options.BackupTarget,
            BackupScratchDir = options.BackupScratchDir,
            BackupStorageProvider = options.BackupStorageProvider,
            BackupClusterId = options.BackupClusterId,
            BackupMacKeyFile = options.BackupMacKeyFile,
            RestoreRoot = options.RestoreRoot,
            AllowUnconfinedRemoteRestore = options.AllowUnconfinedRemoteRestore,
            BackupRetentionMaxChains = options.BackupRetentionMaxChains,
            BackupRetentionMaxAge = options.BackupRetentionMaxAge,
            BackupRetentionMaxBytes = options.BackupRetentionMaxBytes,
            BackupGcInterval = options.BackupGcInterval,
            BackupRestoreThrottleBytesPerSec = options.BackupRestoreThrottleBytesPerSec,
            RangeSplitThreshold = options.RangeSplitThreshold,
            RangeSplitMinRangeSize = options.RangeSplitMinRangeSize,
            RangeSplitLoadThreshold = options.RangeSplitLoadThreshold,
            RangeSplitLoadMinQueueDepth = options.RangeSplitLoadMinQueueDepth,
            RangeSplitLoadMinCommitWaitMs = options.RangeSplitLoadMinCommitWaitMs,
            RangeSplitLoadWindow = options.RangeSplitLoadWindow,
            RangeSplitLoadPollInterval = options.RangeSplitLoadPollInterval,
            RangeSplitLoadImbalanceMax = options.RangeSplitLoadImbalanceMax,
            RangeSplitIndivisibleCooldown = options.RangeSplitIndivisibleCooldown,
            RangeSplitSettleWindow = options.RangeSplitSettleWindow,
            RangeMoveSettleTimeout = options.RangeMoveSettleTimeout,
            RangeMergeMinSize = options.RangeMergeMinSize,
            StagedWriteIntentLeaseMs = options.StagedWriteIntentLeaseMs,
            ScanPageRetryBudgetMs = options.ScanPageRetryBudgetMs,
            SessionOwnedIntentCeilingMs = options.SessionOwnedIntentCeilingMs,
            StagedBaseFenceRetentionMs = options.StagedBaseFenceRetentionMs,
            OnePhaseApplyTimeValidation = options.OnePhaseApplyTimeValidation,
            FusedEphemeralFinalize = options.FusedEphemeralFinalize,
            ScriptActorTurns = options.ScriptActorTurns,
            SingleProcessRaftGroup = singleProcessRaftGroup
        }, options.WalPath);
    }

    internal static RaftConfiguration CreateRaftConfiguration(EmbeddedKahunaOptions options)
    {
        return new()
        {
            NodeName = options.NodeName,
            NodeId = options.NodeId,
            Host = options.Host,
            Port = options.Port,
            InitialPartitions = options.InitialPartitions,
            HttpScheme = options.HttpScheme,
            HttpAuthBearerToken = options.HttpAuthBearerToken,
            TransportSecurity = options.TransportSecurity ?? new(),
            HttpTimeout = options.HttpTimeout,
            HttpVersion = options.HttpVersion,
            HeartbeatInterval = options.HeartbeatInterval,
            RecentHeartbeat = options.RecentHeartbeat,
            VotingTimeout = options.VotingTimeout,
            EnableCheckQuorum = options.EnableCheckQuorum,
            CheckQuorumIntervalMultiplier = options.CheckQuorumIntervalMultiplier,
            CheckLeaderInterval = options.CheckLeaderInterval,
            TimerInitialDelay = options.TimerInitialDelay,
            UpdateNodesInterval = options.UpdateNodesInterval,
            StartElectionTimeout = options.StartElectionTimeout,
            EndElectionTimeout = options.EndElectionTimeout,
            StartElectionTimeoutIncrement = options.StartElectionTimeoutIncrement,
            EndElectionTimeoutIncrement = options.EndElectionTimeoutIncrement,
            SlowRaftStateMachineLog = options.SlowRaftStateMachineLog,
            SlowRaftWALMachineLog = options.SlowRaftWALMachineLog,
            MaxWalGroupBatchPartitions = options.RaftMaxWalGroupBatchPartitions,
            WalGroupCommitLingerMs = options.RaftWalGroupCommitLingerMs,
            WalSingleFsyncCommit = options.RaftWalSingleFsyncCommit,
            ReadIOThreads = options.ReadIOThreads,
            WriteIOThreads = options.WriteIOThreads,
            // Share a bounded thread pool across all partitions instead of one OS thread each, so
            // split-created partitions stay cheap. PoolSize 0 auto-sizes to the core count.
            EnableSharedExecutorPool = options.EnableSharedExecutorPool,
            PartitionExecutorPoolSize = options.PartitionExecutorPoolSize,
            CompactEveryOperations = options.CompactEveryOperations,
            CompactNumberEntries = options.CompactNumberEntries,
            MaxEntriesPerCompaction = options.MaxEntriesPerCompaction,
            // Embedded nodes keep the classic per-partition heartbeat model: they run a tiny,
            // fixed partition count with in-process witnesses and fast election timers, so the
            // O(N×M) heartbeat pressure that quiescence targets does not apply. Disabling it also
            // avoids the SWIM dependency quiescence requires (PingInterval > 0 and
            // < StartElectionTimeout), which the embedded fast timers would otherwise violate.
            EnableQuiescence = false,
            // Leader balancer: off by default; opt in via EmbeddedKahunaOptions.
            EnableLeaderBalancer = options.EnableLeaderBalancer,
            LeaderBalancerReportInterval = options.LeaderBalancerReportInterval,
            LeaderBalancerInterval = options.LeaderBalancerInterval,
            LeaderBalancerReportTtl = options.LeaderBalancerReportTtl,
            MinLeaderStabilityMs = (long)options.MinLeaderStability.TotalMilliseconds,
            LeaderBalancerOpsWeight = options.LeaderBalancerOpsWeight,
            LeaderBalancerQueueWeight = options.LeaderBalancerQueueWeight,
            ReplicationFactor = options.ReplicationFactor,
            EnablePlacementRebalancer = options.EnablePlacementRebalancer,
            // Placement runs on its own cadence, so an embedded node with a replication factor
            // converges without also enabling the leader balancer.
            PlacementPassInterval = options.PlacementPassInterval,
            MaxReplicaMovesPerPass = options.MaxReplicaMovesPerPass,
            MaxConcurrentReplicaTransfers = options.MaxConcurrentReplicaTransfers,
            MaxConcurrentReplicaRepairs = options.MaxConcurrentReplicaRepairs,
            DecommissionDrainTimeout = options.DecommissionDrainTimeout,
            ReplicaCountDeadband = options.ReplicaCountDeadband,
            Zone = options.Zone,
            EnableLoadReports = options.EnableLoadReports,
#if KAHUNA_THREAD_FREE
            // The thread-free build cannot start threads: Kommander's host pump drives the node instead.
            EnableHostPumpedScheduling = options.EnableHostPumpedScheduling
#endif
        };
    }

    private static void ValidateOptions(EmbeddedKahunaOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.NodeName))
            throw new ArgumentException("NodeName is required.", nameof(options));

        if (options.Host == "*")
            throw new ArgumentException("Host must be a concrete value for embedded nodes.", nameof(options));

        if (string.IsNullOrWhiteSpace(options.Host))
            throw new ArgumentException("Host is required.", nameof(options));

        if (options.InitialPartitions <= 0)
            throw new ArgumentException("InitialPartitions must be greater than zero.", nameof(options));

#if KAHUNA_THREAD_FREE
        // The thread-free (browser) build has only in-memory storage, and a partition executor on its
        // own thread has nothing to pump it. Refused here, with the Kahuna option named, instead of
        // deep inside the backend or Raft construction.
        if (options.Storage != "memory")
            throw new ArgumentException(
                $"Storage '{options.Storage}' is not supported in the thread-free (browser) build; use 'memory'.", nameof(options));

        if (options.WalStorage != "memory")
            throw new ArgumentException(
                $"WalStorage '{options.WalStorage}' is not supported in the thread-free (browser) build; use 'memory'.", nameof(options));

        if (!options.EnableSharedExecutorPool)
            throw new ArgumentException(
                "EnableSharedExecutorPool must be true in the thread-free (browser) build: a partition executor on its own thread cannot run there.",
                nameof(options));
#endif

        if (options.EnableLeaderBalancer &&
            options.LeaderBalancerReportInterval >= options.LeaderBalancerReportTtl)
            throw new ArgumentException(
                $"LeaderBalancerReportInterval ({options.LeaderBalancerReportInterval}) must be less than " +
                $"LeaderBalancerReportTtl ({options.LeaderBalancerReportTtl}); " +
                "otherwise the balancer treats every node as silent and never rebalances.",
                nameof(options));

        try
        {
            ConfigurationValidator.ValidateSettleWindow(
                new() { RangeSplitSettleWindow = options.RangeSplitSettleWindow },
                (long)options.MinLeaderStability.TotalMilliseconds);

#if !KAHUNA_THREAD_FREE
            // Checked here and not only where the WAL is built: on the memory and sqlite backends
            // the shard knobs are inert, so nothing downstream would ever read them and a typo
            // would survive until the deployment that switches the WAL to RocksDB.
            ConfigurationValidator.ValidateRaftWalShardTuning(options);
#endif
        }
        catch (KahunaServerException ex)
        {
            throw new ArgumentException(ex.Message, nameof(options));
        }
    }

    private static void EnsureStorageDirectories(EmbeddedKahunaOptions options)
    {
        EnsureDirectory(options.Storage, options.StoragePath);
        EnsureDirectory(options.WalStorage, options.WalPath);
    }

    private static void EnsureDirectory(string storage, string path)
    {
        if (storage is not ("rocksdb" or "sqlite") || string.IsNullOrWhiteSpace(path))
            return;

        Directory.CreateDirectory(path);
    }
}

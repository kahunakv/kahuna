using Kommander;
using Kommander.Communication;
using Kommander.Discovery;
using Kommander.Time;
using Kommander.WAL;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Nixie;

namespace Kahuna;

/// <summary>
/// Boots a single-node Kahuna engine in-process without the ASP.NET host.
/// </summary>
public sealed class EmbeddedKahunaNode : IAsyncDisposable
{
    private readonly ActorSystem actorSystem;

    private readonly MemoryInterNodeCommmunication? standaloneComm;

    private bool started;

    private bool disposed;

    public IKahuna Kahuna { get; }

    public IRaft Raft { get; }

    public EmbeddedKahunaNode(EmbeddedKahunaOptions options, ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        ValidateOptions(options);
        EnsureStorageDirectories(options);

        loggerFactory ??= NullLoggerFactory.Instance;

        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        ILogger<IKahuna> kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        actorSystem = new(logger: raftLogger);
        EmbeddedRaftCommunication raftCommunication = new();

        RaftConfiguration raftConfiguration = CreateRaftConfiguration(options);

        this.Raft = new RaftManager(
            raftConfiguration,
            new StaticDiscovery(EmbeddedRaftCommunication.Witnesses),
            CreateWal(options, raftLogger),
            raftCommunication,
            new HybridLogicalClock(),
            raftLogger
        );

        KahunaConfiguration kahunaConfiguration = ConfigurationValidator.Validate(new()
        {
            HttpsCertificate = "",
            HttpsCertificatePassword = "",
            LocksWorkers = options.LocksWorkers,
            KeyValueWorkers = options.KeyValueWorkers,
            BackgroundWriterWorkers = options.BackgroundWriterWorkers,
            Storage = options.Storage,
            StoragePath = options.StoragePath,
            StorageRevision = string.IsNullOrWhiteSpace(options.StorageRevision) ? Guid.NewGuid().ToString() : options.StorageRevision,
            DefaultTransactionTimeout = options.DefaultTransactionTimeout,
            ScriptCacheExpiration = options.ScriptCacheExpiration,
            RevisionsToKeepCached = options.RevisionsToKeepCached,
            CacheEntryTtl = options.CacheEntryTtl,
            CacheEntriesToRemove = options.CacheEntriesToRemove,
            CollectionInterval = options.CollectionInterval,
            MaxEntriesPerActor = options.MaxEntriesPerActor,
            MaxBytesPerActor = options.MaxBytesPerActor,
            CollectBatchMax = options.CollectBatchMax,
            RevisionRetention = options.RevisionRetention,
            DirtyObjectsWriterDelay = options.DirtyObjectsWriterDelay,
            PersistentRevisionRetentionCount = options.PersistentRevisionRetentionCount,
            PersistentRevisionRetentionAge = options.PersistentRevisionRetentionAge,
            PersistentRevisionCleanupInterval = options.PersistentRevisionCleanupInterval,
            PersistentRevisionCleanupBatchSize = options.PersistentRevisionCleanupBatchSize,
            PersistentRevisionCleanupOnWrite = options.PersistentRevisionCleanupOnWrite,
            PitrWindow = options.PitrWindow,
            BaseSnapshotInterval = options.BaseSnapshotInterval,
            BackupDir = options.BackupDir,
            RangeSplitThreshold = options.RangeSplitThreshold,
            RangeSplitMinRangeSize = options.RangeSplitMinRangeSize,
            RangeSplitLoadThreshold = options.RangeSplitLoadThreshold,
            RangeSplitLoadMinQueueDepth = options.RangeSplitLoadMinQueueDepth,
            RangeSplitLoadMinCommitWaitMs = options.RangeSplitLoadMinCommitWaitMs,
            RangeSplitLoadWindow = options.RangeSplitLoadWindow,
            RangeSplitLoadPollInterval = options.RangeSplitLoadPollInterval,
            RangeSplitLoadImbalanceMax = options.RangeSplitLoadImbalanceMax,
            RangeSplitIndivisibleCooldown = options.RangeSplitIndivisibleCooldown,
            RangeSplitSettleWindow = options.RangeSplitSettleWindow
        }, options.WalPath);

        this.standaloneComm = new();
        this.Kahuna = new KahunaManager(actorSystem, Raft, kahunaConfiguration, standaloneComm, kahunaLogger);
    }

    /// <summary>
    /// Boots a Kahuna engine with externally supplied communication implementations.
    /// Use this overload for cluster mode where real gRPC inter-node and Raft transports
    /// replace the in-process fakes used by the parameterless constructor.
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

        ValidateOptions(options);
        EnsureStorageDirectories(options);

        loggerFactory ??= NullLoggerFactory.Instance;

        ILogger<IRaft> raftLogger = loggerFactory.CreateLogger<IRaft>();
        ILogger<IKahuna> kahunaLogger = loggerFactory.CreateLogger<IKahuna>();

        actorSystem = new(logger: raftLogger);

        RaftConfiguration raftConfiguration = CreateRaftConfiguration(options);

        this.Raft = new RaftManager(
            raftConfiguration,
            discovery,
            CreateWal(options, raftLogger),
            raftComm,
            new HybridLogicalClock(),
            raftLogger
        );

        KahunaConfiguration kahunaConfiguration = ConfigurationValidator.Validate(new()
        {
            HttpsCertificate = "",
            HttpsCertificatePassword = "",
            LocksWorkers = options.LocksWorkers,
            KeyValueWorkers = options.KeyValueWorkers,
            BackgroundWriterWorkers = options.BackgroundWriterWorkers,
            Storage = options.Storage,
            StoragePath = options.StoragePath,
            StorageRevision = string.IsNullOrWhiteSpace(options.StorageRevision) ? Guid.NewGuid().ToString() : options.StorageRevision,
            DefaultTransactionTimeout = options.DefaultTransactionTimeout,
            ScriptCacheExpiration = options.ScriptCacheExpiration,
            RevisionsToKeepCached = options.RevisionsToKeepCached,
            CacheEntryTtl = options.CacheEntryTtl,
            CacheEntriesToRemove = options.CacheEntriesToRemove,
            CollectionInterval = options.CollectionInterval,
            MaxEntriesPerActor = options.MaxEntriesPerActor,
            MaxBytesPerActor = options.MaxBytesPerActor,
            CollectBatchMax = options.CollectBatchMax,
            RevisionRetention = options.RevisionRetention,
            DirtyObjectsWriterDelay = options.DirtyObjectsWriterDelay,
            PersistentRevisionRetentionCount = options.PersistentRevisionRetentionCount,
            PersistentRevisionRetentionAge = options.PersistentRevisionRetentionAge,
            PersistentRevisionCleanupInterval = options.PersistentRevisionCleanupInterval,
            PersistentRevisionCleanupBatchSize = options.PersistentRevisionCleanupBatchSize,
            PersistentRevisionCleanupOnWrite = options.PersistentRevisionCleanupOnWrite,
            PitrWindow = options.PitrWindow,
            BaseSnapshotInterval = options.BaseSnapshotInterval,
            BackupDir = options.BackupDir,
            RangeSplitThreshold = options.RangeSplitThreshold,
            RangeSplitMinRangeSize = options.RangeSplitMinRangeSize,
            RangeSplitLoadThreshold = options.RangeSplitLoadThreshold,
            RangeSplitLoadMinQueueDepth = options.RangeSplitLoadMinQueueDepth,
            RangeSplitLoadMinCommitWaitMs = options.RangeSplitLoadMinCommitWaitMs,
            RangeSplitLoadWindow = options.RangeSplitLoadWindow,
            RangeSplitLoadPollInterval = options.RangeSplitLoadPollInterval,
            RangeSplitLoadImbalanceMax = options.RangeSplitLoadImbalanceMax,
            RangeSplitIndivisibleCooldown = options.RangeSplitIndivisibleCooldown,
            RangeSplitSettleWindow = options.RangeSplitSettleWindow
        }, options.WalPath);

        this.standaloneComm = null;
        this.Kahuna = new KahunaManager(actorSystem, Raft, kahunaConfiguration, interNode, kahunaLogger);
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (started)
            return;

        Raft.OnLogRestored += Kahuna.OnLogRestored;
        Raft.OnReplicationReceived += Kahuna.OnReplicationReceived;
        Raft.OnReplicationError += Kahuna.OnReplicationError;

        if (standaloneComm is not null)
        {
            string localEndpoint = Raft.GetLocalEndpoint();
            standaloneComm.SetNodes(new() { { localEndpoint, Kahuna } });
        }

        await Raft.JoinCluster().ConfigureAwait(false);
        started = true;

        await Raft.WaitForLeader(0, cancellationToken).ConfigureAwait(false);

        for (int partitionId = 1; partitionId <= Raft.Configuration.InitialPartitions; partitionId++)
            await Raft.WaitForLeader(partitionId, cancellationToken).ConfigureAwait(false);
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
    }

    private static IWAL CreateWal(EmbeddedKahunaOptions options, ILogger<IRaft> logger)
    {
        string revision = string.IsNullOrWhiteSpace(options.WalRevision) ? Guid.NewGuid().ToString() : options.WalRevision;

        return options.WalStorage switch
        {
            "memory" => new InMemoryWAL(logger),
            "sqlite" => new SqliteWAL(options.WalPath, revision, logger, syncWrites: options.WalSyncWrites),
            "rocksdb" => new RocksDbWAL(options.WalPath, revision, logger, syncWrites: options.WalSyncWrites),
            _ => throw new KahunaServerException("Invalid WAL storage type: " + options.WalStorage)
        };
    }

    private static RaftConfiguration CreateRaftConfiguration(EmbeddedKahunaOptions options)
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
            HttpTimeout = options.HttpTimeout,
            HttpVersion = options.HttpVersion,
            HeartbeatInterval = options.HeartbeatInterval,
            RecentHeartbeat = options.RecentHeartbeat,
            VotingTimeout = options.VotingTimeout,
            CheckLeaderInterval = options.CheckLeaderInterval,
            TimerInitialDelay = options.TimerInitialDelay,
            UpdateNodesInterval = options.UpdateNodesInterval,
            StartElectionTimeout = options.StartElectionTimeout,
            EndElectionTimeout = options.EndElectionTimeout,
            StartElectionTimeoutIncrement = options.StartElectionTimeoutIncrement,
            EndElectionTimeoutIncrement = options.EndElectionTimeoutIncrement,
            SlowRaftStateMachineLog = options.SlowRaftStateMachineLog,
            SlowRaftWALMachineLog = options.SlowRaftWALMachineLog,
            ReadIOThreads = options.ReadIOThreads,
            WriteIOThreads = options.WriteIOThreads,
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
            LeaderBalancerQueueWeight = options.LeaderBalancerQueueWeight
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

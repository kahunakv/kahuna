using Kommander;
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
    private readonly MemoryInterNodeCommmunication interNodeCommunication;

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

        ActorSystem actorSystem = new(logger: raftLogger);
        EmbeddedRaftCommunication raftCommunication = new();

        RaftConfiguration raftConfiguration = new()
        {
            NodeName = options.NodeName,
            NodeId = options.NodeId,
            Host = options.Host,
            Port = options.Port,
            InitialPartitions = options.InitialPartitions,
            ReadIOThreads = options.ReadIOThreads,
            WriteIOThreads = options.WriteIOThreads,
            StartElectionTimeout = options.StartElectionTimeout,
            EndElectionTimeout = options.EndElectionTimeout,
            CompactEveryOperations = options.CompactEveryOperations,
            CompactNumberEntries = options.CompactNumberEntries
        };

        this.Raft = new RaftManager(
            actorSystem,
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
            DirtyObjectsWriterDelay = options.DirtyObjectsWriterDelay
        }, options.WalPath);

        this.interNodeCommunication = new();
        this.Kahuna = new KahunaManager(actorSystem, Raft, kahunaConfiguration, interNodeCommunication, kahunaLogger);
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(disposed, this);

        if (started)
            return;

        Raft.OnLogRestored += Kahuna.OnLogRestored;
        Raft.OnReplicationReceived += Kahuna.OnReplicationReceived;
        Raft.OnReplicationError += Kahuna.OnReplicationError;

        string localEndpoint = Raft.GetLocalEndpoint();
        interNodeCommunication.SetNodes(new() { { localEndpoint, Kahuna } });
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

            await Raft.LeaveCluster(disposeActorSystem: true).ConfigureAwait(false);
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
            "sqlite" => new SqliteWAL(options.WalPath, revision, logger),
            "rocksdb" => new RocksDbWAL(options.WalPath, revision, logger),
            _ => throw new KahunaServerException("Invalid WAL storage type: " + options.WalStorage)
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

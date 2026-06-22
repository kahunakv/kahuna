
using Nixie;
using System.Net;
using Flurl.Http;
using CommandLine;

using Kahuna;
using Kahuna.Services;
using Kahuna.Server.Configuration;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Rest;
using Kahuna.Server.Communication.Internode;

using Kommander;
using Kommander.Time;
using Kommander.WAL;
using Kommander.Discovery;
using Kommander.Communication.Grpc;
using Kommander.Communication.Rest;

using Microsoft.AspNetCore.Server.Kestrel.Core;

Console.WriteLine("  _           _                     ");
Console.WriteLine(" | | ____ _| |__  _   _ _ __   __ _ ");
Console.WriteLine(" | |/ / _` | '_ \\| | | | '_ \\ / _` |");
Console.WriteLine(" |   < (_| | | | | |_| | | | | (_| |");
Console.WriteLine(" |_|\\_\\__,_|_| |_|\\__,_|_| |_|\\__,_|");
Console.WriteLine("");

ParserResult<KahunaCommandLineOptions> optsResult = Parser.Default.ParseArguments<KahunaCommandLineOptions>(args);

KahunaCommandLineOptions? opts = optsResult.Value;
if (opts is null)
    return;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

// Suppress noisy per-request ASP.NET Core infrastructure logs (request start/end,
// endpoint routing). These fire on every Raft Ping and flood the console in a cluster.
builder.Logging.AddFilter("Microsoft.AspNetCore.Hosting.Diagnostics", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.AspNetCore.Routing.EndpointMiddleware", LogLevel.Warning);

if (string.IsNullOrEmpty(opts.RaftNodeName))
    opts.RaftNodeName = Environment.MachineName;

// With no peers configured the node runs standalone: an in-process embedded engine backed by
// phantom witness nodes for a clean single-node quorum (no real-peer election churn), exposed
// over the same gRPC/REST surface as a clustered node.
bool standalone = opts.InitialCluster is null || !opts.InitialCluster.Any();

if (standalone)
{
    builder.Services.AddSingleton<EmbeddedKahunaNode>(services =>
        new EmbeddedKahunaNode(CreateEmbeddedOptions(opts), services.GetRequiredService<ILoggerFactory>()));

    builder.Services.AddSingleton<IRaft>(services => services.GetRequiredService<EmbeddedKahunaNode>().Raft);
    builder.Services.AddSingleton<IKahuna>(services => services.GetRequiredService<EmbeddedKahunaNode>().Kahuna);
}
else
{
    // Assemble a Kahuna cluster from static discovery
    builder.Services.AddSingleton<IRaft>(services =>
    {
        ILogger<IRaft> logger = services.GetRequiredService<ILogger<IRaft>>();

        RaftConfiguration configuration = CreateRaftConfiguration(opts);

        bool walSyncWrites = opts.GetWalSyncWrites();

        IWAL walAdapter = opts.WalStorage switch
        {
            "rocksdb" => new RocksDbWAL(path: opts.WalPath, revision: opts.WalRevision, logger, syncWrites: walSyncWrites),
            "sqlite" => new SqliteWAL(path: opts.WalPath, revision: opts.WalRevision, logger, syncWrites: walSyncWrites),
            _ => throw new KahunaServerException("Invalid WAL storage")
        };

        return new RaftManager(
            configuration,
            new StaticDiscovery([.. opts.InitialCluster!.Select(k => new RaftNode(k))]),
            walAdapter,
            new GrpcCommunication(),
            new HybridLogicalClock(),
            logger
        );
    });

    builder.Services.AddSingleton<ActorSystem>(services => new(services, services.GetRequiredService<ILogger<IRaft>>()));
    builder.Services.AddSingleton<IKahuna, KahunaManager>();
    builder.Services.AddSingleton<IInterNodeCommunication, GrpcInterNodeCommunication>();
    builder.Services.AddSingleton(opts);
    builder.Services.AddHostedService<ReplicationService>();
}

builder.Services.AddGrpc();
builder.Services.AddGrpcReflection();

// Listen on all http/https ports in the configuration    
builder.WebHost.ConfigureKestrel(options =>
{
    options.AllowSynchronousIO = false;
    
    if (opts.HttpPorts is null || !opts.HttpPorts.Any())
        options.Listen(IPAddress.Any, 2070, listenOptions =>
        {
            listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
        });
    else
        foreach (string port in opts.HttpPorts)
            options.Listen(IPAddress.Any, int.Parse(port), listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
            });

    if (opts.HttpsPorts is null || !opts.HttpsPorts.Any())
        options.Listen(IPAddress.Any, 2071, listenOptions =>
        {
            listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
            listenOptions.UseHttps(opts.HttpsCertificate, opts.HttpsCertificatePassword);
        });
    else
    {
        foreach (string port in opts.HttpsPorts)
        {
            options.Listen(IPAddress.Any, int.Parse(port), listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
                listenOptions.UseHttps(opts.HttpsCertificate, opts.HttpsCertificatePassword);
            });
        }
    }
});

ThreadPool.SetMinThreads(256, 128);
    
// @todo Review certificate validation
FlurlHttp.Clients.WithDefaults(x => x.ConfigureInnerHandler(ih => ih.ServerCertificateCustomValidationCallback = (a, b, c, d) => true));

KahunaConfiguration kahunaConfiguration = ConfigurationValidator.Validate(new()
{
    HttpsCertificate = opts.HttpsCertificate,
    HttpsCertificatePassword = opts.HttpsCertificatePassword,
    LocksWorkers = opts.LocksWorkers,
    KeyValueWorkers = opts.KeyValueWorkers,
    BackgroundWriterWorkers = opts.BackgroundWritersWorkers,
    Storage = opts.Storage,
    StoragePath = opts.StoragePath,
    StorageRevision = opts.StorageRevision,
    DefaultTransactionTimeout = opts.DefaultTransactionTimeout,
    ScriptCacheExpiration = TimeSpan.FromSeconds(opts.ScriptCacheExpiration),
    CacheEntryTtl = TimeSpan.FromSeconds(opts.CacheEntryTtl),
    CacheEntriesToRemove = opts.CacheEntriesToRemove,
    DirtyObjectsWriterDelay = opts.DirtyObjectsWriterDelay,
    PersistentRevisionRetentionCount = opts.PersistentRevisionRetentionCount,
    PersistentRevisionRetentionAge = TimeSpan.FromSeconds(opts.PersistentRevisionRetentionAge),
    PersistentRevisionCleanupInterval = TimeSpan.FromSeconds(opts.PersistentRevisionCleanupInterval),
    PersistentRevisionCleanupBatchSize = opts.PersistentRevisionCleanupBatchSize,
    PersistentRevisionCleanupOnWrite = opts.GetPersistentRevisionCleanupOnWrite(),
    PitrWindow = TimeSpan.FromSeconds(opts.PitrWindowSeconds),
    BaseSnapshotInterval = TimeSpan.FromSeconds(opts.BaseSnapshotIntervalSeconds)
}, opts.WalPath);

ConfigurationValidator.ValidateSettleWindow(kahunaConfiguration, opts.RaftMinLeaderStabilityMs);

builder.Services.AddSingleton(kahunaConfiguration);

// Start server
WebApplication app = builder.Build();

app.MapRestRaftRoutes();
app.MapRestKahunaRoutes();

app.MapGrpcRaftRoutes();
app.MapGrpcKahunaRoutes();
app.MapGrpcReflectionService();

if (standalone)
{
    // Bind Kestrel first, then boot the embedded engine (join + leader election), then block
    // until shutdown and dispose the node so its actor system drains cleanly.
    await app.StartAsync();
    await app.Services.GetRequiredService<EmbeddedKahunaNode>().StartAsync();
    await app.WaitForShutdownAsync();
    await app.Services.GetRequiredService<EmbeddedKahunaNode>().DisposeAsync();
}
else
{
    app.Run();
}

static EmbeddedKahunaOptions CreateEmbeddedOptions(KahunaCommandLineOptions opts) => new()
{
    NodeName = opts.RaftNodeName,
    NodeId = opts.RaftNodeId,
    Host = opts.RaftHost,
    Port = opts.RaftPort,
    InitialPartitions = opts.InitialClusterPartitions,
    Storage = opts.Storage,
    StoragePath = opts.StoragePath,
    StorageRevision = opts.StorageRevision,
    WalStorage = opts.WalStorage,
    WalPath = opts.WalPath,
    WalRevision = opts.WalRevision,
    WalSyncWrites = opts.GetWalSyncWrites(),
    LocksWorkers = opts.LocksWorkers,
    KeyValueWorkers = opts.KeyValueWorkers,
    BackgroundWriterWorkers = opts.BackgroundWritersWorkers,
    DefaultTransactionTimeout = opts.DefaultTransactionTimeout,
    ScriptCacheExpiration = TimeSpan.FromSeconds(opts.ScriptCacheExpiration),
    CacheEntryTtl = TimeSpan.FromSeconds(opts.CacheEntryTtl),
    CacheEntriesToRemove = opts.CacheEntriesToRemove,
    DirtyObjectsWriterDelay = opts.DirtyObjectsWriterDelay,
    PersistentRevisionRetentionCount = opts.PersistentRevisionRetentionCount,
    PersistentRevisionRetentionAge = TimeSpan.FromSeconds(opts.PersistentRevisionRetentionAge),
    PersistentRevisionCleanupInterval = TimeSpan.FromSeconds(opts.PersistentRevisionCleanupInterval),
    PersistentRevisionCleanupBatchSize = opts.PersistentRevisionCleanupBatchSize,
    PersistentRevisionCleanupOnWrite = opts.GetPersistentRevisionCleanupOnWrite(),
    PitrWindow = TimeSpan.FromSeconds(opts.PitrWindowSeconds),
    BaseSnapshotInterval = TimeSpan.FromSeconds(opts.BaseSnapshotIntervalSeconds),
    BackupDir = opts.PitrBackupDir
};

static RaftConfiguration CreateRaftConfiguration(KahunaCommandLineOptions opts)
{
    return new()
    {
        NodeName = opts.RaftNodeName,
        NodeId = opts.RaftNodeId,
        Host = opts.RaftHost,
        Port = opts.RaftPort,
        TransportSecurity = new()
        {
            AllowInsecureCertificateValidation = opts.RaftAllowInsecureCertificateValidation
        },
        InitialPartitions = opts.InitialClusterPartitions,
        HttpScheme = opts.RaftHttpScheme,
        HttpAuthBearerToken = opts.RaftHttpAuthBearerToken,
        HttpTimeout = opts.RaftHttpTimeout,
        HttpVersion = opts.RaftHttpVersion,
        HeartbeatInterval = TimeSpan.FromMilliseconds(opts.RaftHeartbeatInterval),
        RecentHeartbeat = TimeSpan.FromMilliseconds(opts.RaftRecentHeartbeat),
        VotingTimeout = TimeSpan.FromMilliseconds(opts.RaftVotingTimeout),
        CheckLeaderInterval = TimeSpan.FromMilliseconds(opts.RaftCheckLeaderInterval),
        TimerInitialDelay = TimeSpan.FromMilliseconds(opts.RaftTimerInitialDelay),
        UpdateNodesInterval = TimeSpan.FromMilliseconds(opts.RaftUpdateNodesInterval),
        StartElectionTimeout = opts.RaftStartElectionTimeout,
        EndElectionTimeout = opts.RaftEndElectionTimeout,
        StartElectionTimeoutIncrement = opts.RaftStartElectionTimeoutIncrement,
        EndElectionTimeoutIncrement = opts.RaftEndElectionTimeoutIncrement,
        SlowRaftStateMachineLog = opts.RaftSlowStateMachineLog,
        SlowRaftWALMachineLog = opts.RaftSlowWalMachineLog,
        ReadIOThreads = opts.ReadIOThreads,
        WriteIOThreads = opts.WriteIOThreads,
        CompactEveryOperations = opts.RaftCompactEveryOperations,
        CompactNumberEntries = opts.RaftCompactNumberEntries,
        MaxEntriesPerCompaction = opts.RaftMaxEntriesPerCompaction,
        ElectionTimeoutSeed = opts.RaftElectionTimeoutSeed == 0 ? null : opts.RaftElectionTimeoutSeed,
        MaxQueuedClientProposalsPerPartition = opts.RaftMaxQueuedClientProposals,
        MaxWalQueueDepthPerPartition = opts.RaftMaxWalQueueDepthPerPartition,
        MaxGlobalWalQueueDepth = opts.RaftMaxGlobalWalQueueDepth,
        MaxWalBatchSize = opts.RaftMaxWalBatchSize,
        MaxWalGroupBatchPartitions = opts.RaftMaxWalGroupBatchPartitions,
        SqliteWalShardCount = opts.RaftSqliteWalShardCount,
        MaxDrainQuantumControl = opts.RaftMaxDrainQuantumControl,
        MaxDrainQuantumReplication = opts.RaftMaxDrainQuantumReplication,
        MaxDrainQuantumClient = opts.RaftMaxDrainQuantumClient,
        MaxDrainQuantumMaintenance = opts.RaftMaxDrainQuantumMaintenance,
        GrpcScheme = opts.RaftGrpcScheme,
        GrpcChannelsPerNode = opts.RaftGrpcChannelsPerNode,
        GrpcEnableMultipleHttp2Connections = opts.RaftGrpcEnableMultipleHttp2Connections,
        GrpcEnableSnapshotCompression = opts.RaftGrpcEnableSnapshotCompression,
        BackfillThreshold = opts.RaftBackfillThreshold,
        MaxBackfillEntriesPerRound = opts.RaftMaxBackfillEntriesPerRound,
        LearnerPromotionLag = opts.RaftLearnerPromotionLag,
        LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(opts.RaftLearnerPromotionStableWindow),
        GossipInterval = TimeSpan.FromMilliseconds(opts.RaftGossipInterval),
        GossipFanout = opts.RaftGossipFanout,
        PingTimeout = TimeSpan.FromMilliseconds(opts.RaftPingTimeout),
        IndirectPingFanout = opts.RaftIndirectPingFanout,
        SuspicionTimeout = TimeSpan.FromMilliseconds(opts.RaftSuspicionTimeout),
        DeadMemberEvictionGrace = TimeSpan.FromMilliseconds(opts.RaftDeadMemberEvictionGrace),
        PingInterval = opts.RaftPingInterval == 0 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(opts.RaftPingInterval),
        // Quiesce idle partitions in cluster mode: with many partitions the per-partition
        // keep-alive heartbeats dominate idle traffic, so a leader stops heartbeating a partition
        // once it has been idle for QuiesceAfter and leans on SWIM node liveness instead. Requires
        // PingInterval > 0 and < StartElectionTimeout, validated by RaftConfiguration at startup.
        EnableQuiescence = opts.RaftEnableQuiescence,
        QuiesceAfter = TimeSpan.FromMilliseconds(opts.RaftQuiesceAfter),
        EnableLeaderBalancer = opts.RaftEnableLeaderBalancer,
        LeaderBalancerReportInterval = TimeSpan.FromMilliseconds(opts.RaftLeaderBalancerReportInterval),
        LeaderBalancerInterval = TimeSpan.FromMilliseconds(opts.RaftLeaderBalancerInterval),
        LeaderBalancerReportTtl = TimeSpan.FromMilliseconds(opts.RaftLeaderBalancerReportTtl),
        CountDeadband = opts.RaftCountDeadband,
        LoadImbalanceThreshold = opts.RaftLoadImbalanceThreshold,
        MinLeaderStabilityMs = opts.RaftMinLeaderStabilityMs,
        MoveCooldown = TimeSpan.FromMilliseconds(opts.RaftMoveCooldown),
        MaxMovesPerPass = opts.RaftMaxMovesPerPass,
        MaxConcurrentTransfers = opts.RaftMaxConcurrentTransfers,
        LeaderBalancerOpsWeight = opts.RaftLeaderBalancerOpsWeight,
        LeaderBalancerQueueWeight = opts.RaftLeaderBalancerQueueWeight,
        SuggestionTimeout = TimeSpan.FromMilliseconds(opts.RaftSuggestionTimeout)
    };
}

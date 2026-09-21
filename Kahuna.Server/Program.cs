
using Nixie;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using CommandLine;

using Kahuna;
using Kahuna.Server;
using Kahuna.Services;
using Kahuna.Server.Configuration;
using Kahuna.Communication.External.Grpc;
using Kahuna.Communication.External.Rest;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Server.Communication;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Diagnostics;

using Kommander;
using Kommander.Diagnostics;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
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

ParserResult<KahunaCommandLineOptions> optsResult = ParseCommandLine(args);

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

// An unset storage/WAL path would reach the backend as "", which composes to "/{revision}" — an
// absolute path at the root of the filesystem rather than anything the user owns. Resolve both to a
// concrete per-user directory here, once, before either the standalone or cluster branch reads them,
// so the two branches cannot drift. An explicitly configured path is kept verbatim, so the container
// entrypoint and the cluster run scripts are unaffected.
if (!DataPathResolver.IsInMemory(opts.Storage))
    opts.StoragePath = DataPathResolver.ResolveStoragePath(opts.StoragePath);

if (!DataPathResolver.IsInMemory(opts.WalStorage))
    opts.WalPath = DataPathResolver.ResolveWalPath(opts.WalPath);

// A server's storage identity must be stable across restarts. Left unset it reaches the embedded
// node, which mints a per-boot GUID to give in-process test nodes an isolated keyspace — correct
// there, silent data loss here.
opts.StorageRevision = DataPathResolver.ResolveStorageRevision(opts.StorageRevision);

// Extension assemblies are loaded once, here, before either node shape is built. Both shapes then
// share the one registry, so a standalone node and a clustered node cannot end up with different
// function sets from the same command line. Any failure throws and the node does not start: a node
// that came up missing one function would answer scripts that call it with Errored while its peers
// answered normally, which is the hardest kind of failure to diagnose in a cluster.
Kahuna.Extensibility.KahunaFunctionRegistry extensionFunctions = ExtensionAssemblyLoader.Load(
    opts.ExtensionAssemblies,
    LoggerFactory.Create(logging => logging.AddConsole()).CreateLogger("Kahuna.Extensions"));

bool httpsConfigured = ConfigurationValidator.ShouldBindHttps(opts.HttpsCertificate, opts.HttpsPorts);

// One options object for Raft and Kahuna's inter-node traffic, built and validated before anything binds.
RaftTransportSecurityOptions transportSecurity = NodeTransportSecurityPolicy.Build(opts);
NodeTransportSecurityPolicy.Validate(opts, transportSecurity);

if (!standalone)
    NodeTransportSecurityPolicy.ValidateRaftPortListener(opts, httpsConfigured);

bool bindPlaintextListeners = NodeTransportSecurityPolicy.ShouldBindPlaintextListeners(httpsConfigured, opts.AllowPlaintextListener);

if (standalone)
{
    builder.Services.AddSingleton<EmbeddedKahunaNode>(services =>
        new EmbeddedKahunaNode(EmbeddedOptionsFactory.CreateEmbeddedOptions(opts, transportSecurity, extensionFunctions), services.GetRequiredService<ILoggerFactory>()));

    builder.Services.AddSingleton<IRaft>(services => services.GetRequiredService<EmbeddedKahunaNode>().Raft);
    builder.Services.AddSingleton<IKahuna>(services => services.GetRequiredService<EmbeddedKahunaNode>().Kahuna);
}
else
{
    // Assemble a Kahuna cluster from static discovery.
    //
    // When RocksDB shared memory is enabled and both the WAL and the KV/locks backend are RocksDB, create
    // one shared bundle (block cache + WriteBufferManager) so both databases draw from a single unified
    // budget. Registered as a singleton FIRST in this branch so the DI container disposes it LAST — after
    // the Raft/WAL and Kahuna singletons that borrow it. (Early disposal is safe anyway: each open DB holds
    // its own native refcount, so this only affects budget accounting, not crash-safety.)
    RocksDbSharedResources? sharedResources = null;

    if (opts.RocksDbSharedMemory && opts.Storage == "rocksdb" && opts.WalStorage == "rocksdb")
    {
        sharedResources = RocksDbSharedResources.CreateWithUnifiedBudget(
            (long)opts.RocksDbSharedMemoryBudgetMb * 1024 * 1024,
            (long)opts.RocksDbSharedMemtableBudgetMb * 1024 * 1024);

        builder.Services.AddSingleton(sharedResources);
    }

    builder.Services.AddSingleton<IRaft>(services =>
    {
        ILogger<IRaft> logger = services.GetRequiredService<ILogger<IRaft>>();

        RaftConfiguration configuration = CreateRaftConfiguration(opts, transportSecurity);

        ConfigurationValidator.ValidateReplicaPlacement(
            configuration.ReplicationFactor, opts.InitialCluster!.Count(), logger);

        bool walSyncWrites = opts.GetWalSyncWrites();

        // Routed through the embedded options object rather than read off the CLI here, so the
        // command line reaches the WAL tuning through exactly one mapping. A second CLI-to-tuning
        // mapping is how a flag ends up honoured in standalone mode and ignored in a cluster.
        RocksDbWalTuning walTuning = RaftWalTuningFactory.Build(EmbeddedOptionsFactory.CreateEmbeddedOptions(opts));

        IWAL walAdapter = opts.WalStorage switch
        {
            "rocksdb" => new RocksDbWAL(path: opts.WalPath, revision: opts.WalRevision, logger, syncWrites: walSyncWrites, sharedResources: sharedResources, tuning: walTuning),
            "sqlite" => new SqliteWAL(path: opts.WalPath, revision: opts.WalRevision, logger, syncWrites: walSyncWrites),
            "memory" => new InMemoryWAL(logger),
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
    // Resolve KahunaManager through a factory so the (optional) shared bundle is injected when registered
    // and null otherwise — GetService returns null for an unregistered service, preserving the default path.
    builder.Services.AddSingleton<IKahuna>(services =>
    {
        IRaft raft = services.GetRequiredService<IRaft>();

        KahunaManager manager = new(
            services.GetRequiredService<ActorSystem>(),
            raft,
            services.GetRequiredService<KahunaConfiguration>(),
            services.GetRequiredService<IInterNodeCommunication>(),
            services.GetService<RocksDbSharedResources>(),
            services.GetRequiredService<ILogger<IKahuna>>(),
            services.GetRequiredService<ILogger<IRaft>>());

        // Restart replay and WAL compaction consult Kahuna's application-durability floor. Wired
        // here — before ReplicationService joins the cluster — and read lazily by Kommander, so
        // the first partition restore already sees it.
        raft.Configuration.ApplicationDurabilityProvider = manager.DurabilityProvider;

        return manager;
    });
    // The inter-node transport must dial with the exact options Kommander uses; see GrpcInterNodeCommunication.
    builder.Services.AddSingleton<IInterNodeCommunication>(services => new GrpcInterNodeCommunication(
        services.GetRequiredService<KahunaConfiguration>(),
        services.GetRequiredService<IRaft>().Configuration.GetTransportAuthenticator().Options,
        services.GetRequiredService<ILogger<GrpcInterNodeCommunication>>()));
    builder.Services.AddHostedService<ReplicationService>();
}

// Guards the node-only gRPC surfaces with the same trust policy Raft applies.
builder.Services.AddNodeTransportGate();

// Registered outside both branches: the dashboard's summary endpoint reads the resolved storage
// paths and the node name from here, and it answers on a standalone node as well as a clustered one.
builder.Services.AddSingleton(opts);

// Kahuna and Kommander publish their instruments whether or not anything listens, so without this
// collector the node's own throughput, WAL batching, executor queue depth and commit latency are
// readable only after someone wires up OpenTelemetry or Prometheus. Constructed by the container and
// disposed with it, which stops the MeterListener at shutdown — a leaked listener observes forever.
builder.Services.AddSingleton<EngineMetricsCollector>();

builder.Services.AddGrpc();
builder.Services.AddGrpcReflection();

// The REST handlers of Kahuna and of Kommander's Raft transport bind request bodies and write
// responses through the minimal-API JSON options. Resolving their types from generated metadata keeps
// those endpoints working in a trimmed build, where the reflection resolver cannot see members the
// trimmer removed. The options themselves keep the web defaults, so the wire format does not change.
builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, KahunaJsonContext.Default);
    options.SerializerOptions.TypeInfoResolverChain.Insert(1, Kommander.Communication.RestJsonContext.Default);
});

// Listen on all http/https ports in the configuration    
builder.WebHost.ConfigureKestrel(options =>
{
    options.AllowSynchronousIO = false;

    // Cleartext listeners beside a configured certificate are an explicit opt-in.
    if (bindPlaintextListeners)
    {
        foreach (int port in NodeTransportSecurityPolicy.GetHttpPorts(opts))
            options.Listen(IPAddress.Any, port, listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http1AndHttp2AndHttp3;
            });

        // Cleartext HTTP/2 (h2c) for gRPC. Kestrel only accepts prior-knowledge HTTP/2 without
        // TLS when the listener speaks HTTP/2 exclusively: with Http1AndHttp2 and no TLS there
        // is no ALPN, so protocol selection falls back to HTTP/1.1 and gRPC calls fail.
        //
        // A standalone node with no explicit ports binds 2072, so a gRPC client can reach it out of
        // the box: HTTPS needs a certificate the node does not have by default, and the plain HTTP
        // listener negotiates HTTP/1.1 without ALPN. A node joining a cluster keeps the listener off
        // unless it is asked for — the port carries no TLS and no authentication.
        IReadOnlyList<int> cleartextGrpcPorts = NodeTransportSecurityPolicy.ParsePorts(opts.GrpcCleartextPorts, "--grpc-cleartext-ports");

        if (cleartextGrpcPorts.Count == 0 && standalone)
            cleartextGrpcPorts = [NodeTransportSecurityPolicy.DefaultStandaloneCleartextGrpcPort];

        foreach (int port in cleartextGrpcPorts)
            options.Listen(IPAddress.Any, port, listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http2;
            });
    }

    if (!httpsConfigured)
        return;

    // Under mTLS the listener on --raft-port is the cluster listener: it demands a client certificate.
    // The other HTTPS listeners stay server-TLS only for application clients.
    foreach (int port in NodeTransportSecurityPolicy.GetHttpsPorts(opts))
    {
        bool clusterListener = NodeTransportSecurityPolicy.IsClusterListener(transportSecurity, port, opts.RaftPort);

        options.Listen(IPAddress.Any, port, listenOptions =>
        {
            listenOptions.Protocols = NodeTransportSecurityPolicy.GetHttpsProtocols(clusterListener);
            listenOptions.UseHttps(opts.HttpsCertificate, opts.HttpsCertificatePassword,
                httpsOptions => NodeTransportSecurityPolicy.ConfigureClientCertificate(httpsOptions, clusterListener));
        });
    }
});

ThreadPool.SetMinThreads(256, 128);

// No process-wide certificate bypass: Kommander's cluster clients and Kahuna's inter-node channels take
// their certificate policy from the transport-security options, so other HTTP clients validate normally.

KahunaConfiguration kahunaConfiguration = ConfigurationValidator.Validate(new()
{
    HttpsCertificate = opts.HttpsCertificate,
    HttpsCertificatePassword = opts.HttpsCertificatePassword,
    InterNodeGrpcScheme = opts.RaftGrpcScheme,
    AdvertisedClientEndpoint = opts.AdvertisedClientEndpoint,
    AdvertisedClientScheme = opts.AdvertisedClientScheme,
    AdvertisePeerEndpoints = !opts.DisablePeerEndpointAdvertisement,
    RoutingHintsEnabled = !opts.DisableRoutingHints,
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
    Storage = opts.Storage,
    StoragePath = opts.StoragePath,
    StorageRevision = opts.StorageRevision,
    RocksDbDirectReads = opts.GetRocksDbDirectReads(),
    RocksDbStatistics = opts.RocksDbStatistics,
    DefaultTransactionTimeout = opts.DefaultTransactionTimeout,
    FusedEphemeralFinalize = !opts.DisableFusedEphemeralFinalize,
    ScriptActorTurns = !opts.DisableScriptActorTurns,
    DefaultAdmissionWaitMs = opts.DefaultAdmissionWaitMs,
    MaxAdmissionWaitMs = opts.MaxAdmissionWaitMs,
    MaxConcurrentTransactions = opts.MaxConcurrentTransactions,
    MaxConcurrentSessions = opts.MaxConcurrentSessions,
    TransactionPriorityReservedSlots = opts.TransactionPriorityReservedSlots,
    TransactionPriorityAgingThreshold = opts.TransactionPriorityAgingThreshold,
    TransactionPriorityMaxQueued = opts.TransactionPriorityMaxQueued,
    ScriptCacheExpiration = TimeSpan.FromSeconds(opts.ScriptCacheExpiration),
    MaxScriptLength = opts.MaxScriptLength,
    MaxScriptDepth = opts.MaxScriptDepth,
    Functions = extensionFunctions,
    FunctionSlowWarnMs = opts.FunctionSlowWarnMs,
    CacheEntryTtl = TimeSpan.FromSeconds(opts.CacheEntryTtl),
    CacheEntriesToRemove = opts.CacheEntriesToRemove,
    KeyValueWriteLingerMs = opts.KeyValueWriteLingerMs,
    KeyValueWritePostCompletionHoldMs = opts.KeyValueWritePostCompletionHoldMs,
    KeyValueWriteMaxBatchItems = opts.KeyValueWriteMaxBatchItems,
    KeyValueWriteMaxInFlightBatchesPerPartition = opts.KeyValueWriteMaxInFlightBatchesPerPartition,
    KeyValueWriteMaxBatchBytes = opts.KeyValueWriteMaxBatchBytes,
    KeyValueWriteMaxQueuedItemsPerPartition = opts.KeyValueWriteMaxQueuedItemsPerPartition,
    KeyValueWriteMaxQueuedBytesPerPartition = opts.KeyValueWriteMaxQueuedBytesPerPartition,
    KeyValueWriteMaxQueueDelayMs = opts.KeyValueWriteMaxQueueDelayMs,
    MaxKeyValueWriteAggregatorInboxSize = opts.MaxKeyValueWriteAggregatorInboxSize,
    DirtyObjectsWriterDelay = opts.DirtyObjectsWriterDelay,
    CheckpointInterval = TimeSpan.FromSeconds(opts.CheckpointIntervalSeconds),
    PersistentRevisionRetentionCount = opts.PersistentRevisionRetentionCount,
    PersistentRevisionRetentionAge = TimeSpan.FromSeconds(opts.PersistentRevisionRetentionAge),
    PersistentRevisionCleanupInterval = TimeSpan.FromSeconds(opts.PersistentRevisionCleanupInterval),
    PersistentRevisionCleanupBatchSize = opts.PersistentRevisionCleanupBatchSize,
    PersistentRevisionCleanupOnWrite = opts.GetPersistentRevisionCleanupOnWrite(),
    PersistentRevisionCleanupTimeBudget = TimeSpan.FromMilliseconds(opts.PersistentRevisionCleanupTimeBudgetMs),
    PersistenceMaxUnflushedItems = opts.PersistenceMaxUnflushedItems,
    PersistenceMaxUnflushedBytes = opts.PersistenceMaxUnflushedBytes,
    PersistenceWriteStallWarnMs = opts.PersistenceWriteStallWarnMs,
    PitrWindow = TimeSpan.FromSeconds(opts.PitrWindowSeconds),
    BaseSnapshotInterval = TimeSpan.FromSeconds(opts.BaseSnapshotIntervalSeconds),
    // The range knobs are carried here as well as on the embedded options: this is the instance the
    // validators below inspect, and without them the settle-window check would compare a default
    // against the operator's leader-stability setting and pass no matter what was asked for.
    RangeSplitThreshold = opts.RangeSplitThreshold,
    RangeSplitMinRangeSize = opts.RangeSplitMinRangeSize,
    RangeSplitSettleWindow = TimeSpan.FromSeconds(opts.RangeSplitSettleWindowSeconds),
    RangeMoveSettleTimeout = TimeSpan.FromSeconds(opts.RangeMoveSettleTimeoutSeconds),
    RangeMergeMinSize = opts.RangeMergeMinSize,
    CollectionInterval = TimeSpan.FromSeconds(opts.RangeCollectionIntervalSeconds),
    RangeSplitLoadThreshold = opts.RangeSplitLoadThreshold,
    RangeSplitLoadMinQueueDepth = opts.RangeSplitLoadMinQueueDepth,
    RangeSplitLoadWindow = TimeSpan.FromSeconds(opts.RangeSplitLoadWindowSeconds),
    RangeSplitLoadPollInterval = TimeSpan.FromSeconds(opts.RangeSplitLoadPollIntervalSeconds)
}, opts.WalPath);

// Process-wide, and this host runs exactly one node: with hints off a served request pays neither
// the ambient write nor the capture object, so the switch actually removes the cost rather than
// only blanking the field.
Kahuna.Server.Routing.RouteCaptureScope.Enabled = kahunaConfiguration.RoutingHintsEnabled;

ConfigurationValidator.ValidateSettleWindow(kahunaConfiguration, opts.RaftMinLeaderStabilityMs);
ConfigurationValidator.ValidateCollectionInterval(kahunaConfiguration);

builder.Services.AddSingleton(kahunaConfiguration);

// Start server
WebApplication app = builder.Build();

// "Where is my data?" and "am I actually on TLS?" must both be answerable from the console without
// reconstructing the flag defaults — especially for a node started with no flags at all.
if (app.Logger.IsEnabled(LogLevel.Information))
{
    app.Logger.LogInformation("Storage: {Storage} at {StoragePath}", opts.Storage, DataPathResolver.IsInMemory(opts.Storage) ? "(in-memory)" : opts.StoragePath);
    app.Logger.LogInformation("WAL: {WalStorage} at {WalPath}", opts.WalStorage, DataPathResolver.IsInMemory(opts.WalStorage) ? "(in-memory)" : opts.WalPath);
}

if (!httpsConfigured)
    app.Logger.LogInformation("HTTPS disabled: no certificate configured (pass --https-certificate to enable it)");

if (!bindPlaintextListeners)
    app.Logger.LogInformation("Cleartext HTTP and h2c listeners not bound: an HTTPS certificate is configured (pass --allow-plaintext-listener to bind them)");

if (transportSecurity.NodeAuthenticationMode == RaftNodeAuthenticationMode.MutualTls && app.Logger.IsEnabled(LogLevel.Information))
    app.Logger.LogInformation("MutualTls: port {RaftPort} requires a trusted client certificate and serves HTTP/1.1 and HTTP/2 only", opts.RaftPort);

if (transportSecurity.NodeAuthenticationMode == RaftNodeAuthenticationMode.SharedSecret)
    app.Logger.LogWarning("SharedSecret authenticates Raft only; Kahuna's inter-node gRPC stays unauthenticated. Use MutualTls to cover both");

if (opts.RaftAllowInsecureCertificateValidation)
    app.Logger.LogWarning("--raft-allow-insecure-certificate-validation is set: peer certificates are not validated on Raft or Kahuna inter-node connections. Other HTTP clients are unaffected. Development only");

// Must wrap the pipeline before any route runs: maps retryable infrastructure exceptions
// (Raft resolution, inter-node transport) escaping the kv/locks/sequences surfaces to a typed
// MustRetry response instead of an unclassifiable HTTP 500.
app.UseRetryableExceptionMapping();

app.MapRestRaftRoutes();
app.MapRestKahunaRoutes(opts);

app.MapGrpcRaftRoutes();
app.MapGrpcKahunaRoutes();
app.MapGrpcReflectionService();

// KAHUNA_WAL_INSTRUMENT=1 brackets a WAL double-fsync measurement window with the process lifetime:
// reset + enable when the node finishes starting, snapshot + disable + log when it begins stopping.
// The instrumentation is inert (a single volatile read per record) while the env var is unset.
if (Environment.GetEnvironmentVariable("KAHUNA_WAL_INSTRUMENT") == "1")
{
    app.Lifetime.ApplicationStarted.Register(() =>
    {
        WalPhaseInstrumentation.Reset();
        WalPhaseInstrumentation.Enabled = true;
        app.Logger.LogInformation("WAL phase instrumentation enabled (KAHUNA_WAL_INSTRUMENT=1)");
    });

    app.Lifetime.ApplicationStopping.Register(() =>
    {
        WalPhaseInstrumentation.Enabled = false;
        if (app.Logger.IsEnabled(LogLevel.Information))
        {
            InstrumentationSnapshot snap = WalPhaseInstrumentation.Snapshot();
            app.Logger.LogInformation(
                "WAL phase instrumentation snapshot — " +
                "propose[enq={ProposeEnq} dur={ProposeDur} p50={ProposeP50:F2}ms p99={ProposeP99:F2}ms] " +
                "commit[enq={CommitEnq} dur={CommitDur} p50={CommitP50:F2}ms p99={CommitP99:F2}ms] " +
                "followerAppend[enq={FollowerEnq} dur={FollowerDur} p50={FollowerP50:F2}ms p99={FollowerP99:F2}ms]",
                snap.Propose.Enqueued, snap.Propose.Durable, snap.Propose.P50Ms, snap.Propose.P99Ms,
                snap.Commit.Enqueued, snap.Commit.Durable, snap.Commit.P50Ms, snap.Commit.P99Ms,
                snap.FollowerAppend.Enqueued, snap.FollowerAppend.Durable, snap.FollowerAppend.P50Ms, snap.FollowerAppend.P99Ms);
        }

        // fsyncs-per-committed-write: TotalSyncBatchesWritten (real fsyncs) drops toward ~1× with the
        // single-fsync fast path on, while TotalBatchesWritten (Write-call count) stays ~2×. The ratio
        // is the deterministic fsync-count assertion the double-fsync spec asks for, alongside p50.
        if (app.Services.GetRequiredService<IRaft>().WalScheduler is FairWalScheduler wal
            && app.Logger.IsEnabled(LogLevel.Information))
            app.Logger.LogInformation(
                "WAL fsync counters — totalBatches={TotalBatches} totalSyncBatches={TotalSyncBatches} (sync/total={Ratio:F3})",
                wal.TotalBatchesWritten, wal.TotalSyncBatchesWritten,
                wal.TotalBatchesWritten > 0 ? (double)wal.TotalSyncBatchesWritten / wal.TotalBatchesWritten : 0.0);
    });
}

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
    await app.StartAsync();
    await app.WaitForShutdownAsync();

    // Ordered teardown mirroring EmbeddedKahunaNode: drain the key/value write aggregator and the actor
    // system while Raft and the actors are still alive, THEN dispose KahunaManager (which stops the backend
    // I/O schedulers) so no actor enqueues backend work onto an already-stopped scheduler, THEN dispose Raft.
    // KahunaManager.Dispose is idempotent, so the later DI container teardown is a no-op.
    IKahuna kahuna = app.Services.GetRequiredService<IKahuna>();
    ActorSystem actorSystem = app.Services.GetRequiredService<ActorSystem>();

    if (kahuna is KahunaManager kahunaManager)
        await kahunaManager.DrainKeyValueWritesAsync(TimeSpan.FromSeconds(5));

    await actorSystem.GracefulShutdownAll(TimeSpan.FromSeconds(5));

    (kahuna as IDisposable)?.Dispose();
    (app.Services.GetRequiredService<IRaft>() as IDisposable)?.Dispose();
}

static RaftConfiguration CreateRaftConfiguration(KahunaCommandLineOptions opts, RaftTransportSecurityOptions transportSecurity)
{
    return new()
    {
        NodeName = opts.RaftNodeName,
        NodeId = opts.RaftNodeId,
        Host = opts.RaftHost,
        Port = opts.RaftPort,
        TransportSecurity = transportSecurity,
        InitialPartitions = opts.InitialClusterPartitions,
        HttpScheme = opts.RaftHttpScheme,
        HttpAuthBearerToken = opts.RaftHttpAuthBearerToken,
        HttpTimeout = opts.RaftHttpTimeout,
        HttpVersion = opts.RaftHttpVersion,
        HeartbeatInterval = TimeSpan.FromMilliseconds(opts.RaftHeartbeatInterval),
        RecentHeartbeat = TimeSpan.FromMilliseconds(opts.RaftRecentHeartbeat),
        VotingTimeout = TimeSpan.FromMilliseconds(opts.RaftVotingTimeout),
        CheckLeaderInterval = TimeSpan.FromMilliseconds(opts.RaftCheckLeaderInterval),
        LeadershipBarrierTimeout = TimeSpan.FromMilliseconds(opts.RaftLeadershipBarrierTimeout),
        LeadershipConfirmationTimeout = TimeSpan.FromMilliseconds(opts.RaftLeadershipConfirmationTimeout),
        ProposalTimeout = TimeSpan.FromMilliseconds(opts.RaftProposalTimeout),
        WalStallStepDownTimeout = TimeSpan.FromMilliseconds(opts.RaftWalStallStepDownTimeout),
        WalStallWarnThreshold = TimeSpan.FromMilliseconds(opts.RaftWalStallWarnThreshold),
        // Check-quorum: a leader that hears no same-term ack from a majority of voters for the
        // window steps down, which bounds how long an isolated leader keeps answering as one. On
        // by default because the acknowledged-write loss of a two-leader window is silent to
        // clients. KAHUNA_CHECK_QUORUM=0 forces it off (the CLI bool is a bare switch and cannot
        // express "false").
        EnableCheckQuorum = opts.RaftEnableCheckQuorum
            && Environment.GetEnvironmentVariable("KAHUNA_CHECK_QUORUM") != "0",
        CheckQuorumIntervalMultiplier = opts.RaftCheckQuorumIntervalMultiplier,
        TimerInitialDelay = TimeSpan.FromMilliseconds(opts.RaftTimerInitialDelay),
        UpdateNodesInterval = TimeSpan.FromMilliseconds(opts.RaftUpdateNodesInterval),
        StartElectionTimeout = opts.RaftStartElectionTimeout,
        EndElectionTimeout = opts.RaftEndElectionTimeout,
        StartElectionTimeoutIncrement = opts.RaftStartElectionTimeoutIncrement,
        EndElectionTimeoutIncrement = opts.RaftEndElectionTimeoutIncrement,
        SlowRaftStateMachineLog = opts.RaftSlowStateMachineLog,
        SlowRaftWALMachineLog = opts.RaftSlowWalMachineLog,
        InvariantChecks = opts.RaftInvariantChecks,
        ReadIOThreads = opts.ReadIOThreads,
        WriteIOThreads = opts.WriteIOThreads,
        // Share a bounded thread pool across all partitions so a large cluster (and split-created
        // partitions) does not spend one OS thread per partition. PoolSize 0 auto-sizes to the core count.
        // KAHUNA_SHARED_POOL=0 forces the original one-thread-per-partition model (diagnostic escape
        // hatch — the CLI bool is a bare switch and cannot express "false").
        EnableSharedExecutorPool = opts.RaftEnableSharedExecutorPool
            && Environment.GetEnvironmentVariable("KAHUNA_SHARED_POOL") != "0",
        PartitionExecutorPoolSize = opts.RaftExecutorPoolSize,
        CompactEveryOperations = opts.RaftCompactEveryOperations,
        CompactNumberEntries = opts.RaftCompactNumberEntries,
        MaxEntriesPerCompaction = opts.RaftMaxEntriesPerCompaction,
        ElectionTimeoutSeed = opts.RaftElectionTimeoutSeed == 0 ? null : opts.RaftElectionTimeoutSeed,
        MaxQueuedClientProposalsPerPartition = opts.RaftMaxQueuedClientProposals,
        MaxWalQueueDepthPerPartition = opts.RaftMaxWalQueueDepthPerPartition,
        MaxGlobalWalQueueDepth = opts.RaftMaxGlobalWalQueueDepth,
        MaxWalBatchSize = opts.RaftMaxWalBatchSize,
        MaxWalGroupBatchPartitions = opts.RaftMaxWalGroupBatchPartitions,
        WalGroupCommitLingerMs = opts.RaftWalGroupCommitLingerMs,
        WalSingleFsyncCommit = opts.RaftWalSingleFsyncCommit,
        SqliteWalShardCount = opts.RaftSqliteWalShardCount,
        MaxDrainQuantumControl = opts.RaftMaxDrainQuantumControl,
        MaxDrainQuantumReplication = opts.RaftMaxDrainQuantumReplication,
        MaxDrainQuantumClient = opts.RaftMaxDrainQuantumClient,
        MaxDrainQuantumMaintenance = opts.RaftMaxDrainQuantumMaintenance,
        GrpcScheme = opts.RaftGrpcScheme,
        GrpcChannelsPerNode = opts.RaftGrpcChannelsPerNode,
        GrpcEnableMultipleHttp2Connections = opts.RaftGrpcEnableMultipleHttp2Connections,
        GrpcEnableSnapshotCompression = opts.RaftGrpcEnableSnapshotCompression,
        GrpcMaxMessageBytes = opts.RaftGrpcMaxMessageBytes,
        SnapshotReceiveSessionTtl = TimeSpan.FromMilliseconds(opts.RaftSnapshotReceiveSessionTtl),
        SnapshotMaxPendingSessions = opts.RaftSnapshotMaxPendingSessions,
        SnapshotMaxPendingBytes = opts.RaftSnapshotMaxPendingBytes,
        AllowLegacySnapshotSenders = opts.RaftAllowLegacySnapshotSenders,
        SnapshotTransferStepTimeout = TimeSpan.FromMilliseconds(opts.RaftSnapshotTransferStepTimeout),
        SnapshotChunkAckTimeout = TimeSpan.FromMilliseconds(opts.RaftSnapshotChunkAckTimeout),
        MaxPreAuthRequestBodyBytes = opts.RaftMaxPreAuthRequestBodyBytes,
        GrpcEnableAppendLogsCoalescing = opts.RaftGrpcEnableAppendLogsCoalescing,
        GrpcAppendLogsMaxCoalesceBatch = opts.RaftGrpcAppendLogsMaxCoalesceBatch,
        BackfillEnabled = opts.RaftBackfillEnabled,
        BackfillThreshold = opts.RaftBackfillThreshold,
        MaxBackfillEntriesPerRound = opts.RaftMaxBackfillEntriesPerRound,
        FollowerSaturationBackoff = TimeSpan.FromMilliseconds(opts.RaftFollowerSaturationBackoff),
        BackfillNoProgressPauseCap = TimeSpan.FromMilliseconds(opts.RaftBackfillNoProgressPauseCap),
        BackfillNoProgressAnchorFallbackShips = opts.RaftBackfillNoProgressAnchorFallbackShips,
        LearnerPromotionLag = opts.RaftLearnerPromotionLag,
        LearnerPromotionStableWindow = TimeSpan.FromMilliseconds(opts.RaftLearnerPromotionStableWindow),
        GossipInterval = TimeSpan.FromMilliseconds(opts.RaftGossipInterval),
        GossipFanout = opts.RaftGossipFanout,
        PingTimeout = TimeSpan.FromMilliseconds(opts.RaftPingTimeout),
        IndirectPingFanout = opts.RaftIndirectPingFanout,
        SuspicionTimeout = TimeSpan.FromMilliseconds(opts.RaftSuspicionTimeout),
        DeadMemberEvictionGrace = TimeSpan.FromMilliseconds(opts.RaftDeadMemberEvictionGrace),
        SelfRepairPeerDownGrace = TimeSpan.FromMilliseconds(opts.RaftSelfRepairPeerDownGrace),
        PingInterval = opts.RaftPingInterval == 0 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(opts.RaftPingInterval),
        // A node evicted from the roster while it was down (dead-member eviction firing across a
        // slow restart) re-runs the join flow instead of parking as NotMember forever. KAHUNA_AUTO_REJOIN=0
        // forces it off (the CLI bool is a bare switch and cannot express "false").
        EnableAutoRejoin = opts.RaftEnableAutoRejoin
            && Environment.GetEnvironmentVariable("KAHUNA_AUTO_REJOIN") != "0",
        // Quiesce idle partitions in cluster mode: with many partitions the per-partition
        // keep-alive heartbeats dominate idle traffic, so a leader stops heartbeating a partition
        // once it has been idle for QuiesceAfter and leans on SWIM node liveness instead. Requires
        // PingInterval > 0 and < StartElectionTimeout, validated by RaftConfiguration at startup.
        // KAHUNA_QUIESCENCE=0 forces quiescence off (diagnostic escape hatch — the CLI bool is a
        // bare switch and cannot express "false").
        EnableQuiescence = opts.RaftEnableQuiescence
            && Environment.GetEnvironmentVariable("KAHUNA_QUIESCENCE") != "0",
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
        SuggestionTimeout = TimeSpan.FromMilliseconds(opts.RaftSuggestionTimeout),
        EnableSlowNodeAvoidance = opts.RaftEnableSlowNodeAvoidance,
        SlowNodeMultiplier = opts.RaftSlowNodeMultiplier,
        SlowNodeFloorMs = opts.RaftSlowNodeFloorMs,
        SlowNodeMinSamples = opts.RaftSlowNodeMinSamples,
        SlowNodeObservationTtl = TimeSpan.FromMilliseconds(opts.RaftSlowNodeObservationTtl),
        SlowNodeEnterPasses = opts.RaftSlowNodeEnterPasses,
        SlowNodeExitPasses = opts.RaftSlowNodeExitPasses,
        ReplicationFactor = opts.RaftReplicationFactor,
        EnablePlacementRebalancer = opts.RaftEnablePlacementRebalancer,
        PlacementPassInterval = TimeSpan.FromMilliseconds(opts.RaftPlacementPassInterval),
        MaxReplicaMovesPerPass = opts.RaftMaxReplicaMovesPerPass,
        MaxConcurrentReplicaTransfers = opts.RaftMaxConcurrentReplicaTransfers,
        MaxConcurrentReplicaRepairs = opts.RaftMaxConcurrentReplicaRepairs,
        DecommissionDrainTimeout = TimeSpan.FromMilliseconds(opts.RaftDecommissionDrainTimeout),
        ReplicaCountDeadband = opts.RaftReplicaCountDeadband,
        Zone = opts.RaftZone,
        EnableLoadReports = opts.RaftEnableLoadReports,
        MaxOutboundQueueBytesPerPeer = opts.RaftMaxOutboundQueueBytesPerPeer,
        MaxOutboundBatchBytes = opts.RaftMaxOutboundBatchBytes,
        MaxBackfillBytesPerRound = opts.RaftMaxBackfillBytesPerRound,
        SnapshotRescueMaxConsecutiveCycles = opts.RaftSnapshotRescueMaxConsecutiveCycles,
        SnapshotRescueProbeInterval = TimeSpan.FromMilliseconds(opts.RaftSnapshotRescueProbeInterval),
        SnapshotExportRetryCacheMaxBytes = opts.RaftSnapshotExportRetryCacheMaxBytes,
        CompactionLiveReplicaLagBudget = opts.RaftCompactionLiveReplicaLagBudget,
        CompactionSilentPeerRetentionWindow = TimeSpan.FromMilliseconds(opts.RaftCompactionSilentPeerRetentionWindow),
        CompactionDurabilityClampReportInterval = TimeSpan.FromMilliseconds(opts.RaftCompactionDurabilityClampReportInterval)
    };
}

// CommandLineParser creates the options object and fills its properties by reflection. The trimmer
// cannot see that use, so it would remove the constructor and the property setters, and a trimmed
// build would fail at startup. The dependency keeps every member of the options type.
[DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(KahunaCommandLineOptions))]
static ParserResult<KahunaCommandLineOptions> ParseCommandLine(string[] args) =>
    Parser.Default.ParseArguments<KahunaCommandLineOptions>(args);

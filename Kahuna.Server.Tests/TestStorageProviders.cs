using System.Text;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Persistence.Providers;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Data;
using Kommander.System;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kahuna.Server.Tests;

/// <summary>
/// Storage engines chosen by name through <see cref="StorageProviders"/>: a node opens the backend and the WAL
/// that its options name, including engines registered by the host, refuses a name nothing registered, and a
/// backup restores through the provider of the engine that wrote it.
/// </summary>
public sealed class TestStorageProviders : IDisposable
{
    private readonly string tempRoot = Path.Combine(Path.GetTempPath(), "kahuna_providers_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        if (Directory.Exists(tempRoot))
            Directory.Delete(tempRoot, recursive: true);
    }

    [Fact]
    public void TheDefaultRegistry_HoldsKahunasOwnEngines()
    {
        StorageProviders providers = StorageProviders.CreateDefault();

        Assert.Equal(["memory", "rocksdb", "sqlite"], providers.BackendTypes);
        Assert.Equal(["memory", "rocksdb", "sqlite"], providers.WalTypes);
        Assert.False(providers.GetBackend("memory").KeepsDataOnDisk);
        Assert.True(providers.GetBackend("rocksdb").StagesLiveCheckpointsInStore);
        Assert.False(providers.GetBackend("sqlite").StagesLiveCheckpointsInStore);

        // A backup records an empty type for a store that never named its engine: the memory backend.
        Assert.Same(providers.GetBackend("memory"), providers.FindBackend(""));
        Assert.Null(providers.FindBackend("nope"));
    }

    [Fact]
    public void AnUnregisteredStorageType_FailsTheNode_NamingTheRegisteredTypes()
    {
        KahunaServerException backend = Assert.Throws<KahunaServerException>(() =>
            new EmbeddedKahunaNode(Options(storage: "nope", walStorage: "memory"), NullLoggerFactory.Instance));
        Assert.Contains("'nope'", backend.Message);
        Assert.Contains("memory, rocksdb, sqlite", backend.Message);

        KahunaServerException wal = Assert.Throws<KahunaServerException>(() =>
            new EmbeddedKahunaNode(Options(storage: "memory", walStorage: "nope"), NullLoggerFactory.Instance));
        Assert.Contains("WAL storage type: 'nope'", wal.Message);
    }

    [Fact]
    public async Task RegisteredEngines_OpenTheNodesBackendAndWal_AndServeItsWrites()
    {
        string storagePath = Path.Combine(tempRoot, "store");
        string walPath = Path.Combine(tempRoot, "wal");
        RecordingBackendProvider backendProvider = new("recorded");
        RecordingWalProvider walProvider = new("recorded-wal");

        EmbeddedKahunaOptions options = Options(storage: "recorded", walStorage: "recorded-wal");
        options.StoragePath = storagePath;
        options.StorageRevision = "r1";
        options.WalPath = walPath;
        options.WalRevision = "w1";
        options.StorageProviders.Register(backendProvider);
        options.StorageProviders.Register(walProvider);

        await using (EmbeddedKahunaNode node = new(options, NullLoggerFactory.Instance))
        {
            await node.StartAsync(TestContext.Current.CancellationToken);
            await node.WaitForLeaderForKeyAsync("tenant/table/k", TestContext.Current.CancellationToken);

            (KeyValueResponseType setType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "tenant/table/k", Encoding.UTF8.GetBytes("v"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            await node.FlushAsync();

            // The node's Raft log went to the registered WAL; it closes with the node.
            Assert.NotEmpty(walProvider.Wal!.ReadLogsRange(1, 1));
        }

        PersistenceBackendContext opened = Assert.Single(backendProvider.Opened);
        Assert.Equal(storagePath, opened.StoragePath);
        Assert.Equal("r1", opened.StorageRevision);
        Assert.Equal("v", Encoding.UTF8.GetString(backendProvider.Backend!.GetKeyValue("tenant/table/k")!.Value!));

        WalContext walOpened = Assert.Single(walProvider.Opened);
        Assert.Equal(walPath, walOpened.Path);
        Assert.Equal("w1", walOpened.Revision);

        // No shared memtable budget unless the node sets one.
        Assert.Null(opened.SharedMemory);
        Assert.Null(walOpened.SharedMemory);

        // Both engines keep their data on disk, so the node created their directories.
        Assert.True(Directory.Exists(storagePath));
        Assert.True(Directory.Exists(walPath));
    }

    [Fact]
    public async Task ABackup_RestoresThroughTheProviderOfItsEngine()
    {
        MemoryPersistenceBackend backend = new();
        backend.StoreKeyValues([new("k1", Encoding.UTF8.GetBytes("v1"), 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1)]);
        RecordingBackendProvider provider = new("recorded");
        StorageProviders providers = StorageProviders.CreateDefault();
        providers.Register(provider);

        BackupService service = MakeService(backend, "recorded", providers);
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await service.TakeFullAsync(ct: TestContext.Current.CancellationToken);

        string target = Path.Combine(tempRoot, "restored");
        await service.RestoreToAsync(full.BackupId, target, HLCTimestamp.Zero, ct: TestContext.Current.CancellationToken);

        Assert.NotEmpty(provider.OpenedAt);
        MemoryPersistenceBackend restored = MemoryPersistenceBackend.OpenCheckpoint(target);
        Assert.Equal("v1", Encoding.UTF8.GetString(restored.GetKeyValue("k1")!.Value!));
    }

    [Fact]
    public async Task ARestore_OfAnUnregisteredEngine_Refuses()
    {
        MemoryPersistenceBackend backend = new();
        backend.StoreKeyValues([new("k1", Encoding.UTF8.GetBytes("v1"), 1, 0, 0, 0, 0, 1, 0, 0, 1, 0, 1)]);

        BackupService service = MakeService(backend, "unregistered", StorageProviders.CreateDefault());
        Kahuna.Shared.Communication.Rest.KahunaBackupInfo full = await service.TakeFullAsync(ct: TestContext.Current.CancellationToken);

        string target = Path.Combine(tempRoot, "refused");
        Exception refusal = await Assert.ThrowsAnyAsync<Exception>(() =>
            service.RestoreToAsync(full.BackupId, target, HLCTimestamp.Zero, ct: TestContext.Current.CancellationToken));
        Assert.Contains("'unregistered' storage engine is not registered", refusal.Message);
    }

    private BackupService MakeService(MemoryPersistenceBackend backend, string storageType, StorageProviders providers)
    {
        InMemoryWAL wal = new(NullLogger<IRaft>.Instance);
        wal.Write([(1, [new RaftLog { Id = 1, Type = RaftLogType.Committed, Time = new HLCTimestamp(0, 100, 0) }])]);
        TestBackupService.StubRaft raft = new(wal, [new RaftPartitionRange { PartitionId = 1, State = RaftPartitionState.Active }]);
        string backupDir = Path.Combine(tempRoot, "backups");

        return new BackupService(
            raft,
            backend,
            backupDir,
            BackupTestStores.Manifests(backupDir),
            BackupTestStores.Artifacts(backupDir),
            storageType: storageType,
            storageRevision: "",
            flushBeforeCheckpoint: () => Task.CompletedTask,
            queryMinInFlight: () => Task.FromResult(HLCTimestamp.Zero),
            storageProviders: providers,
            acquireRetentionHold: raft.AcquireRetentionHold);
    }

    /// <summary>
    /// A node with a shared memtable budget hands one budget object to its WAL and to its store, so engines of
    /// the same kind find one accounting object between them, and an engine's object is created once.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheNodesWalAndStore_ReceiveOneSharedMemory(bool standalone)
    {
        RecordingBackendProvider backendProvider = new("recorded");
        RecordingWalProvider walProvider = new("recorded-wal");

        EmbeddedKahunaOptions options = Options(storage: "recorded", walStorage: "recorded-wal");
        options.StoragePath = Path.Combine(tempRoot, "store");
        options.WalPath = Path.Combine(tempRoot, "wal");
        options.StorageSharedMemtableBudgetMb = 24;
        options.StorageSharedCacheMb = 40;
        options.StorageProviders.Register(backendProvider);
        options.StorageProviders.Register(walProvider);

        await using (EmbeddedKahunaNode node = standalone
            ? new(options, NullLoggerFactory.Instance)
            : new(options, new Kahuna.Server.Communication.Internode.MemoryInterNodeCommmunication(), new Kommander.Communication.Memory.InMemoryCommunication(),
                new Kommander.Discovery.StaticDiscovery([]), NullLoggerFactory.Instance))
        {
        }

        SharedStorageMemory walMemory = Assert.Single(walProvider.Opened).SharedMemory!;
        SharedStorageMemory storeMemory = Assert.Single(backendProvider.Opened).SharedMemory!;
        Assert.Same(walMemory, storeMemory);
        Assert.Equal(24L * 1024 * 1024, walMemory.MemtableBudgetBytes);
        Assert.Equal(40L * 1024 * 1024, walMemory.CacheBytes);

        object engineResources = walMemory.GetOrAdd(memory => new List<long> { memory.MemtableBudgetBytes, memory.CacheBytes });
        Assert.Same(engineResources, storeMemory.GetOrAdd<List<long>>(_ => throw new InvalidOperationException("created twice")));
        Assert.Equal([24L * 1024 * 1024, 40L * 1024 * 1024], (List<long>)engineResources);
    }

    /// <summary>The node's extension options reach the contexts of the providers that open its WAL and its store.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheNodesWalAndStore_ReceiveTheExtensionOptions(bool standalone)
    {
        RecordingBackendProvider backendProvider = new("recorded");
        RecordingWalProvider walProvider = new("recorded-wal");

        EmbeddedKahunaOptions options = Options(storage: "recorded", walStorage: "recorded-wal");
        options.StoragePath = Path.Combine(tempRoot, "store");
        options.WalPath = Path.Combine(tempRoot, "wal");
        options.ExtensionOptions = new Dictionary<string, string> { ["recorded.mode"] = "fast" };
        options.StorageProviders.Register(backendProvider);
        options.StorageProviders.Register(walProvider);

        await using (EmbeddedKahunaNode node = standalone
            ? new(options, NullLoggerFactory.Instance)
            : new(options, new Kahuna.Server.Communication.Internode.MemoryInterNodeCommmunication(), new Kommander.Communication.Memory.InMemoryCommunication(),
                new Kommander.Discovery.StaticDiscovery([]), NullLoggerFactory.Instance))
        {
        }

        Assert.Equal("fast", Assert.Single(walProvider.Opened).ExtensionOptions["recorded.mode"]);
        PersistenceBackendContext store = Assert.Single(backendProvider.Opened);
        Assert.Equal("fast", store.ExtensionOptions["recorded.mode"]);
        Assert.Same(store.ExtensionOptions, store.Configuration.ExtensionOptions);
    }

    private static EmbeddedKahunaOptions Options(string storage, string walStorage) => new()
    {
        Storage = storage,
        WalStorage = walStorage,
        InitialPartitions = 1,
        HeartbeatInterval = TimeSpan.FromMilliseconds(100),
        StartElectionTimeout = 500,
        EndElectionTimeout = 1500,
    };

    /// <summary>
    /// An engine registered under its own name that stores like the memory backend, but keeps its data
    /// "on disk" so the node creates its directory, and records what it was asked to open.
    /// </summary>
    private sealed class RecordingBackendProvider(string storageType) : IPersistenceBackendProvider
    {
        private readonly IPersistenceBackendProvider memory = StorageProviders.CreateDefault().GetBackend("memory");

        public List<PersistenceBackendContext> Opened { get; } = [];

        public List<string> OpenedAt { get; } = [];

        public MemoryPersistenceBackend? Backend { get; private set; }

        public string StorageType => storageType;

        public bool KeepsDataOnDisk => true;

        public IPersistenceBackend Open(PersistenceBackendContext context)
        {
            Opened.Add(context);
            Backend = new MemoryPersistenceBackend();
            return Backend;
        }

        public IPersistenceBackend OpenAt(string directory, string revision, ILogger logger)
        {
            OpenedAt.Add(directory);
            return memory.OpenAt(directory, revision, logger);
        }

        public string DataDirectory(string storageRoot, string revision) => storageRoot;

        public bool HasData(string storageRoot, string revision) => false;

        public void MergeCheckpointInto(string targetDirectory, string targetRevision, string sourceDirectory, CancellationToken ct) =>
            memory.MergeCheckpointInto(targetDirectory, targetRevision, sourceDirectory, ct);
    }

    /// <summary>A WAL registered under its own name, in memory, that records what it was asked to open.</summary>
    private sealed class RecordingWalProvider(string storageType) : IWalProvider
    {
        public List<WalContext> Opened { get; } = [];

        public InMemoryWAL? Wal { get; private set; }

        public string StorageType => storageType;

        public bool KeepsDataOnDisk => true;

        public IWAL Open(WalContext context)
        {
            Opened.Add(context);
            Wal = new InMemoryWAL(context.Logger);
            return Wal;
        }
    }
}

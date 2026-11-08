using System.Runtime.Loader;
using System.Text;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Persistence.Providers;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Covers backup targets chosen by name from <see cref="StorageProviders"/>: the built-in <c>local</c> target,
/// a target loaded with <c>--extension-assembly</c>, the validation of the target at startup, the start of the
/// node's backup storage before the join, and the storage a PITR bootstrap opens of its own.
/// </summary>
public sealed class TestBackupStorageProviders : IDisposable
{
    private const string RemoteTarget = "test-remote";

    private readonly string tempRoot = Path.Combine(Path.GetTempPath(), "kahuna_backup_targets_" + Guid.NewGuid().ToString("N"));

    public TestBackupStorageProviders() => Directory.CreateDirectory(tempRoot);

    public void Dispose()
    {
        try { Directory.Delete(tempRoot, recursive: true); } catch { /* best effort */ }
    }

    private string Dir(string name) => Path.Combine(tempRoot, name);

    /// <summary>Where the storage fixture assembly is built, next to this assembly's output.</summary>
    private static string FixturePath()
    {
        string baseDirectory = AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar);

        string path = baseDirectory.Replace(
            Path.DirectorySeparatorChar + "Kahuna.Server.Tests" + Path.DirectorySeparatorChar,
            Path.DirectorySeparatorChar + "Kahuna.TestStorageExtension" + Path.DirectorySeparatorChar,
            StringComparison.Ordinal);

        return Path.Combine(path, "Kahuna.TestStorageExtension.dll");
    }

    private EmbeddedKahunaOptions Options(StorageProviders providers, string target, string? scratch, IReadOnlyDictionary<string, string>? extensionOptions = null) => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1,
        HeartbeatInterval = TimeSpan.FromMilliseconds(100),
        StartElectionTimeout = 500,
        EndElectionTimeout = 1500,
        StorageProviders = providers,
        BackupDir = Dir("backups"),
        BackupTarget = target,
        BackupScratchDir = scratch ?? "",
        ExtensionOptions = extensionOptions ?? new Dictionary<string, string>(StringComparer.Ordinal),
    };

    private static async Task SetKey(EmbeddedKahunaNode node, string key, string value)
    {
        (KeyValueResponseType type, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
            HLCTimestamp.Zero, key, Encoding.UTF8.GetBytes(value), null, -1,
            KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);

        Assert.Equal(KeyValueResponseType.Set, type);
    }

    // ── the registry ────────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void TheDefaultRegistry_HasTheLocalTarget()
    {
        StorageProviders providers = StorageProviders.CreateDefault();

        Assert.Equal([LocalBackupStorageProvider.Name], providers.BackupStorageTypes);
        Assert.IsType<LocalBackupStorageProvider>(providers.FindBackupStorage("local"));

        // The name is compared ordinally: "LOCAL" is another target, not an alias.
        Assert.Null(providers.FindBackupStorage("LOCAL"));
    }

    // ── the extension loader ──────────────────────────────────────────────────────────────────────

    [Fact]
    public void AnExtensionAssembly_RegistersItsBackupTarget_AndLogsIt()
    {
        StorageProviders providers = StorageProviders.CreateDefault();
        RecordingLogger logger = new();

        ExtensionAssemblyLoader.Load([FixturePath()], logger, providers);

        IBackupStorageProvider provider = Assert.IsAssignableFrom<IBackupStorageProvider>(providers.FindBackupStorage(RemoteTarget));
        Assert.True(provider.RequiresLocalScratch);
        Assert.StartsWith("kahuna-extension:", AssemblyLoadContext.GetLoadContext(provider.GetType().Assembly)!.Name, StringComparison.Ordinal);
        Assert.Contains("backup:" + RemoteTarget, Assert.Single(logger.Lines), StringComparison.Ordinal);
    }

    [Fact]
    public void ABackupTarget_ThatTakesARegisteredName_FailsWithItsOwnMessage()
    {
        // "local" is registered the same way before any extension loads, so this is also the refusal an
        // extension that publishes "local" gets.
        StorageProviders providers = StorageProviders.CreateDefault();
        RecordingBackupStorageProvider registered = new(RemoteTarget, requiresLocalScratch: true, Dir("root"));
        providers.Register(registered);

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            ExtensionAssemblyLoader.Load([FixturePath()], NullLogger.Instance, providers));

        Assert.Contains($"registers backup target '{RemoteTarget}', which is already registered", ex.Message, StringComparison.Ordinal);
        Assert.Same(registered, providers.FindBackupStorage(RemoteTarget));
    }

    [Fact]
    public void AHostThatTakesNoStorage_RefusesAnAssemblyThatPublishesABackupTarget()
    {
        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            ExtensionAssemblyLoader.Load([FixturePath()], NullLogger.Instance));

        Assert.Contains("backup target", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnExtensionBuiltAgainstANewerHostAssembly_GetsAVersionMessage()
    {
        string coreName = typeof(KahunaConfiguration).Assembly.GetName().Name!;
        Version hostVersion = typeof(KahunaConfiguration).Assembly.GetName().Version!;
        Version newer = new(hostVersion.Major + 1, 0, 0, 0);

        FileLoadException mismatch = new("bind failed", $"{coreName}, Version={newer}, Culture=neutral, PublicKeyToken=null");
        string message = Assert.IsType<string>(ExtensionAssemblyLoader.HostVersionMismatch(mismatch, "/ext/a.dll"));

        Assert.Contains($"is built against {coreName} {newer}", message, StringComparison.Ordinal);
        Assert.Contains($"this server has {coreName} {hostVersion}", message, StringComparison.Ordinal);

        // The same or an older version binds, and an assembly the server does not provide is a missing
        // dependency, not a version mismatch.
        Assert.Null(ExtensionAssemblyLoader.HostVersionMismatch(
            new FileLoadException("x", $"{coreName}, Version={hostVersion}, Culture=neutral, PublicKeyToken=null"), "/ext/a.dll"));
        Assert.Null(ExtensionAssemblyLoader.HostVersionMismatch(
            new FileNotFoundException("x", "Acme.Private, Version=9.0.0.0, Culture=neutral, PublicKeyToken=null"), "/ext/a.dll"));
        Assert.Null(ExtensionAssemblyLoader.HostVersionMismatch(new InvalidOperationException("x"), "/ext/a.dll"));
    }

    [Fact]
    public async Task ALoadedBackupTarget_TakesListsAndRestoresBackups_InItsOwnRoot()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        StorageProviders providers = StorageProviders.CreateDefault();
        ExtensionAssemblyLoader.Load([FixturePath()], NullLogger.Instance, providers);

        string remoteRoot = Dir("remote");

        await using EmbeddedKahunaNode node = new(
            Options(providers, RemoteTarget, Dir("scratch"), new Dictionary<string, string> { [RemoteTarget + ".root"] = remoteRoot }),
            NullLoggerFactory.Instance);
        await node.StartAsync(ct);

        await SetKey(node, "targets/k", "v1");
        KahunaBackupInfo full = await node.Kahuna.TakeFullBackupAsync(ct);

        // The manifest and the artifacts are in the target's root, not in the backup directory.
        Assert.True(File.Exists(Path.Combine(remoteRoot, full.BackupId.ToString("N") + ".manifest")));
        Assert.False(Directory.Exists(Dir("backups")) && Directory.EnumerateFiles(Dir("backups"), "*.manifest").Any());

        IReadOnlyList<KahunaBackupInfo> listed = await node.Kahuna.ListBackupsAsync(ct);
        Assert.Contains(listed, backup => backup.BackupId == full.BackupId);

        await SetKey(node, "targets/k2", "v2");
        KahunaBackupInfo incremental = await node.Kahuna.TakeIncrementalBackupAsync(full.BackupId, ct);

        KahunaRestoreResponse restored = await node.Kahuna.RestoreToAsync(incremental.BackupId, Dir("restored"), targetTimeMs: 0, ct);
        Assert.Equal(2, restored.Chain.Count);
    }

    [Fact]
    public void ALoadedBackupTarget_RefusesAMissingOrUnknownSetting_WhenTheNodeOpensIt()
    {
        StorageProviders providers = StorageProviders.CreateDefault();
        ExtensionAssemblyLoader.Load([FixturePath()], NullLogger.Instance, providers);

        KahunaServerException missing = Assert.Throws<KahunaServerException>(() =>
            new EmbeddedKahunaNode(Options(providers, RemoteTarget, Dir("scratch")), NullLoggerFactory.Instance));
        Assert.Contains(RemoteTarget + ".root", missing.Message, StringComparison.Ordinal);

        KahunaServerException unknown = Assert.Throws<KahunaServerException>(() => new EmbeddedKahunaNode(
            Options(providers, RemoteTarget, Dir("scratch"), new Dictionary<string, string>
            {
                [RemoteTarget + ".root"] = Dir("remote"),
                [RemoteTarget + ".rooot"] = Dir("remote"),
            }),
            NullLoggerFactory.Instance));
        Assert.Contains(RemoteTarget + ".rooot", unknown.Message, StringComparison.Ordinal);
    }

    // ── validation ───────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void AnUnregisteredTarget_FailsValidation_AndNamesTheRegisteredOnes()
    {
        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            new EmbeddedKahunaNode(Options(StorageProviders.CreateDefault(), "gcs", Dir("scratch")), NullLoggerFactory.Instance));

        Assert.Contains("Invalid backup target: 'gcs'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("local", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ATargetThatNeedsScratch_FailsValidation_WithoutAScratchDirectory()
    {
        StorageProviders providers = StorageProviders.CreateDefault();
        RecordingBackupStorageProvider provider = new(RemoteTarget, requiresLocalScratch: true, Dir("root"));
        providers.Register(provider);

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            new EmbeddedKahunaNode(Options(providers, RemoteTarget, scratch: null), NullLoggerFactory.Instance));

        Assert.Contains("requires a local scratch directory", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, provider.Opens);
    }

    [Fact]
    public void TheServerShapes_ResolveTheSameTarget_FromOneCommandLine()
    {
        StorageProviders providers = StorageProviders.CreateDefault();
        RecordingBackupStorageProvider provider = new(RemoteTarget, requiresLocalScratch: true, Dir("root"));
        providers.Register(provider);

        KahunaCommandLineOptions opts = CommandLine.Parser.Default.ParseArguments<KahunaCommandLineOptions>(
        [
            "--storage-revision", "v1",
            "--pitr-backup-dir", Dir("backups"),
            "--pitr-backup-target", RemoteTarget,
            "--pitr-backup-scratch-dir", Dir("scratch"),
        ]).Value;

        KahunaConfiguration server = ServerConfigurationFactory.Create(opts, null, providers);
        EmbeddedKahunaOptions standalone = EmbeddedOptionsFactory.CreateEmbeddedOptions(opts, transportSecurity: null, null, providers);

        Assert.Equal(RemoteTarget, server.BackupTarget);
        Assert.Same(provider, server.StorageProviders.FindBackupStorage(server.BackupTarget));
        Assert.Equal(RemoteTarget, standalone.BackupTarget);
        Assert.Same(provider, standalone.StorageProviders.FindBackupStorage(standalone.BackupTarget));

        opts.PitrBackupScratchDir = "";
        Assert.Throws<KahunaServerException>(() => ServerConfigurationFactory.Create(opts, null, providers));
    }

    // ── the node's storage lifecycle ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task TheNode_OpensAndStartsItsStorageOnce_BeforeItJoins_AndReleasesItOnDispose()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        StorageProviders providers = StorageProviders.CreateDefault();
        RecordingBackupStorageProvider provider = new(RemoteTarget, requiresLocalScratch: true, Dir("root"));
        providers.Register(provider);

        EmbeddedKahunaNode node = new(Options(providers, RemoteTarget, Dir("scratch")), NullLoggerFactory.Instance);
        provider.JoinedProbe = () => node.Raft.Joined;
        try
        {
            // Construction opens the storage and does no I/O.
            Assert.Equal(1, provider.Opens);
            Assert.Equal(0, provider.Starts);
            Assert.False(Directory.Exists(Dir("root")));

            await node.StartAsync(ct);

            Assert.Equal(1, provider.Starts);
            Assert.False(provider.JoinedAtStart);
            Assert.True(node.Raft.Joined);
            Assert.Equal(Dir("scratch"), provider.LastContext!.ScratchDir);
            Assert.Equal(Dir("backups"), provider.LastContext.BackupDir);

            await SetKey(node, "lifecycle/k", "v");
            await node.Kahuna.TakeFullBackupAsync(ct);
            await node.Kahuna.ListBackupsAsync(ct);

            Assert.Equal(1, provider.Opens);
            Assert.Equal(1, provider.Starts);
        }
        finally
        {
            await node.DisposeAsync();
        }

        Assert.Equal(1, provider.Disposes);
    }

    [Fact]
    public async Task ATargetThatCannotStart_StopsTheNodeStart_AndIsTriedAgainAtTheNextOperation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        StorageProviders providers = StorageProviders.CreateDefault();
        RecordingBackupStorageProvider provider = new(RemoteTarget, requiresLocalScratch: true, Dir("root")) { FailStarts = 2 };
        providers.Register(provider);

        await using EmbeddedKahunaNode node = new(Options(providers, RemoteTarget, Dir("scratch")), NullLoggerFactory.Instance);

        InvalidOperationException startFailure = await Assert.ThrowsAsync<InvalidOperationException>(() => node.StartAsync(ct));
        Assert.Equal(RecordingBackupStorageProvider.StartFailureMessage, startFailure.Message);
        Assert.False(node.Raft.Joined);

        // A node that did start without its storage (an embedding host that ignored the failure) gets the
        // failure as a typed backup outcome, and the next operation tries the start again.
        KahunaBackupException mapped = await Assert.ThrowsAsync<KahunaBackupException>(() => node.Kahuna.ListBackupsAsync(ct));
        Assert.Contains("(operation ", mapped.Message, StringComparison.Ordinal);
        Assert.Equal(2, provider.Starts);

        Assert.Empty(await node.Kahuna.ListBackupsAsync(ct));
        Assert.Equal(3, provider.Starts);
    }

    [Fact]
    public async Task ANodeAssembledWithoutTheStartStep_StartsItsStorageAtTheFirstOperation()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        StorageProviders providers = StorageProviders.CreateDefault();
        RecordingBackupStorageProvider provider = new(RemoteTarget, requiresLocalScratch: true, Dir("root"));
        providers.Register(provider);

        await using EmbeddedKahunaNode node = new(Options(providers, RemoteTarget, Dir("scratch")), NullLoggerFactory.Instance);

        Assert.True(node.Kahuna.IsBackupConfigured);

        // Two first operations at once start the storage one time.
        await Task.WhenAll(node.Kahuna.ListBackupsAsync(ct), node.Kahuna.ListBackupsAsync(ct));

        Assert.Equal(1, provider.Starts);
    }

    [Fact]
    public async Task TheLocalTarget_NeverCallsAnotherRegisteredTarget()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        StorageProviders providers = StorageProviders.CreateDefault();
        RecordingBackupStorageProvider provider = new(RemoteTarget, requiresLocalScratch: true, Dir("root"));
        providers.Register(provider);

        await using (EmbeddedKahunaNode node = new(Options(providers, LocalBackupStorageProvider.Name, scratch: null), NullLoggerFactory.Instance))
        {
            await node.StartAsync(ct);
            await SetKey(node, "local/k", "v");
            KahunaBackupInfo full = await node.Kahuna.TakeFullBackupAsync(ct);

            Assert.True(File.Exists(Path.Combine(Dir("backups"), full.BackupId.ToString("N") + ".manifest")));
        }

        // A node with no backup directory has no storage to open either.
        EmbeddedKahunaOptions noBackups = Options(providers, RemoteTarget, Dir("scratch"));
        noBackups.BackupDir = "";
        await using (EmbeddedKahunaNode node = new(noBackups, NullLoggerFactory.Instance))
        {
            await node.StartAsync(ct);
            Assert.False(node.Kahuna.IsBackupConfigured);
        }

        Assert.Equal(0, provider.Opens);
    }

    // ── the PITR bootstrap ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ThePitrBootstrap_OpensStartsAndReleasesAStorageOfItsOwn()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        StorageProviders providers = StorageProviders.CreateDefault();
        RecordingBackupStorageProvider provider = new(RemoteTarget, requiresLocalScratch: true, Dir("root"));
        providers.Register(provider);

        // A seed puts a base image into the storage path, so it needs an engine that keeps its data on disk.
        KahunaConfiguration configuration = new()
        {
            Storage = "sqlite",
            StoragePath = Dir("store"),
            StorageRevision = "v1",
            StorageProviders = providers,
            BackupDir = Dir("backups"),
            BackupTarget = RemoteTarget,
            BackupScratchDir = Dir("scratch"),
        };

        // No chain at all, with the fallback on: the plan says to join without a seed.
        PitrBootstrapPlan plan = await BackupFacade.PrepareBootstrapAsync(
            configuration, leafBackupId: null, HLCTimestamp.Zero, TimeSpan.FromHours(1), TimeSpan.FromMinutes(30),
            fallbackToJoin: true, NullLogger.Instance, ct);

        Assert.Equal(PitrBootstrapAction.Fallback, plan.Action);
        Assert.Equal(1, provider.Opens);
        Assert.Equal(1, provider.Starts);
        Assert.Equal(1, provider.Disposes);
    }

    [Fact]
    public async Task ThePitrBootstrap_RefusesANodeWithoutABackupDirectory()
    {
        KahunaConfiguration configuration = new() { Storage = "memory", StorageRevision = "v1" };

        await Assert.ThrowsAsync<KahunaBackupException>(() => BackupFacade.PrepareBootstrapAsync(
            configuration, leafBackupId: null, HLCTimestamp.Zero, TimeSpan.FromHours(1), TimeSpan.FromMinutes(30),
            fallbackToJoin: true, NullLogger.Instance, TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// A backup target over local directories that counts its calls. With <see cref="JoinedProbe"/> set, it
    /// records whether the node had already joined its cluster when the storage started.
    /// </summary>
    private sealed class RecordingBackupStorageProvider(string name, bool requiresLocalScratch, string root) : IBackupStorageProvider
    {
        public const string StartFailureMessage = "the recording target refuses to start";

        private int opens;

        private int starts;

        private int disposes;

        public string TargetName => name;

        public bool RequiresLocalScratch => requiresLocalScratch;

        public string Root => root;

        /// <summary>How many starts fail before one succeeds.</summary>
        public int FailStarts { get; init; }

        public int Opens => Volatile.Read(ref opens);

        public int Starts => Volatile.Read(ref starts);

        public int Disposes => Volatile.Read(ref disposes);

        public Func<bool>? JoinedProbe { get; set; }

        public bool JoinedAtStart { get; private set; }

        public BackupStorageContext? LastContext { get; private set; }

        public IBackupStorage Open(BackupStorageContext context)
        {
            Interlocked.Increment(ref opens);
            LastContext = context;
            return new Storage(this);
        }

        private sealed class Storage(RecordingBackupStorageProvider owner) : IBackupStorage
        {
            private IBackupStorageTarget? manifests;

            private IBackupArtifactStore? artifacts;

            public IBackupStorageTarget Manifests => manifests ?? throw new InvalidOperationException("Not started.");

            public IBackupArtifactStore Artifacts => artifacts ?? throw new InvalidOperationException("Not started.");

            public async Task StartAsync(CancellationToken ct)
            {
                int attempt = Interlocked.Increment(ref owner.starts);

                // Gives a concurrent first operation the time to race this one.
                await Task.Yield();

                if (attempt <= owner.FailStarts)
                    throw new InvalidOperationException(StartFailureMessage);

                owner.JoinedAtStart = owner.JoinedProbe?.Invoke() ?? false;
                manifests = new LocalDirectoryStorageTarget(owner.Root);
                artifacts = new LocalDirectoryArtifactStore(owner.Root);
            }

            public void Dispose() => Interlocked.Increment(ref owner.disposes);
        }
    }

    private sealed class RecordingLogger : ILogger
    {
        public List<string> Lines { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Lines.Add(formatter(state, exception));
    }
}

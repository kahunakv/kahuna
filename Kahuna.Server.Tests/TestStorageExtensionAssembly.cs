using System.Reflection;
using System.Runtime.Loader;
using System.Text;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Kahuna.Server.KeyValues;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Providers;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Covers storage engines loaded with <c>--extension-assembly</c>: an assembly that publishes an
/// <see cref="IPersistenceBackendProvider"/> and an <see cref="IWalProvider"/> registers them, a node opens
/// them by name, their private dependencies load from the extension's own directory, and an engine may not
/// take a name that is already registered.
/// </summary>
public sealed class TestStorageExtensionAssembly
{
    private const string TypeName = "test-extension";

    /// <summary>Where the storage fixture assembly is built, next to this assembly's output, as for the function fixture.</summary>
    private static string FixturePath()
    {
        string baseDirectory = AppContext.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar);

        string path = baseDirectory.Replace(
            Path.DirectorySeparatorChar + "Kahuna.Server.Tests" + Path.DirectorySeparatorChar,
            Path.DirectorySeparatorChar + "Kahuna.TestStorageExtension" + Path.DirectorySeparatorChar,
            StringComparison.Ordinal);

        return Path.Combine(path, "Kahuna.TestStorageExtension.dll");
    }

    [Fact]
    public async Task AnExtensionAssembly_RegistersItsStorageEngines_AndANodeOpensThem()
    {
        StorageProviders providers = StorageProviders.CreateDefault();
        ExtensionAssemblyLoader.Load([FixturePath()], NullLogger.Instance, providers);

        IPersistenceBackendProvider backend = Assert.IsAssignableFrom<IPersistenceBackendProvider>(providers.FindBackend(TypeName));
        IWalProvider wal = Assert.IsAssignableFrom<IWalProvider>(providers.FindWal(TypeName));
        Assert.StartsWith("kahuna-extension:", AssemblyLoadContext.GetLoadContext(backend.GetType().Assembly)!.Name, StringComparison.Ordinal);
        Assert.StartsWith("kahuna-extension:", AssemblyLoadContext.GetLoadContext(wal.GetType().Assembly)!.Name, StringComparison.Ordinal);

        await using (EmbeddedKahunaNode node = new(new EmbeddedKahunaOptions
        {
            Storage = TypeName,
            WalStorage = TypeName,
            StorageProviders = providers,
            InitialPartitions = 1,
            HeartbeatInterval = TimeSpan.FromMilliseconds(100),
            StartElectionTimeout = 500,
            EndElectionTimeout = 1500,
        }, NullLoggerFactory.Instance))
        {
            await node.StartAsync(TestContext.Current.CancellationToken);
            await node.WaitForLeaderForKeyAsync("tenant/table/k", TestContext.Current.CancellationToken);

            (KeyValueResponseType setType, _, _) = await node.Kahuna.LocateAndTrySetKeyValue(
                HLCTimestamp.Zero, "tenant/table/k", Encoding.UTF8.GetBytes("v"), null, -1,
                KeyValueFlags.Set, 0, KeyValueDurability.Persistent, TestContext.Current.CancellationToken);
            Assert.Equal(KeyValueResponseType.Set, setType);

            await node.FlushAsync();

            // The flush wrote through the extension's backend, whose row store comes from the extension's
            // private dependency, loaded into the extension's context from the extension's directory.
            IPersistenceBackend opened = OpenedBackends(backend).Last();
            Assert.Equal("v", Encoding.UTF8.GetString(opened.GetKeyValue("tenant/table/k")!.Value!));
        }

        Assembly store = Assert.Single(
            AppDomain.CurrentDomain.GetAssemblies(),
            assembly => assembly.GetName().Name == "Kahuna.TestStorageExtension.Store"
                        && AssemblyLoadContext.GetLoadContext(assembly) == AssemblyLoadContext.GetLoadContext(backend.GetType().Assembly));
        Assert.Equal(Path.GetDirectoryName(FixturePath()), Path.GetDirectoryName(store.Location));
    }

    [Fact]
    public void AStorageEngine_ThatTakesARegisteredName_FailsWithItsOwnMessage()
    {
        StorageProviders providers = StorageProviders.CreateDefault();
        providers.Register(new NamedEngine(TypeName));

        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            ExtensionAssemblyLoader.Load([FixturePath()], NullLogger.Instance, providers));

        Assert.Contains($"registers storage type '{TypeName}', which is already registered", ex.Message, StringComparison.Ordinal);
        Assert.IsType<NamedEngine>(providers.FindBackend(TypeName));
    }

    [Fact]
    public void AHostThatTakesNoStorageEngines_RefusesAnAssemblyThatPublishesOne()
    {
        KahunaServerException ex = Assert.Throws<KahunaServerException>(() =>
            ExtensionAssemblyLoader.Load([FixturePath()], NullLogger.Instance));

        Assert.Contains("publishes a storage engine", ex.Message, StringComparison.Ordinal);
    }

    // The fixture keeps the backends it opened in a static list; the test reads it by reflection, since it
    // does not compile against the fixture.
    private static List<IPersistenceBackend> OpenedBackends(IPersistenceBackendProvider provider)
    {
        PropertyInfo property = provider.GetType().GetProperty("Opened", BindingFlags.Public | BindingFlags.Static)!;
        return (List<IPersistenceBackend>)property.GetValue(null)!;
    }

    private sealed class NamedEngine(string storageType) : IPersistenceBackendProvider
    {
        public string StorageType => storageType;

        public bool KeepsDataOnDisk => false;

        public IPersistenceBackend Open(PersistenceBackendContext context) => throw new NotSupportedException();

        public IPersistenceBackend OpenAt(string directory, string revision, ILogger logger) => throw new NotSupportedException();

        public string DataDirectory(string storageRoot, string revision) => storageRoot;

        public bool HasData(string storageRoot, string revision) => false;

        public void MergeCheckpointInto(string targetDirectory, string targetRevision, string sourceDirectory, CancellationToken ct) =>
            throw new NotSupportedException();
    }
}

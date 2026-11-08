
using System.Collections.Concurrent;
using Kahuna.Server.Persistence.Pitr;

namespace Kahuna.Server.Persistence.Providers;

/// <summary>
/// The storage a node can use, by name: the persistence backends (<c>--storage</c>), the Raft WALs
/// (<c>--wal-storage</c>) and the backup targets (<c>--pitr-backup-target</c>). <see cref="CreateDefault"/>
/// holds Kahuna's own; a host adds another with one of the <c>Register</c> overloads before the node starts,
/// and a registration under a name that exists replaces it.
/// </summary>
public sealed class StorageProviders
{
    private readonly ConcurrentDictionary<string, IPersistenceBackendProvider> backends = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, IWalProvider> wals = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, IBackupStorageProvider> backupStorages = new(StringComparer.Ordinal);

    /// <summary>
    /// A registry with Kahuna's own storage: the memory engine, the local backup target, and on platforms with
    /// native storage libraries (every build but the thread-free browser one) RocksDB and SQLite.
    /// </summary>
    public static StorageProviders CreateDefault()
    {
        StorageProviders providers = new();
        providers.Register(new LocalBackupStorageProvider());
        providers.Register(new MemoryPersistenceBackendProvider());
        providers.Register(new MemoryWalProvider());
#if !KAHUNA_THREAD_FREE
        providers.Register(new RocksDbPersistenceBackendProvider());
        providers.Register(new SqlitePersistenceBackendProvider());
        providers.Register(new RocksDbWalProvider());
        providers.Register(new SqliteWalProvider());
#endif
        return providers;
    }

    /// <summary>The registered persistence backend types.</summary>
    public IReadOnlyCollection<string> BackendTypes => [.. backends.Keys.Order(StringComparer.Ordinal)];

    /// <summary>The registered WAL types.</summary>
    public IReadOnlyCollection<string> WalTypes => [.. wals.Keys.Order(StringComparer.Ordinal)];

    /// <summary>The registered backup target names.</summary>
    public IReadOnlyCollection<string> BackupStorageTypes => [.. backupStorages.Keys.Order(StringComparer.Ordinal)];

    /// <summary>Registers <paramref name="provider"/> under its type name, replacing one registered under the same name.</summary>
    public void Register(IPersistenceBackendProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentException.ThrowIfNullOrWhiteSpace(provider.StorageType);
        backends[provider.StorageType] = provider;
    }

    /// <summary>Registers <paramref name="provider"/> under its type name, replacing one registered under the same name.</summary>
    public void Register(IWalProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentException.ThrowIfNullOrWhiteSpace(provider.StorageType);
        wals[provider.StorageType] = provider;
    }

    /// <summary>Registers <paramref name="provider"/> under its target name, replacing one registered under the same name.</summary>
    public void Register(IBackupStorageProvider provider)
    {
        ArgumentNullException.ThrowIfNull(provider);
        ArgumentException.ThrowIfNullOrWhiteSpace(provider.TargetName);
        backupStorages[provider.TargetName] = provider;
    }

    /// <summary>
    /// The backend registered as <paramref name="storageType"/>. An empty name is the memory backend, as
    /// a backup records for a store that never named its engine. Null when none is registered.
    /// </summary>
    public IPersistenceBackendProvider? FindBackend(string? storageType) =>
        backends.GetValueOrDefault(string.IsNullOrEmpty(storageType) ? MemoryPersistenceBackendProvider.TypeName : storageType);

    /// <summary>The WAL registered as <paramref name="storageType"/>, or null.</summary>
    public IWalProvider? FindWal(string? storageType) =>
        storageType is null ? null : wals.GetValueOrDefault(storageType);

    /// <summary>
    /// The backend registered as <paramref name="storageType"/>; throws <see cref="KahunaServerException"/>
    /// naming the registered types when there is none.
    /// </summary>
    public IPersistenceBackendProvider GetBackend(string? storageType) =>
        FindBackend(storageType)
        ?? throw new KahunaServerException(
            $"Invalid storage type: '{storageType}'. Registered storage types: {string.Join(", ", BackendTypes)}.");

    /// <summary>
    /// The WAL registered as <paramref name="storageType"/>; throws <see cref="KahunaServerException"/>
    /// naming the registered types when there is none.
    /// </summary>
    public IWalProvider GetWal(string? storageType) =>
        FindWal(storageType)
        ?? throw new KahunaServerException(
            $"Invalid WAL storage type: '{storageType}'. Registered WAL storage types: {string.Join(", ", WalTypes)}.");

    /// <summary>The backup target registered as <paramref name="targetName"/>, or null.</summary>
    public IBackupStorageProvider? FindBackupStorage(string? targetName) =>
        targetName is null ? null : backupStorages.GetValueOrDefault(targetName);

    /// <summary>
    /// The backup target registered as <paramref name="targetName"/>; throws <see cref="KahunaServerException"/>
    /// naming the registered targets when there is none.
    /// </summary>
    public IBackupStorageProvider GetBackupStorage(string? targetName) =>
        FindBackupStorage(targetName)
        ?? throw new KahunaServerException(
            $"Invalid backup target: '{targetName}'. Registered backup targets: {string.Join(", ", BackupStorageTypes)}. " +
            "A target that is not built in comes from an --extension-assembly.");
}

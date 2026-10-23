using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Persistence.Providers;
using Kahuna.TestStorageExtension.Store;
using Kommander.Time;
using Kommander.WAL;
using Microsoft.Extensions.Logging;

namespace Kahuna.TestStorageExtension;

/// <summary>
/// The storage engine the extension-assembly tests load as <c>--storage test-extension</c>: current rows
/// only, in memory, in a <see cref="RowStore{TRow}"/> from this assembly's private dependency. Every
/// backend it opens is recorded in <see cref="Opened"/>, so a test can read what the node wrote.
/// </summary>
public sealed class TestStorageEngine : IPersistenceBackendProvider
{
    /// <summary>The storage type name.</summary>
    public const string TypeName = "test-extension";

    /// <summary>The backends opened by every instance, newest last.</summary>
    public static List<IPersistenceBackend> Opened { get; } = [];

    public string StorageType => TypeName;

    public bool KeepsDataOnDisk => false;

    public IPersistenceBackend Open(PersistenceBackendContext context)
    {
        TestBackend backend = new();
        lock (Opened)
            Opened.Add(backend);

        return backend;
    }

    public IPersistenceBackend OpenAt(string directory, string revision, ILogger logger) =>
        throw new NotSupportedException("The test engine keeps no checkpoints.");

    public string DataDirectory(string storageRoot, string revision) => storageRoot;

    public bool HasData(string storageRoot, string revision) => false;

    public void MergeCheckpointInto(string targetDirectory, string targetRevision, string sourceDirectory, CancellationToken ct) =>
        throw new NotSupportedException("The test engine keeps no checkpoints.");

    private sealed class TestBackend : IPersistenceBackend
    {
        private readonly RowStore<ReadOnlyKeyValueEntry> keyValues = new();

        private readonly RowStore<LockRow> locks = new();

        public bool StoreLocks(List<PersistenceRequestItem> items)
        {
            foreach (PersistenceRequestItem item in items)
                locks.Put(item.Key, new LockRow(
                    item.Value,
                    item.Revision,
                    new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
                    new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
                    new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                    (LockState)item.State));

            return true;
        }

        public bool StoreKeyValues(List<PersistenceRequestItem> items)
        {
            foreach (PersistenceRequestItem item in items)
            {
                ReadOnlyKeyValueEntry? current = keyValues.Get(item.Key);
                if (current is not null && current.Revision > item.Revision)
                    continue;

                keyValues.Put(item.Key, new ReadOnlyKeyValueEntry(
                    item.Value,
                    item.Revision,
                    new(item.ExpiresNode, item.ExpiresPhysical, item.ExpiresCounter),
                    new(item.LastUsedNode, item.LastUsedPhysical, item.LastUsedCounter),
                    new(item.LastModifiedNode, item.LastModifiedPhysical, item.LastModifiedCounter),
                    (KeyValueState)item.State));
            }

            return true;
        }

        public LockRow? GetLock(string resource) => locks.Get(resource);

        public ReadOnlyKeyValueEntry? GetKeyValue(string keyName) => keyValues.Get(keyName);

        public ReadOnlyKeyValueEntry? GetKeyValueRevision(string keyName, long revision) =>
            keyValues.Get(keyName) is { } row && row.Revision == revision ? row : null;

        public ReadOnlyKeyValueEntry? GetKeyValueRevisionAtOrBefore(string keyName, long maxRevision, HLCTimestamp readTimestamp) =>
            keyValues.Get(keyName) is { } row && row.Revision <= maxRevision && row.LastModified.CompareTo(readTimestamp) <= 0 ? row : null;

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByPrefix(string prefixKeyName) =>
            [.. keyValues.All().Where(pair => pair.Key.StartsWith(prefixKeyName, StringComparison.Ordinal))];

        public List<(string, ReadOnlyKeyValueEntry)> GetKeyValueByRange(string prefix, string? startKey, int limit) =>
            [.. keyValues.All()
                .Where(pair => pair.Key.StartsWith(prefix, StringComparison.Ordinal)
                               && (startKey is null || string.CompareOrdinal(pair.Key, startKey) >= 0))
                .Take(limit)];

        public bool PruneKeyValueRevisions(
            IReadOnlyCollection<string>? keys, int retentionCount, TimeSpan retentionAge, int batchSize, HLCTimestamp floorTimestamp,
            out RevisionPruneResult result)
        {
            result = default;
            return true;
        }

        public CheckpointResult CreateCheckpoint(string destinationPath, long appliedIndex, HLCTimestamp appliedTime) =>
            throw new NotSupportedException("The test engine keeps no checkpoints.");
    }
}

/// <summary>The WAL the extension-assembly tests load as <c>--wal-storage test-extension</c>: Kommander's in-memory WAL.</summary>
public sealed class TestWalEngine : IWalProvider
{
    public string StorageType => TestStorageEngine.TypeName;

    public bool KeepsDataOnDisk => false;

    public IWAL Open(WalContext context) => new InMemoryWAL(context.Logger);
}

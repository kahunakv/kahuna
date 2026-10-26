
using Kahuna.Server.Configuration;
using Microsoft.Extensions.Logging;
#if !KAHUNA_THREAD_FREE
using Kommander.WAL;
#endif

namespace Kahuna.Server.Persistence.Providers;

/// <summary>What <see cref="IPersistenceBackendProvider.Open"/> opens a node's backend with.</summary>
public sealed class PersistenceBackendContext
{
    /// <summary>The storage path: the directory the backend keeps its files in.</summary>
    public required string StoragePath { get; init; }

    /// <summary>The storage revision, which separates the stores of successive formats under one path.</summary>
    public required string StorageRevision { get; init; }

    /// <summary>The node's configuration, for engine settings a provider reads.</summary>
    public required KahunaConfiguration Configuration { get; init; }

    /// <summary>The node's logger.</summary>
    public required ILogger Logger { get; init; }

    /// <summary>
    /// The memtable budget the store shares with the node's WAL, when <c>--storage-shared-memtable-budget-mb</c>
    /// is above 0; null otherwise. The same instance reaches the WAL provider. An engine that has no memtables
    /// ignores it.
    /// </summary>
    public SharedMemtableBudget? SharedMemtableBudget { get; init; }

#if !KAHUNA_THREAD_FREE
    /// <summary>
    /// The block cache and write-buffer budget that the RocksDB backend shares with a RocksDB WAL, when the
    /// node enables the shared budget and both are RocksDB; null otherwise. Other engines ignore it.
    /// </summary>
    public RocksDbSharedResources? SharedResources { get; init; }
#endif
}

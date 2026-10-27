
using Microsoft.Extensions.Logging;
using Kommander;
#if !KAHUNA_THREAD_FREE
using Kommander.WAL;
#endif

namespace Kahuna.Server.Persistence.Providers;

/// <summary>What <see cref="IWalProvider.Open"/> opens a node's WAL with.</summary>
public sealed class WalContext
{
    /// <summary>The WAL path: the directory the WAL keeps its files in.</summary>
    public required string Path { get; init; }

    /// <summary>The WAL revision, which separates the logs of successive formats under one path.</summary>
    public required string Revision { get; init; }

    /// <summary>True to make every write durable before it is acknowledged.</summary>
    public required bool SyncWrites { get; init; }

    /// <summary>The Raft logger.</summary>
    public required ILogger<IRaft> Logger { get; init; }

    /// <summary>
    /// The memtable budget the WAL shares with the node's store, when <c>--storage-shared-memtable-budget-mb</c>
    /// is above 0; null otherwise. The same instance reaches the backend provider. An engine that has no
    /// memtables ignores it.
    /// </summary>
    public SharedStorageMemory? SharedMemory { get; init; }

    /// <summary>
    /// The node's settings for its extensions, from <c>--extension-option key=value</c>
    /// (<see cref="Configuration.KahunaConfiguration.ExtensionOptions"/>); empty when none are given. An engine reads
    /// its own keys and refuses a value it cannot use.
    /// </summary>
    public IReadOnlyDictionary<string, string> ExtensionOptions { get; init; } = new Dictionary<string, string>(StringComparer.Ordinal);

#if !KAHUNA_THREAD_FREE
    /// <summary>
    /// The block cache and write-buffer budget that a RocksDB WAL shares with the RocksDB backend, when the
    /// node enables the shared budget and both are RocksDB; null otherwise. Other engines ignore it.
    /// </summary>
    public RocksDbSharedResources? SharedResources { get; init; }

    /// <summary>The column-family sizing of the RocksDB WAL. Other engines ignore it.</summary>
    public RocksDbWalTuning? RocksDbTuning { get; init; }
#endif
}

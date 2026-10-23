
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

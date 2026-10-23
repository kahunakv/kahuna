
using Kommander.WAL;

namespace Kahuna.Server.Persistence.Providers;

/// <summary>
/// A Raft write-ahead log engine, chosen by its <see cref="StorageType"/> name (the <c>--wal-storage</c>
/// option). Registered in <see cref="StorageProviders"/>; a host registers its own before the node starts.
/// </summary>
public interface IWalProvider
{
    /// <summary>The WAL storage type name this provider answers to, such as <c>"rocksdb"</c>. Compared ordinally.</summary>
    string StorageType { get; }

    /// <summary>
    /// True when the WAL keeps its data in files under the WAL path, which the node then creates.
    /// False for a WAL that holds everything in memory.
    /// </summary>
    bool KeepsDataOnDisk { get; }

    /// <summary>Opens the node's WAL.</summary>
    IWAL Open(WalContext context);
}

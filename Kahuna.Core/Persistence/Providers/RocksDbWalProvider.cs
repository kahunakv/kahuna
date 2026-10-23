#if !KAHUNA_THREAD_FREE

using Kommander.WAL;

namespace Kahuna.Server.Persistence.Providers;

/// <summary>Kommander's RocksDB WAL, <c>"rocksdb"</c>, with the node's column-family tuning and shared budget.</summary>
internal sealed class RocksDbWalProvider : IWalProvider
{
    public string StorageType => "rocksdb";

    public bool KeepsDataOnDisk => true;

    public IWAL Open(WalContext context) =>
        new RocksDbWAL(
            context.Path,
            context.Revision,
            context.Logger,
            syncWrites: context.SyncWrites,
            sharedResources: context.SharedResources,
            tuning: context.RocksDbTuning);
}
#endif

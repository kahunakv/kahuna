#if !KAHUNA_THREAD_FREE

using Kommander.WAL;

namespace Kahuna.Server.Persistence.Providers;

/// <summary>Kommander's SQLite WAL, <c>"sqlite"</c>.</summary>
internal sealed class SqliteWalProvider : IWalProvider
{
    public string StorageType => "sqlite";

    public bool KeepsDataOnDisk => true;

    public IWAL Open(WalContext context) =>
        new SqliteWAL(context.Path, context.Revision, context.Logger, syncWrites: context.SyncWrites);
}
#endif

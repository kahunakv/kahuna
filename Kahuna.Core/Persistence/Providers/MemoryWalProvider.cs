
using Kommander.WAL;

namespace Kahuna.Server.Persistence.Providers;

/// <summary>Kommander's in-memory WAL, <c>"memory"</c>.</summary>
internal sealed class MemoryWalProvider : IWalProvider
{
    public string StorageType => "memory";

    public bool KeepsDataOnDisk => false;

    public IWAL Open(WalContext context) => new InMemoryWAL(context.Logger);
}

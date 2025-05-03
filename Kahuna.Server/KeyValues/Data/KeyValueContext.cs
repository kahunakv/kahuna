
using Nixie;
using Kommander;
using Kahuna.Utils;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueContext
{
    public IActorRef<BackgroundWriterActor, BackgroundWriteRequest> BackgroundWriter { get; }
    
    public IRaft Raft  { get; }

    public IPersistenceBackend PersistenceBackend  { get; }

    public BTree<string, KeyValueEntry> Store  { get; }
    
    public Dictionary<string, KeyValueWriteIntent> LocksByPrefix  { get; }

    public KahunaConfiguration Configuration  { get; }

    public ILogger<IKahuna> Logger  { get; }
    
    public KeyValueContext(
        BTree<string, KeyValueEntry> store,
        Dictionary<string, KeyValueWriteIntent> locksByPrefix,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger)
    {
        Store = store;
        LocksByPrefix = locksByPrefix;
        BackgroundWriter = backgroundWriter;
        PersistenceBackend = persistenceBackend;
        Raft = raft;
        Configuration = configuration;
        Logger = logger;
    }
}
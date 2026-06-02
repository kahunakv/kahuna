
using Nixie;
using Kommander;
using Kahuna.Utils;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Shared.KeyValue;
using Nixie.Routers;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueContext
{
    private long approximateStoreBytes;

    public IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> ActorContext { get; }
    
    public IActorRef<BackgroundWriterActor, BackgroundWriteRequest> BackgroundWriter { get; }
    
    public IActorRef<BalancingActor<KeyValueProposalActor, KeyValueProposalRequest>, KeyValueProposalRequest> ProposalRouter { get; }
    
    public IRaft Raft  { get; }

    public IPersistenceBackend PersistenceBackend  { get; }

    public BTree<string, KeyValueEntry> Store  { get; }
    
    public Dictionary<string, KeyValueWriteIntent> LocksByPrefix  { get; }
    
    public Dictionary<int, KeyValueProposal> Proposals { get; }

    public KahunaConfiguration Configuration  { get; }

    public ILogger<IKahuna> Logger  { get; }

    public long ApproximateStoreBytes => approximateStoreBytes;

    public int CollectBatchMax =>
        Configuration.CollectBatchMax > 0
            ? Configuration.CollectBatchMax
            : Math.Max(Configuration.CacheEntriesToRemove, 1);
    
    public KeyValueContext(
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext, 
        BTree<string, KeyValueEntry> store,
        Dictionary<string, KeyValueWriteIntent> locksByPrefix,
        Dictionary<int, KeyValueProposal> proposals,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IActorRef<BalancingActor<KeyValueProposalActor, KeyValueProposalRequest>, KeyValueProposalRequest> proposalRouter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        ActorContext = actorContext;
        Store = store;
        LocksByPrefix = locksByPrefix;
        Proposals = proposals;
        BackgroundWriter = backgroundWriter;
        ProposalRouter = proposalRouter;
        PersistenceBackend = persistenceBackend;
        Raft = raft;
        Configuration = configuration;
        Logger = logger;
    }

    public bool IsOverStoreBudget()
    {
        return Store.Count > Configuration.MaxEntriesPerActor
            || approximateStoreBytes > Configuration.MaxBytesPerActor;
    }

    public void InsertStoreEntry(string key, KeyValueEntry entry)
    {
        if (Store.TryGetValue(key, out KeyValueEntry? existing))
            approximateStoreBytes -= KeyValueStoreAccounting.EstimateEntryBytes(key, existing);

        Store.Insert(key, entry);
        approximateStoreBytes += KeyValueStoreAccounting.EstimateEntryBytes(key, entry);
    }

    public bool RemoveStoreEntry(string key)
    {
        if (!Store.TryGetValue(key, out KeyValueEntry? entry))
            return false;

        Store.Remove(key);
        approximateStoreBytes -= KeyValueStoreAccounting.EstimateEntryBytes(key, entry);
        return true;
    }

    public void AdjustEntryValueBytes(int previousValueLength, int newValueLength)
    {
        approximateStoreBytes += newValueLength - previousValueLength;
    }

    public void AdjustEstimatedEntryBytes(long delta)
    {
        approximateStoreBytes += delta;
    }

    public void ScheduleFollowUpCollect()
    {
        if (ActorContext is null)
            return;

        ActorContext.Self.Send(new KeyValueRequest(KeyValueRequestType.Collect));
    }
}

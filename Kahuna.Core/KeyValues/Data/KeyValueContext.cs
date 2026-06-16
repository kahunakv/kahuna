
using Nixie;
using Kommander;
using Kahuna.Utils;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
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

    public KeySpaceRegistry KeySpaceRegistry { get; }

    public RangeMapStore RangeMapStore { get; }

    public IPersistenceBackend PersistenceBackend  { get; }

    public BTree<string, KeyValueEntry> Store  { get; }
    
    public Dictionary<string, KeyValueWriteIntent> LocksByPrefix  { get; }

    public Dictionary<string, List<KeyValueRangeLock>> LocksByRange { get; }

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
        Dictionary<string, List<KeyValueRangeLock>> locksByRange,
        Dictionary<int, KeyValueProposal> proposals,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IActorRef<BalancingActor<KeyValueProposalActor, KeyValueProposalRequest>, KeyValueProposalRequest> proposalRouter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KeySpaceRegistry keySpaceRegistry,
        RangeMapStore rangeMapStore,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        ActorContext = actorContext;
        Store = store;
        LocksByPrefix = locksByPrefix;
        LocksByRange = locksByRange;
        Proposals = proposals;
        BackgroundWriter = backgroundWriter;
        ProposalRouter = proposalRouter;
        PersistenceBackend = persistenceBackend;
        Raft = raft;
        KeySpaceRegistry = keySpaceRegistry;
        RangeMapStore = rangeMapStore;
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
        {
#if DEBUG
            System.Diagnostics.Debug.Assert(
                existing.CachedBytes == KeyValueStoreAccounting.EstimateEntryBytes(key, existing) - (key.Length * sizeof(char)),
                $"CachedBytes drift on replace: key={key}, cached={existing.CachedBytes}, computed={KeyValueStoreAccounting.EstimateEntryBytes(key, existing) - (key.Length * sizeof(char))}");
#endif
            approximateStoreBytes -= (key.Length * sizeof(char)) + existing.CachedBytes;
        }

        // Initialize CachedBytes the first time an entry enters the store. For new entries
        // (no Revisions, no MvccEntries) EstimateEntryBytes is O(1); subsequent mutations
        // maintain CachedBytes incrementally via AdjustEstimatedEntryBytes / AdjustEntryValueBytes.
        if (entry.CachedBytes == 0)
            entry.CachedBytes = KeyValueStoreAccounting.EstimateEntryBytes(key, entry) - (key.Length * sizeof(char));

        Store.Insert(key, entry);
        approximateStoreBytes += (key.Length * sizeof(char)) + entry.CachedBytes;
    }

    public bool RemoveStoreEntry(string key)
    {
        if (!Store.TryGetValue(key, out KeyValueEntry? entry))
            return false;

        Store.Remove(key);
        approximateStoreBytes -= (key.Length * sizeof(char)) + entry.CachedBytes;
        return true;
    }

    public void AdjustEntryValueBytes(KeyValueEntry entry, int previousValueLength, int newValueLength)
    {
        int delta = newValueLength - previousValueLength;
        approximateStoreBytes += delta;
        entry.CachedBytes += delta;
    }

    public void AdjustEstimatedEntryBytes(KeyValueEntry entry, long delta)
    {
        approximateStoreBytes += delta;
        entry.CachedBytes += delta;
    }

    public void ScheduleFollowUpCollect()
    {
        if (ActorContext is null)
            return;

        ActorContext.Self.Send(new KeyValueRequest(KeyValueRequestType.Collect));
    }
}

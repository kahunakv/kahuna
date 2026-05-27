
using Nixie;
using Kommander;
using Kahuna.Utils;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Nixie.Routers;

namespace Kahuna.Server.KeyValues;

internal sealed class KeyValueContext
{
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
        ILogger<IKahuna> logger)
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
}
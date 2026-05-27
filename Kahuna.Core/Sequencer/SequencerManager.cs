
using Nixie;
using Kommander;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;

namespace Kahuna.Server.Sequencer;

internal sealed class SequencerManager
{
    private readonly ActorSystem actorSystem;

    private readonly IRaft raft;

    private readonly IPersistenceBackend persistenceBackend;
    
    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly ILogger<IKahuna> logger;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorSystem"></param>
    /// <param name="raft"></param>
    /// <param name="persistenceBackend"></param>
    /// <param name="backgroundWriter"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public SequencerManager(
        ActorSystem actorSystem, 
        IRaft raft, 
        IInterNodeCommunication interNodeCommunication,
        IPersistenceBackend persistenceBackend, 
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        KahunaConfiguration configuration, 
        ILogger<IKahuna> logger
    )
    {
        this.actorSystem = actorSystem;
        this.raft = raft;
        this.backgroundWriter = backgroundWriter;
        this.logger = logger;

        this.persistenceBackend = persistenceBackend;
        
        //ephemeralKeyValuesRouter = GetEphemeralRouter(configuration);
        //persistentKeyValuesRouter = GetConsistentRouter(configuration);

        //txCoordinator = new(this, configuration, raft, logger);
        //locator = new(this, configuration, raft, interNodeCommunication, logger);

        //restorer = new(backgroundWriter, raft, logger);
        //replicator = new(backgroundWriter, raft, logger);
    }

    public Task CreateSequence()
    {
        return Task.CompletedTask;
    }
}
using Nixie;
using Nixie.Routers;

using Kommander;

using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Owns the two consistent-hash rings of key-value actors — ephemeral and persistent — and the single
/// entry point every caller uses to reach them. Spawning the workers, the bounded-inbox policy that keeps
/// a hot-key flood from stranding control messages, and the backpressure-to-<c>MustRetry</c> mapping all
/// live here so the execution paths above only pick a ring and send.
/// </summary>
internal sealed class KeyValueActorRouters
{
    private readonly KeyValuesRuntime runtime;

    private readonly ActorSystem actorSystem;

    private readonly ILogger<IKahuna> logger;

    private readonly IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> ephemeralKeyValuesRouter;

    private readonly IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> persistentKeyValuesRouter;

    private readonly List<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> ephemeralInstances = [];

    private readonly List<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> persistentInstances = [];

    /// <summary>The ephemeral (in-memory only) actor ring.</summary>
    internal IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> Ephemeral => ephemeralKeyValuesRouter;

    /// <summary>The persistent (Raft-replicated, disk-backed) actor ring.</summary>
    internal IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> Persistent => persistentKeyValuesRouter;

    internal IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> EphemeralInstances => ephemeralInstances;

    internal IReadOnlyList<IActorRef<KeyValueActor, KeyValueRequest, KeyValueResponse>> PersistentInstances => persistentInstances;

    internal KeyValueActorRouters(KeyValuesRuntime runtime)
    {
        this.runtime = runtime;
        this.actorSystem = runtime.ActorSystem;
        this.logger = runtime.Logger;

        ephemeralKeyValuesRouter = GetEphemeralRouter(runtime.Configuration);
        persistentKeyValuesRouter = GetConsistentRouter(runtime.Configuration);
    }

    // The spawn helpers below read their collaborators off the runtime under the same names the actor
    // constructor arguments use, so the spawn argument lists stay exactly as they were.
    private IRaft raft => runtime.Raft;

    private IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter => runtime.BackgroundWriter;

    private Writes.PartitionWriteAggregator writeAggregator => runtime.WriteAggregator;

    private Kahuna.Server.Persistence.Backend.IPersistenceBackend persistenceBackend => runtime.PersistenceBackend;

    private Kommander.WAL.IO.IRaftReadScheduler backendReadScheduler => runtime.BackendReadScheduler;

    private Ranges.KeySpaceRegistry keySpaceRegistry => runtime.KeySpaceRegistry;

    private Ranges.RangeMapStore rangeMapStore => runtime.RangeMapStore;

    private Ranges.SnapshotFloorStore snapshotFloorStore => runtime.SnapshotFloorStore;

    private CompletionReceiptStore completionReceiptStore => runtime.CompletionReceiptStore;

    private PreparedIntentStore preparedIntentStore => runtime.PreparedIntentStore;

    private TransactionRecordStore transactionRecordStore => runtime.TransactionRecordStore;

    /// <summary>
    /// Builds the bounded-inbox + priority-control options for a key-value actor. The bound applies
    /// only to ordinary user requests; control messages (<see cref="IsControlRequest"/>) are exempt and
    /// delivered ahead of the backlog so a hot-key flood never strands an in-flight completion.
    /// </summary>
    private static ActorRunnerOptions<KeyValueRequest> BuildActorInboxOptions(KahunaConfiguration configuration) => new()
    {
        MaxInboxSize = configuration.MaxKeyValueActorInboxSize > 0 ? configuration.MaxKeyValueActorInboxSize : null,
        IsControlMessage = IsControlRequest
    };

    /// <summary>
    /// Classifies a request as a priority control message, exempt from the inbox bound. Beyond the
    /// caller-promise-carrying completions (<c>ResumeRead</c>, <c>CompleteProposal</c>,
    /// <c>ReleaseProposal</c>) — which would strand in-flight work if rejected — this also exempts the
    /// internal fire-and-forget/maintenance messages that must never be silently dropped under
    /// backpressure: <c>InvalidateOrApply</c> (follower cache coherence — dropping it yields a stale
    /// read), <c>FlushAck</c> (durability marker), and the periodic <c>Collect</c> / snapshot
    /// <c>GetSafeTimestamp</c>. Everything else is an external user op and is subject to the bound.
    /// </summary>
    internal static bool IsControlRequest(KeyValueRequest request) =>
        request.Type is KeyValueRequestType.ResumeRead
            or KeyValueRequestType.CompleteProposal
            or KeyValueRequestType.ReleaseProposal
            or KeyValueRequestType.InvalidateOrApply
            or KeyValueRequestType.FlushAck
            or KeyValueRequestType.Collect
            or KeyValueRequestType.GetSafeTimestamp;

    /// <summary>
    /// Sends a request to a bounded key-value actor router, mapping the actor's inbox-full backpressure
    /// (<see cref="ActorBusyException"/>, thrown when a hot actor is saturated with ordinary requests)
    /// to a retryable <c>MustRetry</c> response. Control messages are exempt from the bound and never
    /// reach this path. The message was never enqueued, so retrying is unconditionally safe.
    /// </summary>
    internal static async Task<KeyValueResponse?> AskKeyValueActor(
        IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> router,
        KeyValueRequest request)
    {
        try
        {
            return await router.Ask(request);
        }
        catch (ActorBusyException)
        {
            return new KeyValueResponse(KeyValueResponseType.MustRetry);
        }
    }

    private IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> GetEphemeralRouter(
        KahunaConfiguration configuration
    )
    {
        logger.LogStartingEphemeralWorkers(configuration.KeyValueWorkers);

        ActorRunnerOptions<KeyValueRequest> options = BuildActorInboxOptions(configuration);

        // Ephemeral actors never participate in durable-intent 2PC (a durable transaction cannot modify an ephemeral
        // key), so they must not consult the persistent prepared-intent / transaction-record stores. Sharing the
        // persistent stores let an ephemeral write to a key whose *persistent* namesake held a committed-but-unsettled
        // intent materialize that foreign intent into the ephemeral entry and over-derive its revision — a
        // cross-keyspace contamination visible only while deferred settlement left the intent lingering.
        //
        // Give them dedicated, empty, disk-free stores instead of null: durable operations only ever route to the
        // persistent actors, so these stay empty for the node's lifetime, but they must be non-null because the
        // server resolves the actor constructor through DI (ActivatorUtilities), which cannot infer a parameter's
        // type from a null argument — passing null there fails constructor selection (it works only under the
        // provider-less Activator path the in-process tests use).
        Transactions.PreparedIntentStore ephemeralPreparedIntentStore = new();
        Transactions.TransactionRecordStore ephemeralTransactionRecordStore = new();

        for (int i = 0; i < configuration.KeyValueWorkers; i++)
            ephemeralInstances.Add(actorSystem.SpawnWithOptions<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                "ephemeral-keyvalue-" + i,
                options,
                backgroundWriter,
                writeAggregator,
                persistenceBackend,
                raft,
                backendReadScheduler,
                keySpaceRegistry,
                rangeMapStore,
                configuration,
                logger,
                snapshotFloorStore,
                completionReceiptStore,
                ephemeralPreparedIntentStore,
                ephemeralTransactionRecordStore
            ));

        return actorSystem.CreateConsistentHashRouter(ephemeralInstances);
    }

    /// <summary>
    /// Creates the consistent key/values router
    /// </summary>
    /// <param name="backgroundWriter"></param>
    /// <param name="persistence"></param>
    /// <param name="workers"></param>
    /// <returns></returns>
    private IActorRef<ConsistentHashActor<KeyValueActor, KeyValueRequest, KeyValueResponse>, KeyValueRequest, KeyValueResponse> GetConsistentRouter(
        KahunaConfiguration configuration
    )
    {
        logger.LogStartingPersistentWorkers(configuration.KeyValueWorkers);

        ActorRunnerOptions<KeyValueRequest> options = BuildActorInboxOptions(configuration);

        for (int i = 0; i < configuration.KeyValueWorkers; i++)
            persistentInstances.Add(actorSystem.SpawnWithOptions<KeyValueActor, KeyValueRequest, KeyValueResponse>(
                "persistent-keyvalue-" + i,
                options,
                backgroundWriter,
                writeAggregator,
                persistenceBackend,
                raft,
                backendReadScheduler,
                keySpaceRegistry,
                rangeMapStore,
                configuration,
                logger,
                snapshotFloorStore,
                completionReceiptStore,
                preparedIntentStore,
                transactionRecordStore
            ));

        return actorSystem.CreateConsistentHashRouter(persistentInstances);
    }
}

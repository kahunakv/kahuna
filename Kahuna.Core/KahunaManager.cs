
using Nixie;
using Kommander;
using Kommander.Time;
using Kommander.WAL;
using Kommander.WAL.IO;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Locks;
using Kahuna.Server.Persistence;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Server.Persistence.Pitr;
using Kahuna.Server.Sequencer;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.Composition;
using Writes = Kahuna.Server.KeyValues.Writes;

namespace Kahuna;

/// <summary>
/// Façade to the internal systems of Kahuna.
/// </summary>
public sealed partial class KahunaManager : IKahuna, IDisposable
{
    /// <summary>
    /// Manages distributed locking mechanisms within the KahunaManager.
    /// Provides functionality for acquiring, extending, and releasing locks
    /// in a distributed system, ensuring proper synchronization and consistency.
    /// Acts as the primary interface for lock-related operations, delegating the
    /// underlying execution to the LockManager class.
    /// </summary>
    private readonly LockManager locks;

    /// <summary>
    /// Manages key-value storage operations in KahunaManager, including the setting,
    /// retrieval, and existence checks of key-value pairs. Provides methods to handle
    /// concurrency, durability, and transactional requirements for key-value interactions.
    /// Acts as a centralized component for key-value operations within the Kahuna system.
    /// </summary>
    private readonly KeyValuesManager keyValues;

    private readonly SequencerManager sequencer;

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    /// <summary>
    /// Watches the committed placement map: owns the per-node placement projection, tears down the
    /// per-partition background state of ranges this node stops replicating, and re-derives the
    /// startup purge on the first committed map application.
    /// </summary>
    private readonly PartitionPlacementCoordinator placement;

    /// <summary>
    /// Kahuna's <see cref="IApplicationDurabilityProvider"/>. Hosts assign it to
    /// <c>RaftConfiguration.ApplicationDurabilityProvider</c> after constructing the manager and
    /// before the Raft node joins, so restart replay and WAL compaction respect this node's
    /// application-durability floor. Kommander reads the configuration property lazily, so
    /// post-construction assignment is safe.
    /// </summary>
    public IApplicationDurabilityProvider DurabilityProvider { get; }

    /// <summary>
    /// Kahuna-owned scheduler for all persistence-backend reads (point gets, scans, read-before-write),
    /// separate from Kommander's WAL read pool so data-plane reads never contend with consensus WAL reads.
    /// </summary>
    private readonly FairReadScheduler backendReadScheduler;

    /// <summary>
    /// Kahuna-owned scheduler dedicated to background batch writes (StoreKeyValues / StoreLocks / revision
    /// pruning). A separate instance keeps fsync-heavy flushes off both the WAL read pool and the backend
    /// read pool. Owned here and stopped/disposed in <see cref="Dispose"/> after the actor system drains.
    /// </summary>
    private readonly FairReadScheduler backendWriteScheduler;

    private readonly IPersistenceBackend persistenceBackend;

    /// <summary>
    /// The node's backup/PITR entry point. Always present: a node with no backup directory
    /// configured gets a disabled instance that refuses every operation.
    /// </summary>
    private readonly BackupFacade backups;

    private readonly bool remoteRestoreAllowed;


    /// <summary>
    /// Constructor
    /// </summary>
    public KahunaManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, IInterNodeCommunication interNodeCommunication, ILogger<IKahuna> logger, ILogger<IRaft>? raftLogger = null)
        : this(actorSystem, raft, configuration, interNodeCommunication, KahunaNodeComposer.CreateBackend(configuration, logger, null), logger, raftLogger)
    {
    }

    /// <summary>
    /// Constructor variant that shares a RocksDB memory bundle (block cache + WriteBufferManager) with the
    /// Raft WAL. The composition root creates one <paramref name="sharedResources"/> and injects the same
    /// instance here and into the WAL so both RocksDB databases draw from a single unified budget. The
    /// bundle is <b>borrowed</b>: this manager (and the backend it creates) never dispose it — the
    /// composition root disposes it after both databases are closed. A null bundle behaves exactly as the
    /// primary constructor.
    /// </summary>
    public KahunaManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, IInterNodeCommunication interNodeCommunication, RocksDbSharedResources? sharedResources, ILogger<IKahuna> logger, ILogger<IRaft>? raftLogger = null)
        : this(actorSystem, raft, configuration, interNodeCommunication, KahunaNodeComposer.CreateBackend(configuration, logger, sharedResources), logger, raftLogger)
    {
    }

    /// <summary>
    /// Test-only variant that injects a per-node decorator over the key/value write aggregator's Raft batch
    /// executor, so a test can count/gate/force the aggregator's batch calls while driving the real public
    /// write entry points. Scoped to this node — never a process-wide static — so a concurrent test cannot be
    /// accidentally wrapped. A null decorator behaves exactly as the public constructor.
    /// </summary>
    internal KahunaManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, IInterNodeCommunication interNodeCommunication, RocksDbSharedResources? sharedResources, ILogger<IKahuna> logger, ILogger<IRaft> raftLogger, Func<Writes.IPartitionBatchExecutor, Writes.IPartitionBatchExecutor>? writeBatchExecutorDecorator)
        : this(actorSystem, raft, configuration, interNodeCommunication, KahunaNodeComposer.CreateBackend(configuration, logger, sharedResources), logger, raftLogger, writeBatchExecutorDecorator)
    {
    }

    /// <summary>
    /// Constructor variant used by PITR bootstrap: accepts a pre-seeded <paramref name="preSeededBackend"/>
    /// instead of creating a fresh one from configuration.  The WAL for the Raft layer is also
    /// pre-seeded externally; only the persistence-backend injection is handled here.
    /// </summary>
    internal KahunaManager(ActorSystem actorSystem, IRaft raft, KahunaConfiguration configuration, IInterNodeCommunication interNodeCommunication, IPersistenceBackend preSeededBackend, ILogger<IKahuna> logger, ILogger<IRaft>? raftLogger = null, Func<Writes.IPartitionBatchExecutor, Writes.IPartitionBatchExecutor>? writeBatchExecutorDecorator = null)
    {
        // The whole object graph is assembled by the composer; the constructor's own job is to take
        // ownership of the parts, subscribe to the placement map, and start the I/O schedulers last.
        KahunaNodeComponents components = KahunaNodeComposer.Build(
            actorSystem, raft, configuration, interNodeCommunication, preSeededBackend, logger, raftLogger, writeBatchExecutorDecorator);

        persistenceBackend = components.PersistenceBackend;
        backendReadScheduler = components.BackendReadScheduler;
        backendWriteScheduler = components.BackendWriteScheduler;
        backgroundWriter = components.BackgroundWriter;
        locks = components.Locks;
        keyValues = components.KeyValues;
        sequencer = components.Sequencer;
        DurabilityProvider = components.DurabilityProvider;

        // Starts watching the committed placement map for replica gains and losses; it subscribes
        // during construction and releases both subscriptions in Dispose.
        placement = new PartitionPlacementCoordinator(
            raft, keyValues, locks, components.DurabilityProvider, backgroundWriter, logger);

        // The node's backup/PITR wiring (storage target, flush barrier, MVCC snapshot holds,
        // retention policy, GC reaper) lives in the facade; a node without a backup directory gets
        // a disabled one so every caller sees the same refusal.
        backups = string.IsNullOrWhiteSpace(configuration.BackupDir)
            ? BackupFacade.Disabled()
            : BackupFacade.Create(
                actorSystem,
                raft,
                persistenceBackend,
                configuration,
                keyValues,
                FlushPersistenceAsync,
                // Applied-index barrier probe: the writer's max-enqueued commit HLC per partition.
                // The backup blocks on it, so a probe stuck at zero fails the backup closed.
                partitionId => BackgroundWriterActor?.GetMaxEnqueuedHlc(partitionId) ?? HLCTimestamp.Zero,
                logger);

        // Restore is administrative: allow it over the network only when a server-owned restore root
        // confines destinations, or an explicit unconfined opt-in is set.
        remoteRestoreAllowed = !string.IsNullOrWhiteSpace(configuration.RestoreRoot)
            || configuration.AllowUnconfinedRemoteRestore;

        // Construction succeeded — start the backend I/O worker threads last, so a failure above never
        // leaks scheduler threads.
        backendReadScheduler.Start();
        backendWriteScheduler.Start();
    }

    private int disposed;

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        // Idempotent: the composition roots dispose explicitly in a controlled order (after draining the
        // actor system), and the DI container also disposes this singleton at teardown. Only the first wins.
        if (Interlocked.Exchange(ref disposed, 1) != 0)
            return;

        placement.Dispose();

        keyValues.Dispose();

        backups.Dispose();

        // Stop the backend schedulers before closing the backend they call into. Stop() drains every
        // operation accepted before this point (or faults it) and joins the worker threads, so no
        // in-flight read/write touches the backend after it is disposed. This runs after the actor
        // system has drained (embedded/server shutdown order), so actors are no longer enqueuing work.
        backendReadScheduler.Stop();
        backendWriteScheduler.Stop();
        backendReadScheduler.Dispose();
        backendWriteScheduler.Dispose();

        if (persistenceBackend is IDisposable disposable)
            disposable.Dispose();
    }
    
    /// <summary>
    /// Flushes all pending dirty objects to the persistence backend and waits for completion.
    /// Use this before closing/disposing to ensure all queued writes land in storage.
    /// </summary>
    public Task FlushPersistenceAsync()
    {
        TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        backgroundWriter.Send(new(BackgroundWriteType.FlushAndNotify, tcs));
        return tcs.Task;
    }

}

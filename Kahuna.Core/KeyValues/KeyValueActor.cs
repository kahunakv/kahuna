
using Nixie;
using Kommander;
using System.Diagnostics;
using Google.Protobuf;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues.Ranges;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Utils;
using Nixie.Routers;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Each of these actors functions as a worker, accepting requests to perform operations on key/value pairs.
/// The actor maintains an in-memory cache and if a key is not found, it attempts to retrieve it from disk.
/// Operations with Linearizable consistency persist all modifications to disk.
/// </summary>
internal sealed class KeyValueActor : IActor<KeyValueRequest, KeyValueResponse>
{
    /// <summary>
    /// Represents the threshold for triggering a collection operation in the actor.
    /// Once the specified number of operations has been reached, a collection process
    /// is initiated to manage resources, such as potentially clearing cached or temporary data.
    /// </summary>
    private const int CollectThreshold = 500;

    /// <summary>
    /// Provides the execution context for the KeyValueActor, enabling interaction
    /// with the underlying actor framework. This context facilitates message handling,
    /// scheduling, and coordination for the actor's operations.
    /// </summary>
    private readonly IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext;

    /// <summary>
    /// Stores and manages key-value pairs in memory with efficient retrieval and modification operations.
    /// Utilizes a B-tree data structure to provide scalable performance for large datasets. This
    /// in-memory store is used as a primary cache, with further data coordination to disk
    /// or external systems when necessary. All operations leverage the underlying B-tree
    /// to maintain ordered keys and ensure quick lookups, inserts, updates, and deletions.
    /// </summary>
    private readonly BTree<string, KeyValueEntry> keyValuesStore = new(32);
    
    /// <summary>
    /// Stores write intents for key prefixes. This ensure consistent reads for operations like
    /// "get by bucket". If a write intent is present, only the transaction holding the write intent
    /// can add/modify/delete the keys in the prefix.
    /// </summary>
    private readonly Dictionary<string, KeyValueWriteIntent> locksByPrefix = new();

    /// <summary>
    /// Stores range locks per prefix. Each entry is a list because multiple transactions
    /// may hold non-overlapping range locks under the same prefix simultaneously.
    /// </summary>
    private readonly Dictionary<string, List<KeyValueRangeLock>> locksByRange = new();

    /// <summary>
    ///
    /// </summary>
    private readonly Dictionary<int, KeyValueProposal> proposals = new();

    /// <summary>
    /// Provides logging capabilities for the KeyValueActor class. This logger is used to
    /// record diagnostic information, errors, and other relevant events related to the
    /// operation and behavior of the actor.
    /// </summary>
    private readonly ILogger<IKahuna> logger;

    /// <summary>
    /// Represents the count of remaining operations before a specific action, such as a resource collection, is triggered.
    /// This variable is decremented with each completed operation and resets once the threshold is reached,
    /// initiating the corresponding action to maintain efficient resource management or consistency.
    /// </summary>
    private int operations = CollectThreshold;

    /// <summary>
    /// Represents the handler responsible for processing "set" operations on key-value pairs.
    /// This handler ensures that key-value pairs are stored into the data structures,
    /// adhering to the consistency model and persisting the changes as required for durability.
    /// </summary>
    private readonly TrySetHandler trySetHandler;

    /// <summary>
    /// Handles the "extend" operation for key/value pairs in the actor system.
    /// This field initializes a handler responsible for extending the lease or lifetime
    /// of a key/value pair, ensuring the operation's logic and persistence are appropriately managed.
    /// </summary>
    private readonly TryExtendHandler tryExtendHandler;

    /// <summary>
    /// Responsible for handling delete operations on key/value pairs within the actor.
    /// This handler processes delete requests and interacts with the in-memory cache, disk storage,
    /// and other subsystems to ensure consistent and reliable deletion of data, adhering to the actor's requirements.
    /// </summary>
    private readonly TryDeleteHandler tryDeleteHandler;

    /// <summary>
    /// Responsible for handling key retrieval operations within the actor.
    /// This handler attempts to fetch the value associated with a specified key, leveraging the in-memory cache if possible.
    /// If the key is not available in memory, the handler interacts with the persistence backend to retrieve the data.
    /// Ensures consistency and reliability of key retrieval processes in distributed environments.
    /// </summary>
    private readonly TryGetHandler tryGetHandler;

    /// <summary>
    /// Handles operations for retrieving key/value pairs that match a specified prefix.
    /// This handler processes requests to extract subsets of stored entries efficiently
    /// and in a consistent manner.
    /// </summary>
    private readonly TryGetByBucketHandler tryGetByBucketHandler;

    /// <summary>
    /// Handles paginated range scans over key-value entries, merging in-memory and
    /// disk-backed results with MVCC/RYOW semantics and a cursor for resumption.
    /// </summary>
    private readonly TryGetByRangeHandler tryGetByRangeHandler;

    /// <summary>
    /// Handles operations for scanning keys in the key-value store with a specified prefix.
    /// This handler is responsible for efficiently retrieving all key-value pairs
    /// that match a given prefix. This operation returns entries cached in-memory that
    /// represents committed transactions, ensuring consistency and reliability.
    /// </summary>
    private readonly TryScanByPrefixHandler tryScanByPrefixHandler;
    
    /// <summary>
    /// Handles operations for scanning keys in the key-value store with a specified prefix.
    /// This handler is responsible for efficiently retrieving all key-value pairs
    /// that match a given prefix from disk-based storage when necessary.    
    /// </summary>
    private readonly TryScanByPrefixFromDiskHandler tryScanByPrefixFromDiskHandler;

    /// <summary>
    /// Handles requests to verify the existence of a specific key within the key-value store.
    /// It operates by checking both the in-memory cache and the persistent backend storage
    /// to determine if a given key exists. This handler is utilized for operations requiring
    /// a consistent check for key presence while integrating with the system's persistence
    /// and consistency mechanisms.
    /// </summary>
    private readonly TryExistsHandler tryExistsHandler;

    /// <summary>
    /// Handles
    /// </summary>
    private readonly TryCheckWriteIntentHandler tryCheckWriteIntentHandler;

    /// <summary>
    /// Manages the operation to attempt acquiring an exclusive lock on a key
    /// within the key-value store. This handler ensures that only one client
    /// can hold an exclusive lock on a particular key at any given time,
    /// enforcing mutual exclusion for operations that require this level
    /// of access control.
    /// </summary>
    private readonly TryAcquireExclusiveLockHandler tryAcquireExclusiveLockHandler;
    
    /// <summary>
    /// Manages the operation to attempt acquiring an exclusive lock on a group of keys
    /// by prefix within the key-value store. This handler ensures that only one client
    /// can hold an exclusive lock on a particular prefix at any given time,
    /// enforcing mutual exclusion for operations that require this level
    /// of access control. Since the prefix is locked, no new keys can be added on this prefix.
    /// </summary>
    private readonly TryAcquireExclusivePrefixLockHandler tryAcquireExclusivePrefixLockHandler;

    /// <summary>
    /// Handles the operation of releasing an exclusive lock within the key-value actor system.
    /// This is employed when an exclusive lock, previously acquired on a key, must be relinquished to allow
    /// other operations or actors to access the key.
    /// </summary>
    private readonly TryReleaseExclusiveLockHandler tryReleaseExclusiveLockHandler;

    /// <summary>
    /// 
    /// </summary>
    private readonly TryReleaseExclusivePrefixLockHandler tryReleaseExclusivePrefixLockHandler;

    private readonly TryAcquireExclusiveRangeLockHandler tryAcquireExclusiveRangeLockHandler;

    private readonly TryReleaseExclusiveRangeLockHandler tryReleaseExclusiveRangeLockHandler;

    private readonly GetRangeLocksHandler getRangeLocksHandler;

    private readonly ImportRangeLocksHandler importRangeLocksHandler;

    private readonly GetSafeTimestampHandler getSafeTimestampHandler;

    /// <summary>
    /// Handles the preparation phase for mutations in the key/value store in the Two-Phase-Commit (2PC) protocol.
    /// This component ensures that any necessary preconditions, validations and conflict resolution
    /// for mutations are met before executing them, contributing to data integrity and consistency within the system.
    /// </summary>
    private readonly TryPrepareMutationsHandler tryPrepareMutationsHandler;

    /// <summary>
    /// Responsible for handling the commit of mutation operations in the Two-Phase-Commit (2PC) protocol.
    /// This handler ensures that all specified mutations are processed and stored correctly,
    /// maintaining the consistency and durability of key/value pairs. It is typically invoked
    /// when a transaction or batch of operations needs to be finalized and persisted.
    /// </summary>
    private readonly TryCommitMutationsHandler tryCommitMutationsHandler;

    /// <summary>
    /// Handles the operation of rolling back a set of mutations in the key-value store.
    /// Responsible for ensuring that any changes made during a transactional scope
    /// are reverted in cases where the transaction cannot be successfully completed.
    /// </summary>
    private readonly TryRollbackMutationsHandler tryRollbackMutationsHandler;

    /// <summary>
    /// Unwinds a write intent staged for the partition-batched prepare whose batch never proposed (no-Raft
    /// local rollback), so a partition batch that fails staging cannot leak pinned intents.
    /// </summary>
    private readonly ApplyRolledBackMutationsHandler applyRolledBackMutationsHandler;

    /// <summary>
    /// Handles the process of collecting and managing key/value resources within the actor.
    /// This handler is responsible for triggering cleanup or optimization tasks, such as
    /// consolidating cached data or freeing unneeded resources to maintain efficient operation.
    /// </summary>
    private readonly TryCollectHandler tryCollectHandler;
    
    /// <summary>
    /// 
    /// </summary>
    private readonly CompleteProposalHandler completeProposalHandler;
    
    /// <summary>
    ///
    /// </summary>
    private readonly ReleaseProposalHandler releaseProposalHandler;

    /// <summary>
    ///
    /// </summary>
    private readonly ResumeReadHandler resumeReadHandler;

    /// <summary>
    /// Resolves the caller's promise from an off-mailbox two-phase-commit Raft outcome delivered by
    /// <see cref="KeyValuePhaseTwoActor"/>.
    /// </summary>
    private readonly CompletePhaseTwoHandler completePhaseTwoHandler;

    /// <summary>
    /// Applies a committed Raft log entry to a resident cache entry so a follower (or a newly
    /// promoted leader) never serves a stale revision from memory.
    /// </summary>
    private readonly InvalidateOrApplyHandler invalidateOrApplyHandler;

    /// <summary>
    /// A high-resolution timer used to measure the time elapsed during the handling of requests within the actor.
    /// The stopwatch is utilized to record and log the duration of operations, aiding in performance monitoring
    /// and diagnostics for the KeyValueActor.
    /// </summary>
    private readonly Stopwatch stopwatch = Stopwatch.StartNew();

    private readonly KeyValueContext? kvContext;

    internal long ApproximateStoreBytes => kvContext?.ApproximateStoreBytes ?? 0L;

    internal int PendingReadsCount => kvContext?.PendingReads.Count ?? 0;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorContext"></param>
    /// <param name="backgroundWriter"></param>
    /// <param name="proposalRouter"></param>
    /// <param name="persistenceBackend"></param>
    /// <param name="raft"></param>
    /// <param name="configuration"></param>
    /// <param name="logger"></param>
    public KeyValueActor(
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        Writes.PartitionWriteAggregator writeAggregator,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KeySpaceRegistry keySpaceRegistry,
        RangeMapStore rangeMapStore,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    ) : this(actorContext, backgroundWriter, writeAggregator, null, persistenceBackend, raft,
             keySpaceRegistry, rangeMapStore, configuration, logger, null, null, null)
    {
    }

    public KeyValueActor(
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        Writes.PartitionWriteAggregator writeAggregator,
        IActorRef<BalancingActor<KeyValuePhaseTwoActor, KeyValuePhaseTwoRequest>, KeyValuePhaseTwoRequest>? phaseTwoRouter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KeySpaceRegistry keySpaceRegistry,
        RangeMapStore rangeMapStore,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger,
        SnapshotFloorStore? snapshotFloorStore,
        CompletionReceiptStore? completionReceiptStore,
        Transactions.CoordinatorDecisionStore? coordinatorDecisionStore
    )
    {
        this.actorContext = actorContext;
        this.logger = logger;

        kvContext = new(
            actorContext,
            keyValuesStore,
            locksByPrefix,
            locksByRange,
            proposals,
            backgroundWriter,
            writeAggregator,
            persistenceBackend,
            raft,
            keySpaceRegistry,
            rangeMapStore,
            configuration,
            logger,
            snapshotFloorStore,
            completionReceiptStore,
            coordinatorDecisionStore,
            phaseTwoRouter
        );

        KeyValueContext context = kvContext;

        trySetHandler = new(context);
        tryExtendHandler = new(context);
        tryDeleteHandler = new(context);
        tryGetHandler = new(context);
        tryScanByPrefixHandler = new(context);
        tryScanByPrefixFromDiskHandler = new(context);
        tryGetByBucketHandler = new(context);
        tryGetByRangeHandler = new(context);
        tryExistsHandler = new(context);
        tryCheckWriteIntentHandler = new(context);
        tryAcquireExclusiveLockHandler = new(context);
        tryAcquireExclusivePrefixLockHandler = new(context);
        tryReleaseExclusiveLockHandler = new(context);
        tryReleaseExclusivePrefixLockHandler = new(context);
        tryAcquireExclusiveRangeLockHandler = new(context);
        tryReleaseExclusiveRangeLockHandler = new(context);
        getRangeLocksHandler = new(context);
        importRangeLocksHandler = new(context);
        getSafeTimestampHandler = new(context);
        tryPrepareMutationsHandler = new(context);
        tryCommitMutationsHandler = new(context);
        tryRollbackMutationsHandler = new(context);
        applyRolledBackMutationsHandler = new(context);
        tryCollectHandler = new(context);
        completeProposalHandler = new(context);
        releaseProposalHandler = new(context);
        resumeReadHandler = new(context);
        invalidateOrApplyHandler = new(context);
        completePhaseTwoHandler = new(context);
    }

    /// <summary>
    /// Returns the actor's KeyValueContext. Used only by in-process tests to inspect
    /// per-entry accounting state (CachedBytes) without going through the public API.
    /// </summary>
    internal KeyValueContext GetContext() => kvContext!;

    /// <summary>
    /// Main entry point for the actor.
    /// Receives messages one at a time to prevent concurrency issues
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<KeyValueResponse?> Receive(KeyValueRequest message)
    {
        stopwatch.Restart();

        KeyValueResponse? response = null;

        // Periodic Collect messages and follower-apply InvalidateOrApply messages are
        // internal maintenance — suppress per-message tracing to avoid log spam.
        bool logThis = message.Type != KeyValueRequestType.Collect
                    && message.Type != KeyValueRequestType.InvalidateOrApply
                    && message.Type != KeyValueRequestType.FlushAck;

        try
        {
            if (logThis)
                logger.LogKeyValueActorEnter(
                    actorContext.Self.Runner.Name,
                    message.Type,
                    message.Key,
                    message.Value?.Length,
                    message.ExpiresMs,
                    message.Flags,
                    message.CompareRevision,
                    message.TransactionId,
                    message.Durability
                );

            if (message.Type != KeyValueRequestType.Collect
                && message.Type != KeyValueRequestType.InvalidateOrApply
                && message.Type != KeyValueRequestType.FlushAck)
            {
                if (--operations == 0)
                {
                    if (tryCollectHandler.IsOverBudget())
                        Collect();

                    operations = CollectThreshold;
                }
            }

            response = message.Type switch
            {
                KeyValueRequestType.TrySet => await TrySet(message),
                KeyValueRequestType.TryExtend => await TryExtend(message),
                KeyValueRequestType.TryDelete => await TryDelete(message),
                KeyValueRequestType.TryGet => await TryGet(message),
                KeyValueRequestType.TryExists => await TryExists(message),
                KeyValueRequestType.TryCheckWriteIntent => await TryCheckWriteIntent(message),
                KeyValueRequestType.TryAcquireExclusiveLock => await TryAcquireExclusiveLock(message),
                KeyValueRequestType.TryAcquireExclusivePrefixLock => TryAcquireExclusivePrefixLock(message),
                KeyValueRequestType.TryAcquireExclusiveRangeLock => AcquireExclusiveRangeLock(message),
                KeyValueRequestType.TryReleaseExclusiveLock => await TryReleaseExclusiveLock(message),
                KeyValueRequestType.TryReleaseExclusivePrefixLock => TryReleaseExclusivePrefixLock(message),
                KeyValueRequestType.TryReleaseExclusiveRangeLock => ReleaseExclusiveRangeLock(message),
                KeyValueRequestType.GetRangeLocks => GetRangeLocks(message),
                KeyValueRequestType.ImportRangeLocks => ImportRangeLocks(message),
                KeyValueRequestType.GetSafeTimestamp => await getSafeTimestampHandler.Execute(message),
                KeyValueRequestType.TryPrepareMutations => await TryPrepareMutations(message),
                KeyValueRequestType.StagePrepareMutations => await StagePrepareMutations(message),
                KeyValueRequestType.ApplyRolledBackMutations => await ApplyRolledBackMutations(message),
                KeyValueRequestType.TryCommitMutations => await TryCommitMutations(message),
                KeyValueRequestType.ApplyCommittedMutations => await ApplyCommittedMutations(message),
                KeyValueRequestType.TryRollbackMutations => await TryRollbackMutations(message),
                KeyValueRequestType.GetByBucket => await GetByBucket(message),
                KeyValueRequestType.GetByRange => await GetByRange(message),
                KeyValueRequestType.ScanByPrefix => await ScanByPrefix(message),
                KeyValueRequestType.ScanByPrefixFromDisk => await ScanByPrefixFromDisk(message),
                KeyValueRequestType.CompleteProposal => CompleteProposal(message),
                KeyValueRequestType.ReleaseProposal => ReleaseProposal(message),
                KeyValueRequestType.CompletePhaseTwo => CompletePhaseTwo(message),
                KeyValueRequestType.ResumeRead => ResumeRead(message),
                KeyValueRequestType.InvalidateOrApply => InvalidateOrApply(message),
                KeyValueRequestType.FlushAck => FlushAck(message),
                KeyValueRequestType.Collect => CollectMessage(),
                _ => KeyValueStaticResponses.ErroredResponse
            };

            return response;
        }
        catch (Exception ex)
        {
            logger.LogError("Error processing message: {Type} {Message}\n{Stacktrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }
        finally
        {
            if (logThis)
                logger.LogKeyValueActorTook(
                    actorContext.Self.Runner.Name,
                    message.Type,
                    message.Key,
                    response?.Type,
                    response?.Revision,
                    stopwatch.ElapsedMilliseconds
                );
        }

        return KeyValueStaticResponses.ErroredResponse;
    }

    /// <summary>
    /// Tries to set a key value pair based on the specified flags
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TrySet(KeyValueRequest message)
    {
        return trySetHandler.Execute(message);
    }

    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryExtend(KeyValueRequest message)
    {
        return tryExtendHandler.Execute(message);
    }
    
    /// <summary>
    /// Looks for a KeyValue on the resource and tries to delete it
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryDelete(KeyValueRequest message)
    {
        return tryDeleteHandler.Execute(message);
    }

    /// <summary>
    /// Gets a value by the specified key
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryGet(KeyValueRequest message)
    {
        return tryGetHandler.Execute(message);
    }

    /// <summary>
    /// Scan the actor for keys that match the given prefix
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> ScanByPrefix(KeyValueRequest message)
    {
        return tryScanByPrefixHandler.Execute(message);
    }
    
    /// <summary>
    /// Scan from disk storage for keys that match the given prefix
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> ScanByPrefixFromDisk(KeyValueRequest message)
    {
        return tryScanByPrefixFromDiskHandler.Execute(message);
    }
    
    /// <summary>
    /// Returns keys that start with the given prefix in a consistent way
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> GetByBucket(KeyValueRequest message)
    {
        return tryGetByBucketHandler.Execute(message);
    }

    /// <summary>
    /// Returns a bounded, cursor-paged range of keys within a prefix
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> GetByRange(KeyValueRequest message)
    {
        return tryGetByRangeHandler.Execute(message);
    }
    
    /// <summary>
    /// Checks if a key exists
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryExists(KeyValueRequest message)
    {
        return tryExistsHandler.Execute(message);
    }

    private Task<KeyValueResponse> TryCheckWriteIntent(KeyValueRequest message)
    {
        return tryCheckWriteIntentHandler.Execute(message);
    }
    
    /// <summary>
    /// Acquires an exclusive lock on a key
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryAcquireExclusiveLock(KeyValueRequest message)
    {
        return tryAcquireExclusiveLockHandler.Execute(message);
    }
    
    /// <summary>
    /// Acquires an exclusive lock on group of keys prefixed by the given prefix
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private KeyValueResponse TryAcquireExclusivePrefixLock(KeyValueRequest message)
    {
        return tryAcquireExclusivePrefixLockHandler.Execute(message);
    }
    
    /// <summary>
    /// Releases any acquired exclusive lock on a key if the releaser is the given transaction id
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryReleaseExclusiveLock(KeyValueRequest message)
    {
        return tryReleaseExclusiveLockHandler.Execute(message);
    }
    
    /// <summary>
    /// Releases any acquired exclusive lock on a group of keys prefixed by the given prefix
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private KeyValueResponse TryReleaseExclusivePrefixLock(KeyValueRequest message)
    {
        return tryReleaseExclusivePrefixLockHandler.Execute(message);
    }

    private KeyValueResponse AcquireExclusiveRangeLock(KeyValueRequest message)
    {
        return tryAcquireExclusiveRangeLockHandler.Execute(message);
    }

    private KeyValueResponse ReleaseExclusiveRangeLock(KeyValueRequest message)
    {
        return tryReleaseExclusiveRangeLockHandler.Execute(message);
    }

    private KeyValueResponse GetRangeLocks(KeyValueRequest message)
    {
        return getRangeLocksHandler.Execute(message);
    }

    private KeyValueResponse ImportRangeLocks(KeyValueRequest message)
    {
        return importRangeLocksHandler.Execute(message);
    }

    /// <summary>
    /// Prepare the mutations made to the key currently held in the MVCC entry
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryPrepareMutations(KeyValueRequest message)
    {
        return tryPrepareMutationsHandler.Execute(message);
    }

    /// <summary>
    /// Prepare for the partition-batched path: validate and pin the write intent exactly as a per-key
    /// prepare, but return the serialized proposal for the manager to batch into one partition-wide
    /// <c>ReplicateLogs</c> instead of proposing here.
    /// </summary>
    private Task<KeyValueResponse> StagePrepareMutations(KeyValueRequest message)
    {
        return tryPrepareMutationsHandler.StageExecute(message);
    }

    /// <summary>
    /// Unwinds a write intent staged for a partition batch that never proposed — a no-Raft local rollback of
    /// prepare state, so a batch that fails staging leaves no pinned intent behind.
    /// </summary>
    private Task<KeyValueResponse> ApplyRolledBackMutations(KeyValueRequest message)
    {
        return applyRolledBackMutationsHandler.Execute(message);
    }

    /// <summary>
    /// Commit the mutations made to the key currently held in the MVCC entry
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryCommitMutations(KeyValueRequest message)
    {
        return tryCommitMutationsHandler.Execute(message);
    }

    /// <summary>
    /// Applies a mutation whose partition ticket the manager already committed in one batched CommitLogs —
    /// the per-key apply half of partition-batched commit, with no Raft round trip of its own.
    /// </summary>
    private Task<KeyValueResponse> ApplyCommittedMutations(KeyValueRequest message)
    {
        return tryCommitMutationsHandler.ApplyExecute(message);
    }
    
    /// <summary>
    /// Rollback made to the key currently held in the MVCC entry
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryRollbackMutations(KeyValueRequest message)
    {
        return tryRollbackMutationsHandler.Execute(message);
    }
    
    /// <summary>
    /// Completes a replication proposal by applying the changes to the key-value store
    /// and removing the replication intent from the entry
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private KeyValueResponse CompleteProposal(KeyValueRequest message)
    {
        return completeProposalHandler.Execute(message);
    }
    
    /// <summary>
    /// Releases a failed replication proposal by removing the temporary proposal
    /// and removing the replication intent from the entry
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private KeyValueResponse ReleaseProposal(KeyValueRequest message)
    {
        return releaseProposalHandler.Execute(message);
    }

    private KeyValueResponse? ResumeRead(KeyValueRequest message)
    {
        return resumeReadHandler.Execute(message);
    }

    /// <summary>
    /// Resolves the caller's promise from a two-phase-commit Raft outcome produced off the mailbox
    /// by <see cref="KeyValuePhaseTwoActor"/>.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private KeyValueResponse CompletePhaseTwo(KeyValueRequest message)
    {
        return completePhaseTwoHandler.Execute(message);
    }

    private KeyValueResponse? InvalidateOrApply(KeyValueRequest message)
    {
        return invalidateOrApplyHandler.Execute(message);
    }

    /// <summary>
    /// Applies a flush acknowledgement from the background writer: the revision carried in
    /// <see cref="KeyValueRequest.CompareRevision"/> is now durably on disk for this key.
    /// Advances <see cref="KeyValueEntry.FlushedRevision"/> so the entry becomes eligible for
    /// eviction, but only when the acked revision is one this entry actually reached
    /// (<c>acked &lt;= entry.Revision</c>) — a stale ack for a superseded revision (e.g. the key
    /// was deleted and re-created at a lower revision) is ignored so it can never mark a newer
    /// unflushed revision as clean. If the entry is already gone, the ack is a no-op.
    /// </summary>
    private KeyValueResponse? FlushAck(KeyValueRequest message)
    {
        if (kvContext is null)
            return null;

        if (!kvContext.Store.TryGetValue(message.Key, out KeyValueEntry? entry) || entry is null)
            return null;

        long ackedRevision = message.CompareRevision;
        if (ackedRevision <= entry.Revision && ackedRevision > entry.FlushedRevision)
            entry.FlushedRevision = ackedRevision;

        return null;
    }

    /// <summary>
    /// Try to collect unused objects in the BTree store
    /// </summary>
    private void Collect()
    {
        tryCollectHandler.Execute();
    }

    private KeyValueResponse? CollectMessage()
    {
        Collect();
        return null;
    }
}
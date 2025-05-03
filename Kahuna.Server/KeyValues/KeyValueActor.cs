
using Nixie;
using Kommander;
using System.Diagnostics;
using Google.Protobuf;
using Kahuna.Server.Configuration;
using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Utils;

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
    /// Handles the process of collecting and managing key/value resources within the actor.
    /// This handler is responsible for triggering cleanup or optimization tasks, such as
    /// consolidating cached data or freeing unneeded resources to maintain efficient operation.
    /// </summary>
    private readonly TryCollectHandler tryCollectHandler;

    /// <summary>
    /// A high-resolution timer used to measure the time elapsed during the handling of requests within the actor.
    /// The stopwatch is utilized to record and log the duration of operations, aiding in performance monitoring
    /// and diagnostics for the KeyValueActor.
    /// </summary>
    private readonly Stopwatch stopwatch = Stopwatch.StartNew();

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorContext"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public KeyValueActor(
        IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
        IPersistenceBackend persistenceBackend,
        IRaft raft,
        KahunaConfiguration configuration,
        ILogger<IKahuna> logger
    )
    {
        this.actorContext = actorContext;
        this.logger = logger;
        
        KeyValueContext context = new(keyValuesStore, locksByPrefix, backgroundWriter, persistenceBackend, raft, configuration, logger);

        trySetHandler = new(context);
        tryExtendHandler = new(context);
        tryDeleteHandler = new(context);
        tryGetHandler = new(context);
        tryScanByPrefixHandler = new(context);
        tryScanByPrefixFromDiskHandler = new(context);
        tryGetByBucketHandler = new(context);
        tryExistsHandler = new(context);
        tryAcquireExclusiveLockHandler = new(context);
        tryAcquireExclusivePrefixLockHandler = new(context);
        tryReleaseExclusiveLockHandler = new(context);
        tryReleaseExclusivePrefixLockHandler = new(context);
        tryPrepareMutationsHandler = new(context);
        tryCommitMutationsHandler = new(context);
        tryRollbackMutationsHandler = new(context);
        tryCollectHandler = new(context);
    }

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

        try
        {
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
            
            // Console.WriteLine("{0}", operations);

            if (--operations == 0)
            {
                Collect();
                operations = CollectThreshold;
            }

            response = message.Type switch
            {
                KeyValueRequestType.TrySet => await TrySet(message),
                KeyValueRequestType.TryExtend => await TryExtend(message),
                KeyValueRequestType.TryDelete => await TryDelete(message),
                KeyValueRequestType.TryGet => await TryGet(message),
                KeyValueRequestType.TryExists => await TryExists(message),
                KeyValueRequestType.TryAcquireExclusiveLock => await TryAcquireExclusiveLock(message),
                KeyValueRequestType.TryAcquireExclusivePrefixLock => TryAcquireExclusivePrefixLock(message),
                KeyValueRequestType.TryReleaseExclusiveLock => await TryReleaseExclusiveLock(message),
                KeyValueRequestType.TryReleaseExclusivePrefixLock => TryReleaseExclusivePrefixLock(message),
                KeyValueRequestType.TryPrepareMutations => await TryPrepareMutations(message),
                KeyValueRequestType.TryCommitMutations => await TryCommitMutations(message),
                KeyValueRequestType.TryRollbackMutations => await TryRollbackMutations(message),
                KeyValueRequestType.GetByBucket => await GetByBucket(message),
                KeyValueRequestType.ScanByPrefix => await ScanByPrefix(message),
                KeyValueRequestType.ScanByPrefixFromDisk => await ScanByPrefixFromDisk(message),
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
    /// Checks if a key exists
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryExists(KeyValueRequest message)
    {
        return tryExistsHandler.Execute(message);
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
    /// Commit the mutations made to the key currently held in the MVCC entry
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryCommitMutations(KeyValueRequest message)
    {
        return tryCommitMutationsHandler.Execute(message);
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
    /// Try to collect unused objects in the BTree store
    /// </summary>
    private void Collect()
    {
        tryCollectHandler.Execute();
    }
}
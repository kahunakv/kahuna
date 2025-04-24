
using Nixie;
using Kommander;
using System.Diagnostics;

using Kahuna.Server.Persistence;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.KeyValues.Handlers;
using Kahuna.Server.Persistence.Backend;
using Kahuna.Utils;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Each of these actors functions as a worker, accepting requests to perform operations on key/value pairs.
/// The actor maintains an in-memory cache and if a key is not found, it attempts to retrieve it from disk.
/// Operations with Linearizable consistency persist all modifications to disk.
/// </summary>
public sealed class KeyValueActor : IActor<KeyValueRequest, KeyValueResponse>
{
    private const int CollectThreshold = 1000;
    
    private readonly IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext;
    
    private readonly BTree<string, KeyValueContext> keyValuesStore = new(32);

    private readonly ILogger<IKahuna> logger;

    private int operations = CollectThreshold;

    private readonly TrySetHandler trySetHandler;
    
    private readonly TryExtendHandler tryExtendHandler;
    
    private readonly TryDeleteHandler tryDeleteHandler;

    private readonly TryGetHandler tryGetHandler;
    
    private readonly TryGetByPrefixHandler tryGetByPrefixHandler;
    
    private readonly TryScanByPrefixHandler tryScanByPrefixHandler;
    
    private readonly TryExistsHandler tryExistsHandler;

    private readonly TryAcquireExclusiveLockHandler tryAcquireExclusiveLockHandler;
    
    private readonly TryReleaseExclusiveLockHandler tryReleaseExclusiveLockHandler;

    private readonly TryPrepareMutationsHandler tryPrepareMutationsHandler;
    
    private readonly TryCommitMutationsHandler tryCommitMutationsHandler;

    private readonly TryRollbackMutationsHandler tryRollbackMutationsHandler;

    private readonly TryCollectHandler tryCollectHandler;

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
        ILogger<IKahuna> logger
    )
    {
        this.actorContext = actorContext;
        this.logger = logger;

        trySetHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryExtendHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryDeleteHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryGetHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryScanByPrefixHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryGetByPrefixHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryExistsHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryAcquireExclusiveLockHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryReleaseExclusiveLockHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryPrepareMutationsHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryCommitMutationsHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryRollbackMutationsHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
        tryCollectHandler = new(keyValuesStore, backgroundWriter, persistenceBackend, raft, logger);
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
            logger.LogDebug(
                "KeyValueActor Message: {Actor} {Type} Key={Key} {Value} Expires={ExpiresMs} Flags={Flags} Revision={Revision} TxId={TransactionId} {Consistency}",
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
                KeyValueRequestType.TryReleaseExclusiveLock => await TryReleaseExclusiveLock(message),
                KeyValueRequestType.TryPrepareMutations => await TryPrepareMutations(message),
                KeyValueRequestType.TryCommitMutations => await TryCommitMutations(message),
                KeyValueRequestType.TryRollbackMutations => await TryRollbackMutations(message),
                KeyValueRequestType.GetByPrefix => await GetByPrefix(message),
                KeyValueRequestType.ScanByPrefix => await ScanByPrefix(message),
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
            logger.LogDebug(
                "KeyValueActor Took: {Actor} {Type} Key={Key} Response={Response} Revision={Revision} Time={Elasped}ms",
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
    /// Returns keys that start with the given prefix in a consistent way
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> GetByPrefix(KeyValueRequest message)
    {
        return tryGetByPrefixHandler.Execute(message);
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
    /// Releases any acquired exclusive lock on a key if the releaser is the given transaction id
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> TryReleaseExclusiveLock(KeyValueRequest message)
    {
        return tryReleaseExclusiveLockHandler.Execute(message);
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

    private void Collect()
    {
        tryCollectHandler.Execute();
    }
}
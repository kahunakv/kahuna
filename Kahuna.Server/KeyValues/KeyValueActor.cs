
using System.Diagnostics;
using Nixie;
using Google.Protobuf;
using Kahuna.Server.KeyValues.Handlers;
using Kommander;
using Kommander.Time;
using Kommander.Data;

using Kahuna.Server.Persistence;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.Replication.Protos;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Each of these actors functions as a worker, accepting requests to perform operations on key/value pairs.
/// The actor maintains an in-memory cache and if a key is not found, it attempts to retrieve it from disk.
/// Operations with Linearizable consistency persist all modifications to disk.
/// </summary>
public sealed class KeyValueActor : IActor<KeyValueRequest, KeyValueResponse>
{
    private readonly IActorContext<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext;
    
    private readonly Dictionary<string, KeyValueContext> keyValuesStore = new();

    private readonly ILogger<IKahuna> logger;

    private uint operations;

    private readonly TrySetHandler trySetHandler;
    
    private readonly TryExtendHandler tryExtendHandler;
    
    private readonly TryDeleteHandler tryDeleteHandler;

    private readonly TryGetHandler tryGetHandler;
    
    private readonly TryScanByPrefixHandler tryScanByPrefixHandler;
    
    private readonly TryExistsHandler tryExistsHandler;

    private readonly TryAdquireExclusiveLockHandler tryAdquireExclusiveLockHandler;
    
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
        IPersistence persistence,
        IRaft raft,
        ILogger<IKahuna> logger
    )
    {
        this.actorContext = actorContext;
        this.logger = logger;

        trySetHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
        tryExtendHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
        tryDeleteHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
        tryGetHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
        tryScanByPrefixHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
        tryExistsHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
        tryAdquireExclusiveLockHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
        tryReleaseExclusiveLockHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
        tryPrepareMutationsHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
        tryCommitMutationsHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
        tryRollbackMutationsHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
        tryCollectHandler = new(keyValuesStore, backgroundWriter, persistence, raft, logger);
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

        try
        {
            logger.LogDebug(
                "KeyValueActor Message: {Actor} {Type} Key={Key} {Value} Expires={ExpiresMs} Flags={Flags} TxId={TransactionId} {Consistency}",
                actorContext.Self.Runner.Name,
                message.Type,
                message.Key,
                message.Value?.Length,
                message.ExpiresMs,
                message.Flags,
                message.TransactionId,
                message.Durability
            );

            if ((operations++) % 1000 == 0)
                await Collect();

            return message.Type switch
            {
                KeyValueRequestType.TrySet => await TrySet(message),
                KeyValueRequestType.TryExtend => await TryExtend(message),
                KeyValueRequestType.TryDelete => await TryDelete(message),
                KeyValueRequestType.TryGet => await TryGet(message),
                KeyValueRequestType.TryExists => await TryExists(message),
                KeyValueRequestType.TryAcquireExclusiveLock => await TryAdquireExclusiveLock(message),
                KeyValueRequestType.TryReleaseExclusiveLock => await TryReleaseExclusiveLock(message),
                KeyValueRequestType.TryPrepareMutations => await TryPrepareMutations(message),
                KeyValueRequestType.TryCommitMutations => await TryCommitMutations(message),
                KeyValueRequestType.TryRollbackMutations => await TryRollbackMutations(message),
                KeyValueRequestType.ScanByPrefix => await ScanByPrefix(message),
                _ => new(KeyValueResponseType.Errored)
            };
        }
        catch (Exception ex)
        {
            logger.LogError("Error processing message: {Type} {Message}\n{Stacktrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }
        finally
        {
            logger.LogDebug(
                "KeyValueActor Took: {Actor} {Type} Key={Key} Time={Elasped}ms",
                actorContext.Self.Runner.Name,
                message.Type,
                message.Key,
                stopwatch.ElapsedMilliseconds
            );
        }

        return new(KeyValueResponseType.Errored);
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
    /// 
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private Task<KeyValueResponse> ScanByPrefix(KeyValueRequest message)
    {
        return tryScanByPrefixHandler.Execute(message);
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
    private Task<KeyValueResponse> TryAdquireExclusiveLock(KeyValueRequest message)
    {
        return tryAdquireExclusiveLockHandler.Execute(message);
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

    private Task Collect()
    {
        return tryCollectHandler.Execute();
    }
}
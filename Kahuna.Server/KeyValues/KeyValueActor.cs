
using Nixie;
using Kommander;
using Kommander.Time;

using Kahuna.Persistence;
using Kahuna.Replication;
using Kahuna.Shared.KeyValue;
using Kahuna.Replication.Protos;

namespace Kahuna.KeyValues;

public class KeyValueActor : IActorStruct<KeyValueRequest, KeyValueResponse>
{
    private readonly IActorContextStruct<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext;

    private readonly IActorRef<KeyValueBackgroundWriterActor, KeyValueBackgroundWriteRequest> backgroundWriter;

    private readonly IPersistence persistence;

    private readonly IRaft raft;

    private readonly Dictionary<string, KeyValueContext> keyValues = new();

    private readonly ILogger<IKahuna> logger;

    private long operations;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorContext"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public KeyValueActor(
        IActorContextStruct<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext,
        IActorRef<KeyValueBackgroundWriterActor, KeyValueBackgroundWriteRequest> backgroundWriter,
        IPersistence persistence,
        IRaft raft,
        ILogger<IKahuna> logger
    )
    {
        this.actorContext = actorContext;
        this.backgroundWriter = backgroundWriter;
        this.persistence = persistence;
        this.raft = raft;
        this.logger = logger;
    }

    /// <summary>
    /// Main entry point for the actor.
    /// Receives messages one at a time to prevent concurrency issues
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task<KeyValueResponse> Receive(KeyValueRequest message)
    {
        try
        {
            logger.LogDebug(
                "KeyValueActor Message: {Actor} {Type} {Key} {Value} {ExpiresMs} {Consistency}",
                actorContext.Self.Runner.Name,
                message.Type,
                message.Key,
                message.Value,
                message.ExpiresMs,
                message.Consistency
            );

            if ((operations++) % 1000 == 0)
                await Collect();

            return message.Type switch
            {
                KeyValueRequestType.TrySet => await TrySet(message),
                KeyValueRequestType.TryDelete => await TryDelete(message),
                KeyValueRequestType.TryExtend => await TryExtend(message),
                KeyValueRequestType.TryGet => await TryGet(message),
                _ => new(KeyValueResponseType.Errored)
            };
        }
        catch (Exception ex)
        {
            logger.LogError("Error processing message: {Type} {Message}\n{Stacktrace}", ex.GetType().Name, ex.Message, ex.StackTrace);
        }

        return new(KeyValueResponseType.Errored);
    }
    
    /// <summary>
    /// Tries to set a key value pair based on the specified flags
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TrySet(KeyValueRequest message)
    {
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        if (!keyValues.TryGetValue(message.Key, out KeyValueContext? context))
        {
            KeyValueContext? newContext = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Consistency == KeyValueConsistency.Linearizable)
                newContext = await persistence.GetKeyValue(message.Key);

            newContext ??= new()
            {
                Value = message.Value,
                Expires = currentTime + message.ExpiresMs,
                LastUsed = currentTime
            };

            // If the key is consistent, we need to persist and replicate the KeyValue state
            if (message.Consistency == KeyValueConsistency.Linearizable)
            {
                bool success = await PersistAndReplicateKeyValueMessage(message.Type, message.Key, newContext, currentTime, KeyValueState.Set);
                if (!success)
                    return new(KeyValueResponseType.Errored);
            }

            keyValues.Add(message.Key, newContext);

            return new(KeyValueResponseType.Set);
        }
        
        if (!string.IsNullOrEmpty(context.Value))
        {
            bool isExpired = context.Expires - currentTime < TimeSpan.Zero;

            if (context.Value == message.Value && !isExpired)
                return new(KeyValueResponseType.Set);

            if (!isExpired)
                return new(KeyValueResponseType.Set);
        }
        
        context.Value = message.Value;
        context.Expires = currentTime + message.ExpiresMs;
        context.LastUsed = currentTime;

        if (message.Consistency == KeyValueConsistency.Linearizable)
        {
            bool success = await PersistAndReplicateKeyValueMessage(message.Type, message.Key, context, currentTime, KeyValueState.Set);
            if (!success)
                return new(KeyValueResponseType.Errored);
        }

        return new(KeyValueResponseType.Set);
    }
    
    /// <summary>
    /// Looks for a KeyValue on the resource and tries to extend it
    /// If the KeyValue doesn't exist or the owner is different, return an error
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TryExtend(KeyValueRequest message)
    {
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Consistency);
        if (context is null)
            return new(KeyValueResponseType.DoesNotExist);

        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        context.Expires = currentTime + message.ExpiresMs;
        context.LastUsed = currentTime;

        if (message.Consistency == KeyValueConsistency.Linearizable)
        {
            bool success = await PersistAndReplicateKeyValueMessage(message.Type, message.Key, context, currentTime, KeyValueState.Set);
            if (!success)
                return new(KeyValueResponseType.Errored);
        }

        return new(KeyValueResponseType.Extended);
    }
    
    /// <summary>
    /// Looks for a KeyValue on the resource and tries to unKeyValue it
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TryDelete(KeyValueRequest message)
    {
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Consistency);
        if (context is null)
            return new(KeyValueResponseType.DoesNotExist);
        
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        context.Value = null;
        context.LastUsed = currentTime;

        if (message.Consistency == KeyValueConsistency.Linearizable)
        {
            bool success = await PersistAndReplicateKeyValueMessage(message.Type, message.Key, context, currentTime, KeyValueState.Deleted);
            if (!success)
                return new(KeyValueResponseType.Errored);
        }

        return new(KeyValueResponseType.Deleted);
    }

    /// <summary>
    /// Gets Information about an existing KeyValue
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TryGet(KeyValueRequest message)
    {
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Consistency);
        if (context is null)
            return new(KeyValueResponseType.DoesNotExist);

        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        if (context.Expires - currentTime < TimeSpan.Zero)
            return new(KeyValueResponseType.DoesNotExist);

        ReadOnlyKeyValueContext readOnlyKeyValueContext = new(context.Value, context.Expires);

        return new(KeyValueResponseType.Get, readOnlyKeyValueContext);
    }

    private async ValueTask<KeyValueContext?> GetKeyValueContext(string resource, KeyValueConsistency? consistency)
    {
        if (!keyValues.TryGetValue(resource, out KeyValueContext? context))
        {
            if (consistency == KeyValueConsistency.Linearizable)
            {
                context = await persistence.GetKeyValue(resource);
                if (context is not null)
                {
                    keyValues.Add(resource, context);
                    return context;
                }
                
                return null;
            }
            
            return null;    
        }
        
        return context;
    }

    /// <summary>
    /// Persists and replicates KeyValue message to the Raft cluster
    /// </summary>
    /// <param name="type"></param>
    /// <param name="key"></param>
    /// <param name="context"></param>
    /// <param name="currentTime"></param>
    /// <param name="KeyValueState"></param>
    /// <returns></returns>
    private async Task<bool> PersistAndReplicateKeyValueMessage(KeyValueRequestType type, string key, KeyValueContext context, HLCTimestamp currentTime, KeyValueState KeyValueState)
    {
        if (!raft.Joined)
            return true;

        int partitionId = raft.GetPartitionKey(key);

        (bool success, _, long _) = await raft.ReplicateLogs(
            partitionId,
            "KeyValueMessage",
            ReplicationSerializer.Serialize(new KeyValueMessage
            {
                Type = (int)type,
                Key = key,
                Value = context.Value ?? "",
                ExpireLogical = context.Expires.L,
                ExpireCounter = context.Expires.C,
                TimeLogical = currentTime.L,
                TimeCounter = currentTime.C,
                Consistency = (int)KeyValueConsistency.LinearizableReplication
            })
        );

        if (!success)
            return false;
        
        backgroundWriter.Send(new(
            KeyValueBackgroundWriteType.Queue,
            partitionId,
            key, 
            context.Value, 
            context.Expires,
            KeyValueConsistency.Linearizable,
            KeyValueState
        ));

        return success;
    }

    private async ValueTask Collect()
    {
        if (keyValues.Count < 2000)
            return;
        
        int number = 0;
        TimeSpan range = TimeSpan.FromMinutes(30);
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        HashSet<string> keys = [];

        foreach (KeyValuePair<string, KeyValueContext> key in keyValues)
        {
            if ((currentTime - key.Value.LastUsed) < range)
                continue;
            
            keys.Add(key.Key);
            number++;
            
            if (number > 100)
                break;
        }

        foreach (string key in keys)
        {
            keyValues.Remove(key);
            
            Console.WriteLine("Removed {0}", key);
        }
    }
}
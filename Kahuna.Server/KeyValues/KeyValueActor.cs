
using System.Text;
using Nixie;
using Google.Protobuf;

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
public sealed class KeyValueActor : IActorStruct<KeyValueRequest, KeyValueResponse>
{
    private readonly IActorContextStruct<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext;

    private readonly IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter;

    private readonly IPersistence persistence;

    private readonly IRaft raft;

    private readonly Dictionary<string, KeyValueContext> keyValuesStore = new();
    
    private readonly HashSet<string> keysToEvict = [];

    private readonly ILogger<IKahuna> logger;

    private uint operations;

    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="actorContext"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public KeyValueActor(
        IActorContextStruct<KeyValueActor, KeyValueRequest, KeyValueResponse> actorContext,
        IActorRef<BackgroundWriterActor, BackgroundWriteRequest> backgroundWriter,
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
                "KeyValueActor Message: {Actor} {Type} Key={Key} {Value} Expires={ExpiresMs} Flags={Flags} TxId={TransactionId} {Consistency}",
                actorContext.Self.Runner.Name,
                message.Type,
                message.Key,
                message.Value?.Length,
                message.ExpiresMs,
                message.Flags,
                message.TransactionId,
                message.Consistency
            );

            if ((operations++) % 1000 == 0)
                await Collect();

            return message.Type switch
            {
                KeyValueRequestType.TrySet => await TrySet(message),
                KeyValueRequestType.TryExtend => await TryExtend(message),
                KeyValueRequestType.TryDelete => await TryDelete(message),
                KeyValueRequestType.TryGet => await TryGet(message),
                KeyValueRequestType.TryAcquireExclusiveLock => await TryAdquireExclusiveLock(message),
                KeyValueRequestType.TryReleaseExclusiveLock => await TryReleaseExclusiveLock(message),
                KeyValueRequestType.TryPrepareMutations => await TryPrepareMutations(message),
                KeyValueRequestType.TryCommitMutations => await TryCommitMutations(message),
                KeyValueRequestType.TryRollbackMutations => await TryRollbackMutations(message),
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
        bool exists = true;
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        if (!keyValuesStore.TryGetValue(message.Key, out KeyValueContext? context))
        {
            exists = false;
            KeyValueContext? newContext = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Consistency == KeyValueConsistency.Linearizable)
            {
                newContext = await persistence.GetKeyValue(message.Key);
                if (newContext is not null)
                {
                    if (newContext.State == KeyValueState.Deleted)
                    {
                        newContext.Value = null;
                        exists = false;
                    }
                    else
                    {
                        if (newContext.Expires != HLCTimestamp.Zero && newContext.Expires - currentTime < TimeSpan.Zero)
                        {
                            newContext.State = KeyValueState.Deleted;
                            newContext.Value = null;
                            exists = false;
                        }
                        else
                        {
                            exists = true;
                        }
                    }
                }
            }

            newContext ??= new() { Revision = -1 };
            
            context = newContext;

            keyValuesStore.Add(message.Key, newContext);
        }
        else
        {
            if (context.State == KeyValueState.Deleted)
            {
                context.Value = null;
                exists = false;
            }
        }
        
        if (context.WriteIntent is not null && context.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.MustRetry, 0);

        HLCTimestamp newExpires = message.ExpiresMs > 0 ? (currentTime + message.ExpiresMs) : HLCTimestamp.Zero;
        
        // Temporarily store the value in the MVCC entry if the transaction ID is set
        if (message.TransactionId != HLCTimestamp.Zero)
        {
            exists = true;
            context.MvccEntries ??= new();

            if (!context.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? entry))
            {
                entry = new()
                {
                    Value = context.Value, 
                    Revision = context.Revision, 
                    Expires = context.Expires, 
                    LastUsed = context.LastUsed,
                    State = context.State
                };
                
                context.MvccEntries.Add(message.TransactionId, entry);
            }

            if (entry.State == KeyValueState.Deleted)
            {
                entry.Value = null;
                exists = false;
            }

            if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
            {
                entry.State = KeyValueState.Deleted;
                entry.Value = null;
                exists = false;
            }

            switch (message.Flags)
            {
                case KeyValueFlags.SetIfExists when !exists:
                case KeyValueFlags.SetIfNotExists when exists:
                case KeyValueFlags.SetIfEqualToValue when exists && !((ReadOnlySpan<byte>)entry.Value).SequenceEqual(message.CompareValue):
                case KeyValueFlags.SetIfEqualToRevision when exists && entry.Revision != message.CompareRevision:
                    return new(KeyValueResponseType.NotSet, entry.Revision);

                case KeyValueFlags.None:
                case KeyValueFlags.Set:
                default:
                    break;
            }

            entry.Value = message.Value;
            entry.Expires = newExpires;
            entry.Revision++;
            entry.LastUsed = currentTime;
            entry.State = KeyValueState.Set;
            
            return new(KeyValueResponseType.Set, context.Revision);
        }
        
        if (message.CompareValue is not null)
            Console.WriteLine(
                "{0} {1} {2} {3} {4} {5}", 
                exists, 
                Encoding.UTF8.GetString(context.Value ?? []), 
                Encoding.UTF8.GetString(message.Value ?? []), 
                Encoding.UTF8.GetString(message.CompareValue ?? []),
                context.Revision,
                message.CompareRevision
            );
        
        switch (message.Flags)
        {
            case KeyValueFlags.SetIfExists when !exists:
            case KeyValueFlags.SetIfNotExists when exists:
            case KeyValueFlags.SetIfEqualToValue when exists && !((ReadOnlySpan<byte>)context.Value).SequenceEqual(message.CompareValue):
            case KeyValueFlags.SetIfEqualToRevision when exists && context.Revision != message.CompareRevision:
                return new(KeyValueResponseType.NotSet, context.Revision);

            case KeyValueFlags.None:
            case KeyValueFlags.Set:
            default:
                break;
        }
        
        KeyValueProposal proposal = new(
            message.Key,
            message.Value,
            context.Revision + 1,
            newExpires,
            currentTime,
            KeyValueState.Set
        );

        if (message.Consistency == KeyValueConsistency.Linearizable)
        {
            bool success = await PersistAndReplicateKeyValueMessage(message.Type, proposal, currentTime);
            if (!success)
                return new(KeyValueResponseType.Errored);
        }
        
        context.Value = proposal.Value;
        context.Revision = proposal.Revision;
        context.Expires = proposal.Expires;
        context.LastUsed = proposal.LastUsed;
        context.State = proposal.State;

        return new(KeyValueResponseType.Set, context.Revision);
    }
    
    /// <summary>
    /// Set a timeout on key. After the timeout has expired, the key will automatically be deleted
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TryExtend(KeyValueRequest message)
    {
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Consistency);
        
        if (context is null)
            return new(KeyValueResponseType.DoesNotExist);
        
        if (context.State == KeyValueState.Deleted)
            return new(KeyValueResponseType.DoesNotExist, context.Revision);
        
        if (context.WriteIntent is not null)
            return new(KeyValueResponseType.MustRetry, 0);
        
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();
        
        if (context.Expires != HLCTimestamp.Zero && context.Expires - currentTime < TimeSpan.Zero)
            return new(KeyValueResponseType.DoesNotExist, context.Revision);
        
        KeyValueProposal proposal = new(
            message.Key,
            context.Value,
            context.Revision,
            currentTime + message.ExpiresMs,
            currentTime,
            context.State
        );
        
        if (message.Consistency == KeyValueConsistency.Linearizable)
        {
            bool success = await PersistAndReplicateKeyValueMessage(message.Type, proposal, currentTime);
            if (!success)
                return new(KeyValueResponseType.Errored);
        }
        
        context.Expires = proposal.Expires;
        context.LastUsed = proposal.LastUsed;

        return new(KeyValueResponseType.Extended, context.Revision);
    }
    
    /// <summary>
    /// Looks for a KeyValue on the resource and tries to delete it
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TryDelete(KeyValueRequest message)
    {
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Consistency);
        if (context is null)
            return new(KeyValueResponseType.DoesNotExist);
        
        if (context.WriteIntent is not null && context.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.MustRetry, 0);
        
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();
        
        // Temporarily store the value in the MVCC entry if the transaction ID is set
        if (message.TransactionId != HLCTimestamp.Zero)
        {
            context.MvccEntries ??= new();

            if (!context.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? entry))
            {
                entry = new()
                {
                    Value = context.Value, 
                    Revision = context.Revision, 
                    Expires = context.Expires, 
                    LastUsed = context.LastUsed,
                    State = context.State
                };
                
                context.MvccEntries.Add(message.TransactionId, entry);
            }
            
            if (context.State == KeyValueState.Deleted)
                return new(KeyValueResponseType.DoesNotExist, entry.Revision);
            
            entry.State = KeyValueState.Deleted;
            
            return new(KeyValueResponseType.Deleted, entry.Revision);
        }
        
        if (context.State == KeyValueState.Deleted)
            return new(KeyValueResponseType.DoesNotExist, context.Revision);
        
        KeyValueProposal proposal = new(
            message.Key,
            null,
            context.Revision,
            context.Expires,
            currentTime,
            KeyValueState.Deleted
        );

        if (message.Consistency == KeyValueConsistency.Linearizable)
        {
            bool success = await PersistAndReplicateKeyValueMessage(message.Type, proposal, currentTime);
            if (!success)
                return new(KeyValueResponseType.Errored);
        }
        
        context.Value = proposal.Value;
        context.LastUsed = proposal.LastUsed;
        context.State = proposal.State;
        
        return new(KeyValueResponseType.Deleted, context.Revision);
    }

    /// <summary>
    /// Gets a value by the specified key
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TryGet(KeyValueRequest message)
    {
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Consistency);

        ReadOnlyKeyValueContext readOnlyKeyValueContext;

        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        if (message.TransactionId != HLCTimestamp.Zero)
        {
            if (context is null)
            {
                context = new();
                keyValuesStore.Add(message.Key, context);
            }
            
            context.MvccEntries ??= new();

            if (!context.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? entry))
            {
                entry = new()
                {
                    Value = context.Value, 
                    Revision = context.Revision, 
                    Expires = context.Expires, 
                    LastUsed = context.LastUsed,
                    State = context.State
                };

                context.MvccEntries.Add(message.TransactionId, entry);
            }
            
            if (entry.State == KeyValueState.Deleted)
                return new(KeyValueResponseType.DoesNotExist, new ReadOnlyKeyValueContext(null, context?.Revision ?? 0, HLCTimestamp.Zero));
            
            if (entry.Expires != HLCTimestamp.Zero && entry.Expires - currentTime < TimeSpan.Zero)
                return new(KeyValueResponseType.DoesNotExist, new ReadOnlyKeyValueContext(null, entry?.Revision ?? 0, HLCTimestamp.Zero));
            
            readOnlyKeyValueContext = new(entry.Value, entry.Revision, entry.Expires);

            return new(KeyValueResponseType.Get, readOnlyKeyValueContext);
        }
        
        if (context is null)
            return new(KeyValueResponseType.DoesNotExist, new ReadOnlyKeyValueContext(null, context?.Revision ?? 0, HLCTimestamp.Zero));
        
        if (context.State == KeyValueState.Deleted)
            return new(KeyValueResponseType.DoesNotExist, new ReadOnlyKeyValueContext(null, context?.Revision ?? 0, HLCTimestamp.Zero));

        if (context.Expires != HLCTimestamp.Zero && context.Expires - currentTime < TimeSpan.Zero)
            return new(KeyValueResponseType.DoesNotExist, new ReadOnlyKeyValueContext(null, context?.Revision ?? 0, HLCTimestamp.Zero));
        
        context.LastUsed = currentTime;

        readOnlyKeyValueContext = new(context.Value, context.Revision, context.Expires);

        return new(KeyValueResponseType.Get, readOnlyKeyValueContext);
    }
    
    /// <summary>
    /// Acquires an exclusive lock on a key
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TryAdquireExclusiveLock(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
            return new(KeyValueResponseType.Errored);
        
        HLCTimestamp currentTime = await raft.HybridLogicalClock.ReceiveEvent(message.TransactionId);

        if (!keyValuesStore.TryGetValue(message.Key, out KeyValueContext? context))
        {
            KeyValueContext? newContext = null;

            /// Try to retrieve KeyValue context from persistence
            if (message.Consistency == KeyValueConsistency.Linearizable)
                newContext = await persistence.GetKeyValue(message.Key);

            newContext ??= new() { Revision = -1 };
            
            context = newContext;

            keyValuesStore.Add(message.Key, newContext);
        }

        if (context.WriteIntent is not null)
        {
            // if the transactionId is the same owner no need to acquire the lock
            if (context.WriteIntent.TransactionId == message.TransactionId) 
                return new(KeyValueResponseType.Locked);

            // Check if the lease is still active
            if (context.WriteIntent.Expires != HLCTimestamp.Zero && context.WriteIntent.Expires - currentTime > TimeSpan.Zero)
                return new(KeyValueResponseType.AlreadyLocked);
        }

        context.WriteIntent = new()
        {
            TransactionId = message.TransactionId,
            Expires = message.TransactionId + message.ExpiresMs,
        };
        
        return new(KeyValueResponseType.Locked);
    }
    
    /// <summary>
    /// Releases any acquired exclusive lock on a key if the releaser is the given transaction id
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TryReleaseExclusiveLock(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
            return new(KeyValueResponseType.Errored);
        
        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Consistency);
        
        if (context is null)
            return new(KeyValueResponseType.DoesNotExist);
        
        if (context.WriteIntent is not null && context.WriteIntent.TransactionId != message.TransactionId)
            return new(KeyValueResponseType.AlreadyLocked);

        context.WriteIntent = null;
        
        return new(KeyValueResponseType.Unlocked);
    }
    
    /// <summary>
    /// Prepare the mutations made to the key currently held in the MVCC entry
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TryPrepareMutations(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            logger.LogWarning("Cannot prepare mutations for missing transaction id");
            
            return new(KeyValueResponseType.Errored);
        }

        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Consistency);
        if (context is null)
        {
            logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.WriteIntent is null)
        {
            logger.LogWarning("Write intent is missing for {TransactionId}", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.WriteIntent.TransactionId != message.TransactionId)
        {
            logger.LogWarning("Write intent conflict between {CurrentTransactionId} and {TransactionId}", context.WriteIntent.TransactionId, message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.MvccEntries is null)
        {
            logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [1]", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (!context.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? entry))
        {
            logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }
        
        if (message.Consistency != KeyValueConsistency.Linearizable)
            return new(KeyValueResponseType.Prepared);
        
        KeyValueProposal proposal = new(
            message.Key,
            entry.Value,
            entry.Revision,
            entry.Expires,
            entry.LastUsed,
            entry.State
        );

        (bool success, HLCTimestamp proposalTicket) = await PrepareKeyValueMessage(KeyValueRequestType.TrySet, proposal, message.TransactionId);

        if (!success)
        {
            logger.LogWarning("Failed to propose logs for {TransactionId}", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        return new(KeyValueResponseType.Prepared, proposalTicket);
    }
    
    /// <summary>
    /// Commit the mutations made to the key currently held in the MVCC entry
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TryCommitMutations(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            logger.LogWarning("Cannot commit mutations for missing transaction id");
            
            return new(KeyValueResponseType.Errored);
        }

        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Consistency);

        if (context is null)
        {
            logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.WriteIntent is null)
        {
            logger.LogWarning("Write intent is missing for {TransactionId}", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.WriteIntent.TransactionId != message.TransactionId)
        {
            logger.LogWarning("Write intent conflict between {CurrentTransactionId} and {TransactionId}", context.WriteIntent.TransactionId, message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.MvccEntries is null)
        {
            logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [1]", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (!context.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? entry))
        {
            logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        KeyValueProposal proposal = new(
            message.Key,
            entry.Value,
            entry.Revision,
            entry.Expires,
            entry.LastUsed,
            entry.State
        );

        if (message.Consistency != KeyValueConsistency.Linearizable)
        {
            context.Value = proposal.Value;
            context.Expires = proposal.Expires;
            context.Revision = proposal.Revision;
            context.LastUsed = proposal.LastUsed;
            context.State = proposal.State;
            
            context.MvccEntries.Remove(message.TransactionId);
            
            return new(KeyValueResponseType.Committed);
        }
        
        (bool success, long commitIndex) = await CommitKeyValueMessage(message.Key, message.ProposalTicketId);
        
        if (!success)
            return new(KeyValueResponseType.Errored);
        
        context.Value = proposal.Value;
        context.Expires = proposal.Expires;
        context.Revision = proposal.Revision;
        context.LastUsed = proposal.LastUsed;
        context.State = proposal.State;
        
        context.MvccEntries.Remove(message.TransactionId);
        
        return new(KeyValueResponseType.Committed, commitIndex);
    }
    
    /// <summary>
    /// Rollback made to the key currently held in the MVCC entry
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    private async Task<KeyValueResponse> TryRollbackMutations(KeyValueRequest message)
    {
        if (message.TransactionId == HLCTimestamp.Zero)
        {
            logger.LogWarning("Cannot rollback mutations for missing transaction id");
            
            return new(KeyValueResponseType.Errored);
        }

        KeyValueContext? context = await GetKeyValueContext(message.Key, message.Consistency);

        if (context is null)
        {
            logger.LogWarning("Key/Value context is missing for {TransactionId}", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.WriteIntent is null)
        {
            logger.LogWarning("Write intent is missing for {TransactionId}", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.WriteIntent.TransactionId != message.TransactionId)
        {
            logger.LogWarning("Write intent conflict between {CurrentTransactionId} and {TransactionId}", context.WriteIntent.TransactionId, message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (context.MvccEntries is null)
        {
            logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [1]", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (!context.MvccEntries.TryGetValue(message.TransactionId, out KeyValueMvccEntry? entry))
        {
            logger.LogWarning("Couldn't find MVCC entry for transaction {TransactionId} [2]", message.TransactionId);
            
            return new(KeyValueResponseType.Errored);
        }

        if (message.Consistency != KeyValueConsistency.Linearizable)
        {
            context.MvccEntries.Remove(message.TransactionId);
            
            return new(KeyValueResponseType.RolledBack);
        }
        
        (bool success, long rollbackIndex) = await RollbackKeyValueMessage(message.Key, message.ProposalTicketId);
        
        if (!success)
            return new(KeyValueResponseType.Errored);
        
        context.MvccEntries.Remove(message.TransactionId);
        
        return new(KeyValueResponseType.RolledBack, rollbackIndex);
    }

    /// <summary>
    /// Returns an existing KeyValueContext from memory or retrieves it from the persistence layer
    /// </summary>
    /// <param name="key"></param>
    /// <param name="consistency"></param>
    /// <returns></returns>
    private async ValueTask<KeyValueContext?> GetKeyValueContext(string key, KeyValueConsistency? consistency)
    {
        if (!keyValuesStore.TryGetValue(key, out KeyValueContext? context))
        {
            if (consistency == KeyValueConsistency.Linearizable)
            {
                context = await persistence.GetKeyValue(key);
                if (context is not null)
                {
                    context.LastUsed = await raft.HybridLogicalClock.SendOrLocalEvent();
                    keyValuesStore.Add(key, context);
                    return context;
                }
                
                return null;
            }
            
            return null;    
        }
        
        return context;
    }

    /// <summary>
    /// Persists and replicates KeyValue messages to the Raft partition
    /// </summary>
    /// <param name="type"></param>
    /// <param name="proposal"></param>
    /// <param name="currentTime"></param>
    /// <returns></returns>
    private async Task<bool> PersistAndReplicateKeyValueMessage(KeyValueRequestType type, KeyValueProposal proposal, HLCTimestamp currentTime)
    {
        if (!raft.Joined)
            return true;

        int partitionId = raft.GetPartitionKey(proposal.Key);

        KeyValueMessage kvm = new()
        {
            Type = (int)type,
            Key = proposal.Key,
            Revision = proposal.Revision,
            ExpireLogical = proposal.Expires.L,
            ExpireCounter = proposal.Expires.C,
            TimeLogical = currentTime.L,
            TimeCounter = currentTime.C,
            Consistency = (int)KeyValueConsistency.LinearizableReplication
        };
        
        if (proposal.Value is not null)
            kvm.Value = UnsafeByteOperations.UnsafeWrap(proposal.Value);

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.KeyValues,
            ReplicationSerializer.Serialize(kvm)
        );

        if (!result.Success)
        {
            logger.LogWarning("Failed to replicate key/value {Key} Partition={Partition} Status={Status}", proposal.Key, partitionId, result.Status);
            
            return false;
        }

        backgroundWriter.Send(new(
            BackgroundWriteType.QueueStoreLock,
            partitionId,
            proposal.Key,
            proposal.Value,
            proposal.Revision,
            proposal.Expires,
            (int)KeyValueConsistency.Linearizable,
            (int)proposal.State
        ));

        return result.Success;
    }
    
    /// <summary>
    /// Proposes a key value message to the partition
    /// </summary>
    /// <param name="type"></param>
    /// <param name="proposal"></param>
    /// <param name="currentTime"></param>
    /// <returns></returns>
    private async Task<(bool, HLCTimestamp)> PrepareKeyValueMessage(KeyValueRequestType type, KeyValueProposal proposal, HLCTimestamp currentTime)
    {
        if (!raft.Joined)
            return (true, HLCTimestamp.Zero);

        if (proposal.Key == "test")
            return (false, HLCTimestamp.Zero);

        int partitionId = raft.GetPartitionKey(proposal.Key);

        KeyValueMessage kvm = new()
        {
            Type = (int)type,
            Key = proposal.Key,
            Revision = proposal.Revision,
            ExpireLogical = proposal.Expires.L,
            ExpireCounter = proposal.Expires.C,
            TimeLogical = currentTime.L,
            TimeCounter = currentTime.C,
            Consistency = (int)KeyValueConsistency.LinearizableReplication
        };
        
        if (proposal.Value is not null)
            kvm.Value = UnsafeByteOperations.UnsafeWrap(proposal.Value);

        RaftReplicationResult result = await raft.ReplicateLogs(
            partitionId,
            ReplicationTypes.KeyValues,
            ReplicationSerializer.Serialize(kvm),
            autoCommit: false
        );

        if (!result.Success)
        {
            logger.LogWarning("Failed to propose key/value {Key} Partition={Partition} Status={Status}", proposal.Key, partitionId, result.Status);
            
            return (false, HLCTimestamp.Zero);
        }
        
        logger.LogDebug("Successfully proposed key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}", proposal.Key, partitionId, result.LogIndex);

        return (result.Success, result.TicketId);
    }
    
    /// <summary>
    /// Commits a previously proposed key value message
    /// </summary>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <returns></returns>
    private async Task<(bool, long)> CommitKeyValueMessage(string key, HLCTimestamp proposalTicketId)
    {
        if (!raft.Joined)
            return (true, 0);

        int partitionId = raft.GetPartitionKey(key);

        (bool success, RaftOperationStatus status, long commitLogId) = await raft.CommitLogs(
            partitionId,
            proposalTicketId
        );

        if (!success)
        {
            logger.LogWarning("Failed to commit key/value {Key} Partition={Partition} Status={Status}", key, partitionId, status);
            
            return (false, 0);
        }
        
        logger.LogDebug("Successfully commmitted key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}", key, partitionId, commitLogId);

        return (success, commitLogId);
    }
    
    /// <summary>
    /// Rollbacks a previously proposed key value message
    /// </summary>
    /// <param name="key"></param>
    /// <param name="proposalTicketId"></param>
    /// <returns></returns>
    private async Task<(bool, long)> RollbackKeyValueMessage(string key, HLCTimestamp proposalTicketId)
    {
        if (!raft.Joined)
            return (true, 0);

        int partitionId = raft.GetPartitionKey(key);

        (bool success, RaftOperationStatus status, long logIndex) = await raft.RollbackLogs(
            partitionId,
            proposalTicketId
        );

        if (!success)
        {
            logger.LogWarning("Failed to rollback key/value {Key} Partition={Partition} Status={Status}", key, partitionId, status);
            
            return (false, 0);
        }
        
        logger.LogDebug("Successfully rolled back key/value {Key} Partition={Partition} ProposalIndex={ProposalIndex}", key, partitionId, logIndex);

        return (success, logIndex);
    }

    private async ValueTask Collect()
    {
        if (keyValuesStore.Count < 2000)
            return;
        
        int number = 0;
        TimeSpan range = TimeSpan.FromMinutes(30);
        HLCTimestamp currentTime = await raft.HybridLogicalClock.SendOrLocalEvent();

        foreach (KeyValuePair<string, KeyValueContext> key in keyValuesStore)
        {
            if (key.Value.WriteIntent is not null)
                continue;
            
            if ((currentTime - key.Value.LastUsed) < range)
                continue;
            
            keysToEvict.Add(key.Key);
            number++;
            
            if (number > 100)
                break;
        }

        foreach (string key in keysToEvict)
        {
            keyValuesStore.Remove(key);
            
            Console.WriteLine("Removed {0}", key);
        }
        
        keysToEvict.Clear();
    }
}
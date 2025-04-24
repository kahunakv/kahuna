
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.InteropServices;
using Kommander.Time;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Kahuna.Communication.External.Grpc.KeyValues;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Diagnostics;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Provides gRPC services for managing key-value pairs, locks, and mutations.
/// Implements functionality for setting, extending, deleting, and retrieving
/// key-value pairs, as well as lock management and mutation operations.
/// </summary>
public sealed class KeyValuesService : KeyValuer.KeyValuerBase
{
    private readonly IKahuna keyValues;
    
    private readonly ILogger<IKahuna> logger;
    
    private readonly KeyValueClientBatcher clientBatcher;
    
    private readonly KeyValueServerBatcher serverBatcher;
    
    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="keyValues"></param>
    /// <param name="configuration"></param>
    /// <param name="raft"></param>
    /// <param name="logger"></param>
    public KeyValuesService(IKahuna keyValues, ILogger<IKahuna> logger)
    {
        this.keyValues = keyValues;
        this.logger = logger;
        
        clientBatcher = new(this, logger);
        serverBatcher = new(this, logger);
    }
    
    /// <summary>
    /// Receives requests the key/value "set" service
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTrySetKeyValueResponse> TrySetKeyValue(GrpcTrySetKeyValueRequest request, ServerCallContext context)
    {
        return await TrySetKeyValueInternal(request, context);
    }

    internal async Task<GrpcTrySetKeyValueResponse> TrySetKeyValueInternal(GrpcTrySetKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key) || request.ExpiresMs < 0)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        byte[]? value;
        
        if (MemoryMarshal.TryGetArray(request.Value.Memory, out ArraySegment<byte> segment))
            value = segment.Array;
        else
            value = request.Value.ToByteArray();
        
        byte[]? compareValue;
        
        if (MemoryMarshal.TryGetArray(request.CompareValue.Memory, out segment))
            compareValue = segment.Array;
        else
            compareValue = request.CompareValue.ToByteArray();
        
        (KeyValueResponseType response, long revision, HLCTimestamp lastModified) = await keyValues.LocateAndTrySetKeyValue(
            new(request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key, 
            value,
            compareValue,
            request.CompareRevision,
            (KeyValueFlags)request.Flags,
            request.ExpiresMs, 
            (KeyValueDurability)request.Durability,
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)response,
            Revision = revision,
            LastModifiedPhysical = lastModified.L,
            LastModifiedCounter = lastModified.C,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }
    
    /// <summary>
    /// Receives requests the key/value "extend" service
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryExtendKeyValueResponse> TryExtendKeyValue(GrpcTryExtendKeyValueRequest request, ServerCallContext context)
    {
        return await TryExtendKeyValueInternal(request, context);
    }

    internal async Task<GrpcTryExtendKeyValueResponse> TryExtendKeyValueInternal(GrpcTryExtendKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key) || request.ExpiresMs < 0)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await keyValues.LocateAndTryExtendKeyValue(
            new(request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key, 
            request.ExpiresMs,
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type,
            Revision = revision,
            LastModifiedPhysical = lastModified.L,
            LastModifiedCounter = lastModified.C,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }
    
    /// <summary>
    /// Receives requests for the key/value "delete" service
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryDeleteKeyValueResponse> TryDeleteKeyValue(GrpcTryDeleteKeyValueRequest request, ServerCallContext context)
    {
        return await TryDeleteKeyValueInternal(request, context);
    }

    internal async Task<GrpcTryDeleteKeyValueResponse> TryDeleteKeyValueInternal(GrpcTryDeleteKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await keyValues.LocateAndTryDeleteKeyValue(
            new(request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key, 
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type,
            Revision = revision,
            LastModifiedPhysical = lastModified.L,
            LastModifiedCounter = lastModified.C,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }
    
    /// <summary>
    /// Receives requests for the key/value "get" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryGetKeyValueResponse> TryGetKeyValue(GrpcTryGetKeyValueRequest request, ServerCallContext context)
    {
        return await TryGetKeyValueInternal(request, context);
    }
    
    internal async Task<GrpcTryGetKeyValueResponse> TryGetKeyValueInternal(GrpcTryGetKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? keyValueContext) = await keyValues.LocateAndTryGetValue(
            new(request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key, 
            request.Revision,
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );
        
        if (keyValueContext is not null)
        {
            GrpcTryGetKeyValueResponse response = new()
            {
                ServedFrom = "",
                Type = (GrpcKeyValueResponseType)type,
                Revision = keyValueContext.Revision,
                ExpiresPhysical = keyValueContext.Expires.L,
                ExpiresCounter = keyValueContext.Expires.C,
                TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
            };

            if (keyValueContext.Value is not null)
                response.Value = UnsafeByteOperations.UnsafeWrap(keyValueContext.Value);

            return response;
        }

        return new()
        {
            Type = (GrpcKeyValueResponseType)type,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }
    
    /// <summary>
    /// Receives requests for the key/value "exists" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryExistsKeyValueResponse> TryExistsKeyValue(GrpcTryExistsKeyValueRequest request, ServerCallContext context)
    {
        return await TryExistsKeyValueInternal(request, context);
    }

    internal async Task<GrpcTryExistsKeyValueResponse> TryExistsKeyValueInternal(GrpcTryExistsKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? keyValueContext) = await keyValues.LocateAndTryExistsValue(
            new(request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key, 
            request.Revision,
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );
        
        if (keyValueContext is not null)
        {
            GrpcTryExistsKeyValueResponse response = new()
            {
                ServedFrom = "",
                Type = (GrpcKeyValueResponseType)type,
                Revision = keyValueContext.Revision,
                ExpiresPhysical = keyValueContext.Expires.L,
                ExpiresCounter = keyValueContext.Expires.C,
                TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
            };
            
            return response;
        }

        return new()
        {
            Type = (GrpcKeyValueResponseType)type,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }
    
    /// <summary>
    /// Receives requests for the key/value "AcquireExclusiveLock" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryAcquireExclusiveLockResponse> TryAcquireExclusiveLock(GrpcTryAcquireExclusiveLockRequest request, ServerCallContext context)
    {
        return await TryAcquireExclusiveLockInternal(request, context);
    }
    
    internal async Task<GrpcTryAcquireExclusiveLockResponse> TryAcquireExclusiveLockInternal(GrpcTryAcquireExclusiveLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        (KeyValueResponseType type, _, _) = await keyValues.LocateAndTryAcquireExclusiveLock(
            new(request.TransactionIdPhysical, request.TransactionIdCounter), 
            request.Key, 
            request.ExpiresMs, 
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type
        };
    }
    
    /// <summary>
    /// Receives requests for the key/value "AcquireManyExclusiveLocks" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryAcquireManyExclusiveLocksResponse> TryAcquireManyExclusiveLocks(GrpcTryAcquireManyExclusiveLocksRequest request, ServerCallContext context)
    {
        return await TryAcquireManyExclusiveLocksInternal(request, context);
    }

    internal async Task<GrpcTryAcquireManyExclusiveLocksResponse> TryAcquireManyExclusiveLocksInternal(GrpcTryAcquireManyExclusiveLocksRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = await keyValues.LocateAndTryAcquireManyExclusiveLocks(
            new(request.TransactionIdPhysical, request.TransactionIdCounter), 
            GetRequestLocksItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryAcquireManyExclusiveLocksResponse response = new();
        
        response.Items.Add(GetResponseLocksItems(responses));

        return response;
    }

    private static List<(string key, int expiresMs, KeyValueDurability durability)> GetRequestLocksItems(RepeatedField<GrpcTryAcquireManyExclusiveLocksRequestItem> items)
    {
        List<(string key, int expiresMs, KeyValueDurability durability)> rItems = new(items.Count);
        
        foreach (GrpcTryAcquireManyExclusiveLocksRequestItem item in items)
            rItems.Add((item.Key, item.ExpiresMs, (KeyValueDurability)item.Durability));
        
        return rItems;
    }
    
    private static IEnumerable<GrpcTryAcquireManyExclusiveLocksResponseItem> GetResponseLocksItems(List<(KeyValueResponseType, string, KeyValueDurability)> responses)
    {
        foreach ((KeyValueResponseType response, string key, KeyValueDurability durability) in responses)
            yield return new()
            {
                Type = (GrpcKeyValueResponseType)response,
                Key = key,
                Durability = (GrpcKeyValueDurability)durability
            };
    }

    /// <summary>
    /// Receives requests for the key/value "ReleaseExclusiveLock" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryReleaseExclusiveLockResponse> TryReleaseExclusiveLock(GrpcTryReleaseExclusiveLockRequest request, ServerCallContext context)
    {
        return await TryReleaseExclusiveLockInternal(request, context);   
    }

    internal async Task<GrpcTryReleaseExclusiveLockResponse> TryReleaseExclusiveLockInternal(GrpcTryReleaseExclusiveLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        (KeyValueResponseType type, string _) = await keyValues.LocateAndTryReleaseExclusiveLock(
            new(request.TransactionIdPhysical, request.TransactionIdCounter), 
            request.Key, 
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type
        };
    }
    
    /// <summary>
    /// Receives requests for the key/value "ReleaseManyExclusiveLocks" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryReleaseManyExclusiveLocksResponse> TryReleaseManyExclusiveLocks(GrpcTryReleaseManyExclusiveLocksRequest request, ServerCallContext context)
    {
        return await TryReleaseManyExclusiveLocksInternal(request, context);
    }

    internal async Task<GrpcTryReleaseManyExclusiveLocksResponse> TryReleaseManyExclusiveLocksInternal(GrpcTryReleaseManyExclusiveLocksRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = await keyValues.LocateAndTryReleaseManyExclusiveLocks(
            new(request.TransactionIdPhysical, request.TransactionIdCounter), 
            GetRequestReleaseItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryReleaseManyExclusiveLocksResponse response = new();
        
        response.Items.Add(GetResponseReleaseItems(responses));

        return response;
    }

    private static List<(string key, KeyValueDurability durability)> GetRequestReleaseItems(RepeatedField<GrpcTryReleaseManyExclusiveLocksRequestItem> items)
    {
        List<(string key, KeyValueDurability durability)> rItems = new(items.Count);
        
        foreach (GrpcTryReleaseManyExclusiveLocksRequestItem item in items)
            rItems.Add((item.Key, (KeyValueDurability)item.Durability));
        
        return rItems;
    }
    
    private static IEnumerable<GrpcTryReleaseManyExclusiveLocksResponseItem> GetResponseReleaseItems(List<(KeyValueResponseType, string, KeyValueDurability)> responses)
    {
        foreach ((KeyValueResponseType response, string key, KeyValueDurability durability) in responses)
            yield return new()
            {
                Type = (GrpcKeyValueResponseType)response,
                Key = key,
                Durability = (GrpcKeyValueDurability)durability
            };
    }
    
    /// <summary>
    /// Receives requests for the key/value "PrepareMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryPrepareMutationsResponse> TryPrepareMutations(GrpcTryPrepareMutationsRequest request, ServerCallContext context)
    {
        return await TryPrepareMutationsInternal(request, context); 
    }
    
    internal async Task<GrpcTryPrepareMutationsResponse> TryPrepareMutationsInternal(GrpcTryPrepareMutationsRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        (KeyValueResponseType type, HLCTimestamp proposalTicket, _, _) = await keyValues.LocateAndTryPrepareMutations(
            new(request.TransactionIdPhysical, request.TransactionIdCounter),
            new(request.CommitIdPhysical, request.CommitIdCounter),
            request.Key, 
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type,
            ProposalTicketPhysical = proposalTicket.L,
            ProposalTicketCounter = proposalTicket.C
        };
    }
    
    /// <summary>
    /// Receives requests for the key/value "PrepareManyMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryPrepareManyMutationsResponse> TryPrepareManyMutations(GrpcTryPrepareManyMutationsRequest request, ServerCallContext context)
    {
        return await TryPrepareManyMutationsInternal(request, context);
    }

    internal async Task<GrpcTryPrepareManyMutationsResponse> TryPrepareManyMutationsInternal(GrpcTryPrepareManyMutationsRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> responses = await keyValues.LocateAndTryPrepareManyMutations(
            new(request.TransactionIdPhysical, request.TransactionIdCounter),
            new(request.CommitIdPhysical, request.CommitIdCounter),
            GetRequestPrepareItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryPrepareManyMutationsResponse response = new();
        
        response.Items.Add(GetResponsePrepareItems(responses));

        return response;
    }

    private static List<(string key, KeyValueDurability durability)> GetRequestPrepareItems(RepeatedField<GrpcTryPrepareManyMutationsRequestItem> requestItems)
    {
        List<(string key, KeyValueDurability durability)> rItems = new(requestItems.Count);
        
        foreach (GrpcTryPrepareManyMutationsRequestItem item in requestItems)
            rItems.Add((item.Key, (KeyValueDurability)item.Durability));

        return rItems;
    }
    
    private static IEnumerable<GrpcTryPrepareManyMutationsResponseItem> GetResponsePrepareItems(List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> responses)
    {
        foreach ((KeyValueResponseType type, HLCTimestamp ticketId, string key, KeyValueDurability durability) in responses)
            yield return new()
            {
                Type = (GrpcKeyValueResponseType)type,
                ProposalTicketPhysical = ticketId.L, 
                ProposalTicketCounter = ticketId.C, 
                Key = key,
                Durability = (GrpcKeyValueDurability)durability
            };
    }

    /// <summary>
    /// Receives requests for the key/value "CommitMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryCommitMutationsResponse> TryCommitMutations(GrpcTryCommitMutationsRequest request, ServerCallContext context)
    {
        return await TryCommitMutationsInternal(request, context);
    }
    
    internal async Task<GrpcTryCommitMutationsResponse> TryCommitMutationsInternal(GrpcTryCommitMutationsRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        (KeyValueResponseType type, long commitIndex) = await keyValues.LocateAndTryCommitMutations(
            new(request.TransactionIdPhysical, request.TransactionIdCounter), 
            request.Key, 
            new(request.ProposalTicketPhysical, request.ProposalTicketCounter),
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type,
            ProposalIndex = commitIndex
        };
    }
    
    /// <summary>
    /// Receives requests for the key/value "CommitManyMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryCommitManyMutationsResponse> TryCommitManyMutations(GrpcTryCommitManyMutationsRequest request, ServerCallContext context)
    {
        return await TryCommitManyMutationsInternal(request, context);
    }
    
    /// <summary>
    /// Receives requests for the key/value "CommitManyMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    internal async Task<GrpcTryCommitManyMutationsResponse> TryCommitManyMutationsInternal(GrpcTryCommitManyMutationsRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> responses = await keyValues.LocateAndTryCommitManyMutations(
            new(request.TransactionIdPhysical, request.TransactionIdCounter), 
            GetRequestCommitItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryCommitManyMutationsResponse response = new();
        
        response.Items.Add(GetResponseCommitItems(responses));

        return response;
    }

    private static List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> GetRequestCommitItems(RepeatedField<GrpcTryCommitManyMutationsRequestItem> requestItems)
    {
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> rItems = new(requestItems.Count);
        
        foreach (GrpcTryCommitManyMutationsRequestItem item in requestItems)
            rItems.Add((item.Key, new(item.ProposalTicketPhysical, item.ProposalTicketCounter), (KeyValueDurability)item.Durability));

        return rItems;
    }
    
    private static IEnumerable<GrpcTryCommitManyMutationsResponseItem> GetResponseCommitItems(List<(KeyValueResponseType, string, long, KeyValueDurability)> responses)
    {
        foreach ((KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability) in responses)
            yield return new()
            {
                Type = (GrpcKeyValueResponseType)type,
                Key = key,
                ProposalIndex = proposalIndex,
                Durability = (GrpcKeyValueDurability)durability
            };
    }
    
    /// <summary>
    /// Receives requests for the key/value "RollbackMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryRollbackMutationsResponse> TryRollbackMutations(GrpcTryRollbackMutationsRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        (KeyValueResponseType type, long rollbackIndex) = await keyValues.LocateAndTryRollbackMutations(
            new(request.TransactionIdPhysical, request.TransactionIdCounter), 
            request.Key, 
            new(request.ProposalTicketPhysical, request.ProposalTicketCounter),
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type,
            ProposalIndex = rollbackIndex
        };
    }

    /// <summary>
    /// Executes a transaction script on the key/value store
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryExecuteTransactionScriptResponse> TryExecuteTransactionScript(GrpcTryExecuteTransactionScriptRequest request, ServerCallContext context)
    {
        return await TryExecuteTransactionScriptInternal(request, context);
    }

    internal async Task<GrpcTryExecuteTransactionScriptResponse> TryExecuteTransactionScriptInternal(GrpcTryExecuteTransactionScriptRequest request, ServerCallContext context)
    {
        if (request.Script is null)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
            
        KeyValueTransactionResult result = await keyValues.TryExecuteTransactionScript(
            request.Script.ToByteArray(), 
            request.Hash, 
            GetParameters(request.Parameters)
        );

        GrpcTryExecuteTransactionScriptResponse response = new()
        {
            Type = (GrpcKeyValueResponseType) result.Type,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
                                  
        if (result.ServedFrom is not null)
            response.ServedFrom = result.ServedFrom;
        
        if (result.Reason is not null)
            response.Reason = result.Reason;
        
        List<GrpcTryExecuteTransactionResponseValue> values = new(result.Values?.Count ?? 0);

        if (result.Values is not null)
        {
            foreach (KeyValueTransactionResultValue value in result.Values)
            {
                GrpcTryExecuteTransactionResponseValue responseValue = new()
                {
                    Revision = value.Revision,
                    ExpiresPhysical = value.Expires.L,
                    ExpiresCounter = value.Expires.C,
                    LastModifiedPhysical = value.LastModified.L,
                    LastModifiedCounter = value.LastModified.C
                };
                
                if (value.Key is not null)
                    responseValue.Key = value.Key;
                
                if (value.Value is not null)
                    responseValue.Value = UnsafeByteOperations.UnsafeWrap(value.Value);
                
                values.Add(responseValue);
            }
        }
        
        response.Values.AddRange(values);
        
        return response;
    }
    
    /// <summary>
    /// Returns key/value pairs by prefix
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcScanByPrefixResponse> ScanByPrefix(GrpcScanByPrefixRequest request, ServerCallContext context)
    {
        return await ScanByPrefixInternal(request, context);
    }

    internal async Task<GrpcScanByPrefixResponse> ScanByPrefixInternal(GrpcScanByPrefixRequest request, ServerCallContext context)
    {
        if (request.PrefixKey is null)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
            
        KeyValueGetByPrefixResult result = await keyValues.ScanByPrefix(request.PrefixKey, (KeyValueDurability) request.Durability);

        GrpcScanByPrefixResponse response = new()
        {
            Type = GrpcKeyValueResponseType.TypeGot,
        };
        
        response.Items.Add(GetKeyValueItems(result.Items));
        
        return response;
    }
    
    /// <summary>
    /// Returns key/value pairs by prefix
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcGetByPrefixResponse> GetByPrefix(GrpcGetByPrefixRequest request, ServerCallContext context)
    {
        return await GetByPrefixInternal(request, context);
    }

    internal async Task<GrpcGetByPrefixResponse> GetByPrefixInternal(GrpcGetByPrefixRequest request, ServerCallContext context)
    {
        if (request.PrefixKey is null)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
            
        KeyValueGetByPrefixResult result = await keyValues.LocateAndGetByPrefix(
            new(request.TransactionIdPhysical, request.TransactionIdCounter), 
            request.PrefixKey, 
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        GrpcGetByPrefixResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)result.Type
        };
        
        response.Items.Add(GetKeyValueItems(result.Items));
        
        return response;
    }
    
    /// <summary>
    /// Scans all nodes in the cluster and returns key/value pairs by prefix
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcScanAllByPrefixResponse> ScanAllByPrefix(GrpcScanAllByPrefixRequest request, ServerCallContext context)
    {
        return await ScanAllByPrefixInternal(request, context);
    }

    internal async Task<GrpcScanAllByPrefixResponse> ScanAllByPrefixInternal(GrpcScanAllByPrefixRequest request, ServerCallContext context)
    {
        if (request.PrefixKey is null)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
            
        KeyValueGetByPrefixResult result = await keyValues.ScanAllByPrefix(request.PrefixKey, (KeyValueDurability) request.Durability);

        GrpcScanAllByPrefixResponse response = new()
        {
            Type = GrpcKeyValueResponseType.TypeGot,
        };
        
        if (result.Items.Count > 0)
            response.Items.Add(GetKeyValueItems(result.Items));
        
        return response;
    }

    private static IEnumerable<GrpcKeyValueByPrefixItemResponse> GetKeyValueItems(List<(string, ReadOnlyKeyValueContext)> resultItems)
    {
        foreach ((string key, ReadOnlyKeyValueContext context) in resultItems)
        {
            GrpcKeyValueByPrefixItemResponse response = new()
            {
                Key = key,
                Revision = context.Revision,
                ExpiresPhysical = context.Expires.L,
                ExpiresCounter = context.Expires.C,
            };
            
            if (context.Value is not null)
                response.Value = UnsafeByteOperations.UnsafeWrap(context.Value);
            
            yield return response;
        }
    }

    private static List<KeyValueParameter> GetParameters(RepeatedField<GrpcKeyValueParameter> requestParameters)
    {
        List<KeyValueParameter> parameters = new(requestParameters.Count);
        
        foreach (GrpcKeyValueParameter? parameter in requestParameters)
            parameters.Add(new() { Key = parameter.Key, Value = parameter.Value });
        
        return parameters;
    }

    public override async Task BatchClientKeyValueRequests(
        IAsyncStreamReader<GrpcBatchClientKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        await clientBatcher.BatchClientKeyValueRequests(requestStream, responseStream, context);
    }

    public override async Task BatchServerKeyValueRequests(
        IAsyncStreamReader<GrpcBatchServerKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        await serverBatcher.BatchServerKeyValueRequests(requestStream, responseStream, context);
    }
}
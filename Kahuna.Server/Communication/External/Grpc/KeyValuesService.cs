
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
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Diagnostics;

namespace Kahuna.Communication.External.Grpc;

public sealed class KeyValuesService : KeyValuer.KeyValuerBase
{
    private readonly IKahuna keyValues;
    
    private readonly ILogger<IKahuna> logger;
    
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

    private async Task<GrpcTrySetKeyValueResponse> TrySetKeyValueInternal(GrpcTrySetKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        if (request.ExpiresMs < 0)
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

    private async Task<GrpcTryExtendKeyValueResponse> TryExtendKeyValueInternal(GrpcTryExtendKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        if (request.ExpiresMs < 0)
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

    private async Task<GrpcTryDeleteKeyValueResponse> TryDeleteKeyValueInternal(GrpcTryDeleteKeyValueRequest request, ServerCallContext context)
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
    
    private async Task<GrpcTryGetKeyValueResponse> TryGetKeyValueInternal(GrpcTryGetKeyValueRequest request, ServerCallContext context)
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

    private async Task<GrpcTryExistsKeyValueResponse> TryExistsKeyValueInternal(GrpcTryExistsKeyValueRequest request, ServerCallContext context)
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
    
    private async Task<GrpcTryAcquireExclusiveLockResponse> TryAcquireExclusiveLockInternal(GrpcTryAcquireExclusiveLockRequest request, ServerCallContext context)
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

    private async Task<GrpcTryAcquireManyExclusiveLocksResponse> TryAcquireManyExclusiveLocksInternal(GrpcTryAcquireManyExclusiveLocksRequest request, ServerCallContext context)
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

    private async Task<GrpcTryReleaseExclusiveLockResponse> TryReleaseExclusiveLockInternal(GrpcTryReleaseExclusiveLockRequest request, ServerCallContext context)
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

    private async Task<GrpcTryReleaseManyExclusiveLocksResponse> TryReleaseManyExclusiveLocksInternal(GrpcTryReleaseManyExclusiveLocksRequest request, ServerCallContext context)
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
    
    private async Task<GrpcTryPrepareMutationsResponse> TryPrepareMutationsInternal(GrpcTryPrepareMutationsRequest request, ServerCallContext context)
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

    private async Task<GrpcTryPrepareManyMutationsResponse> TryPrepareManyMutationsInternal(GrpcTryPrepareManyMutationsRequest request, ServerCallContext context)
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
    
    private async Task<GrpcTryCommitMutationsResponse> TryCommitMutationsInternal(GrpcTryCommitMutationsRequest request, ServerCallContext context)
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
    private async Task<GrpcTryCommitManyMutationsResponse> TryCommitManyMutationsInternal(GrpcTryCommitManyMutationsRequest request, ServerCallContext context)
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

    private async Task<GrpcTryExecuteTransactionScriptResponse> TryExecuteTransactionScriptInternal(GrpcTryExecuteTransactionScriptRequest request, ServerCallContext context)
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
        try
        {
            List<Task> tasks = [];

            using SemaphoreSlim semaphore = new(1, 1);

            await foreach (GrpcBatchClientKeyValueRequest request in requestStream.ReadAllAsync())
            {
                switch (request.Type)
                {
                    case GrpcClientBatchType.TrySetKeyValue:
                    {
                        GrpcTrySetKeyValueRequest? setKeyRequest = request.TrySetKeyValue;

                        tasks.Add(TrySetKeyValueDelayed(semaphore, request.RequestId, setKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryGetKeyValue:
                    {
                        GrpcTryGetKeyValueRequest? getKeyRequest = request.TryGetKeyValue;

                        tasks.Add(TryGetKeyValueDelayed(semaphore, request.RequestId, getKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryDeleteKeyValue:
                    {
                        GrpcTryDeleteKeyValueRequest? deleteKeyRequest = request.TryDeleteKeyValue;

                        tasks.Add(TryDeleteKeyValueDelayed(semaphore, request.RequestId, deleteKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryExtendKeyValue:
                    {
                        GrpcTryExtendKeyValueRequest? extendKeyRequest = request.TryExtendKeyValue;

                        tasks.Add(TryExtendKeyValueDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TryExistsKeyValue:
                    {
                        GrpcTryExistsKeyValueRequest? extendKeyRequest = request.TryExistsKeyValue;

                        tasks.Add(TryExistsKeyValueDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcClientBatchType.TryExecuteTransactionScript:
                    {
                        GrpcTryExecuteTransactionScriptRequest? tryExecuteTransactionScriptRequest = request.TryExecuteTransactionScript;

                        tasks.Add(TryExecuteTransactionScriptDelayed(semaphore, request.RequestId, tryExecuteTransactionScriptRequest, responseStream, context));
                    }
                    break;

                    case GrpcClientBatchType.TypeNone:
                    default:
                        logger.LogError("Unknown batch client request type: {Type}", request.Type);
                        break;
                }
            }

            await Task.WhenAll(tasks);
        }
        catch (IOException ex)
        {
            logger.LogDebug("IOException: {Message}", ex.Message);
        }
    }

    private async Task TrySetKeyValueDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTrySetKeyValueRequest setKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTrySetKeyValueResponse trySetResponse = await TrySetKeyValueInternal(setKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TrySetKeyValue,
            RequestId = requestId,
            TrySetKeyValue = trySetResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryGetKeyValueDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryGetKeyValueRequest getKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryGetKeyValueResponse tryGetResponse = await TryGetKeyValueInternal(getKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryGetKeyValue,
            RequestId = requestId,
            TryGetKeyValue = tryGetResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryDeleteKeyValueDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryDeleteKeyValueRequest deleteKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryDeleteKeyValueResponse tryDeleteResponse = await TryDeleteKeyValueInternal(deleteKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryDeleteKeyValue,
            RequestId = requestId,
            TryDeleteKeyValue = tryDeleteResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryExtendKeyValueDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryExtendKeyValueRequest extendKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExtendKeyValueResponse tryExtendResponse = await TryExtendKeyValueInternal(extendKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryExtendKeyValue,
            RequestId = requestId,
            TryExtendKeyValue = tryExtendResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryExistsKeyValueDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryExistsKeyValueRequest existKeyRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExistsKeyValueResponse tryExistsResponse = await TryExistsKeyValueInternal(existKeyRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryExistsKeyValue,
            RequestId = requestId,
            TryExistsKeyValue = tryExistsResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryExecuteTransactionScriptDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryExecuteTransactionScriptRequest tryExecuteTransactionRequest, 
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExecuteTransactionScriptResponse tryExecuteTransactionScriptResponse = await TryExecuteTransactionScriptInternal(tryExecuteTransactionRequest, context);
        
        GrpcBatchClientKeyValueResponse response = new()
        {
            Type = GrpcClientBatchType.TryExecuteTransactionScript,
            RequestId = requestId,
            TryExecuteTransactionScript = tryExecuteTransactionScriptResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
               
    }
    
    public override async Task BatchServerKeyValueRequests(
        IAsyncStreamReader<GrpcBatchServerKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream, 
        ServerCallContext context
    )
    {
        try
        {
            List<Task> tasks = [];

            using SemaphoreSlim semaphore = new(1, 1);

            await foreach (GrpcBatchServerKeyValueRequest request in requestStream.ReadAllAsync())
            {
                switch (request.Type)
                {
                    case GrpcServerBatchType.ServerTrySetKeyValue:
                    {
                        GrpcTrySetKeyValueRequest? setKeyRequest = request.TrySetKeyValue;

                        tasks.Add(TrySetKeyValueServerDelayed(semaphore, request.RequestId, setKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryGetKeyValue:
                    {
                        GrpcTryGetKeyValueRequest? getKeyRequest = request.TryGetKeyValue;

                        tasks.Add(TryGetKeyValueServerDelayed(semaphore, request.RequestId, getKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryDeleteKeyValue:
                    {
                        GrpcTryDeleteKeyValueRequest? deleteKeyRequest = request.TryDeleteKeyValue;

                        tasks.Add(TryDeleteKeyValueServerDelayed(semaphore, request.RequestId, deleteKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryExtendKeyValue:
                    {
                        GrpcTryExtendKeyValueRequest? extendKeyRequest = request.TryExtendKeyValue;

                        tasks.Add(TryExtendKeyValueServerDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;

                    case GrpcServerBatchType.ServerTryExistsKeyValue:
                    {
                        GrpcTryExistsKeyValueRequest? extendKeyRequest = request.TryExistsKeyValue;

                        tasks.Add(TryExistsKeyValueServerDelayed(semaphore, request.RequestId, extendKeyRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryExecuteTransactionScript:
                    {
                        GrpcTryExecuteTransactionScriptRequest? tryExecuteTransactionScriptRequest = request.TryExecuteTransactionScript;

                        tasks.Add(TryExecuteTransactionServerDelayed(semaphore, request.RequestId, tryExecuteTransactionScriptRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryAcquireExclusiveLock:
                    {
                        GrpcTryAcquireExclusiveLockRequest? tryAcquireExclusiveLockRequest = request.TryAcquireExclusiveLock;

                        tasks.Add(TryAcquireExclusiveLockDelayed(semaphore, request.RequestId, tryAcquireExclusiveLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks:
                    {
                        GrpcTryAcquireManyExclusiveLocksRequest? tryAcquireManyExclusiveLocksRequest = request.TryAcquireManyExclusiveLocks;

                        tasks.Add(TryAcquireManyExclusiveLocksDelayed(semaphore, request.RequestId, tryAcquireManyExclusiveLocksRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryReleaseExclusiveLock:
                    {
                        GrpcTryReleaseExclusiveLockRequest? tryReleaseExclusiveLockRequest = request.TryReleaseExclusiveLock;

                        tasks.Add(TryReleaseExclusiveLockDelayed(semaphore, request.RequestId, tryReleaseExclusiveLockRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks:
                    {
                        GrpcTryReleaseManyExclusiveLocksRequest? tryReleaseManyExclusiveLocksRequest = request.TryReleaseManyExclusiveLocks;

                        tasks.Add(TryReleaseManyExclusiveLocksDelayed(semaphore, request.RequestId, tryReleaseManyExclusiveLocksRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryPrepareMutations:
                    {
                        GrpcTryPrepareMutationsRequest? tryPrepareMutationsRequest = request.TryPrepareMutations;

                        tasks.Add(TryPrepareMutationsDelayed(semaphore, request.RequestId, tryPrepareMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryPrepareManyMutations:
                    {
                        GrpcTryPrepareManyMutationsRequest? tryPrepareManyMutationsRequest = request.TryPrepareManyMutations;

                        tasks.Add(TryPrepareManyMutationsDelayed(semaphore, request.RequestId, tryPrepareManyMutationsRequest, responseStream, context));
                    }
                    break;
                    
                    case GrpcServerBatchType.ServerTryCommitMutations:
                    {
                        GrpcTryCommitMutationsRequest? tryCommitMutationsRequest = request.TryCommitMutations;

                        tasks.Add(TryCommitMutationsDelayed(semaphore, request.RequestId, tryCommitMutationsRequest, responseStream, context));
                    }
                        break;
                    
                    case GrpcServerBatchType.ServerTryCommitManyMutations:
                    {
                        GrpcTryCommitManyMutationsRequest? tryCommitManyMutationsRequest = request.TryCommitManyMutations;

                        tasks.Add(TryCommitManyMutationsDelayed(semaphore, request.RequestId, tryCommitManyMutationsRequest, responseStream, context));
                    }
                        break;

                    case GrpcServerBatchType.ServerTypeNone:
                    default:                                                
                        logger.LogError("Unknown batch Server request type: {Type}", request.Type);
                        break;
                }
            }

            await Task.WhenAll(tasks);
        }
        catch (IOException ex)
        {
            logger.LogDebug("IOException: {Message}", ex.Message);
        }
    }

    private async Task TrySetKeyValueServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTrySetKeyValueRequest setKeyRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTrySetKeyValueResponse trySetResponse = await TrySetKeyValueInternal(setKeyRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTrySetKeyValue,
            RequestId = requestId,
            TrySetKeyValue = trySetResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task TryGetKeyValueServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryGetKeyValueRequest getKeyRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryGetKeyValueResponse tryGetResponse = await TryGetKeyValueInternal(getKeyRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryGetKeyValue,
            RequestId = requestId,
            TryGetKeyValue = tryGetResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task TryDeleteKeyValueServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryDeleteKeyValueRequest deleteKeyRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryDeleteKeyValueResponse tryDeleteResponse = await TryDeleteKeyValueInternal(deleteKeyRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryDeleteKeyValue,
            RequestId = requestId,
            TryDeleteKeyValue = tryDeleteResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task TryExtendKeyValueServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryExtendKeyValueRequest extendKeyRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExtendKeyValueResponse tryExtendResponse = await TryExtendKeyValueInternal(extendKeyRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryExtendKeyValue,
            RequestId = requestId,
            TryExtendKeyValue = tryExtendResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task TryExistsKeyValueServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryExistsKeyValueRequest existKeyRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExistsKeyValueResponse tryExistsResponse = await TryExistsKeyValueInternal(existKeyRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryExistsKeyValue,
            RequestId = requestId,
            TryExistsKeyValue = tryExistsResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private async Task TryExecuteTransactionServerDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryExecuteTransactionScriptRequest tryExecuteTransactionRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryExecuteTransactionScriptResponse tryExecuteTransactionScriptResponse = await TryExecuteTransactionScriptInternal(tryExecuteTransactionRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryExecuteTransactionScript,
            RequestId = requestId,
            TryExecuteTransactionScript = tryExecuteTransactionScriptResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryAcquireExclusiveLockDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryAcquireExclusiveLockRequest tryExecuteTransactionRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryAcquireExclusiveLockResponse tryExecuteTransactionResponse = await TryAcquireExclusiveLockInternal(tryExecuteTransactionRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryAcquireExclusiveLock,
            RequestId = requestId,
            TryAcquireExclusiveLock = tryExecuteTransactionResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryAcquireManyExclusiveLocksDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryAcquireManyExclusiveLocksRequest tryAcquireManyExclusiveLocksnRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryAcquireManyExclusiveLocksResponse tryAcquireManyExclusiveLocksResponse = await TryAcquireManyExclusiveLocksInternal(tryAcquireManyExclusiveLocksnRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryAcquireManyExclusiveLocks,
            RequestId = requestId,
            TryAcquireManyExclusiveLocks = tryAcquireManyExclusiveLocksResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryReleaseExclusiveLockDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryReleaseExclusiveLockRequest tryReleaseExclusiveLockRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryReleaseExclusiveLockResponse tryReleaseExclusiveLockResponse = await TryReleaseExclusiveLockInternal(tryReleaseExclusiveLockRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryReleaseExclusiveLock,
            RequestId = requestId,
            TryReleaseExclusiveLock = tryReleaseExclusiveLockResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryReleaseManyExclusiveLocksDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryReleaseManyExclusiveLocksRequest tryReleaseManyExclusiveLocksnRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryReleaseManyExclusiveLocksResponse tryReleaseManyExclusiveLocksResponse = await TryReleaseManyExclusiveLocksInternal(tryReleaseManyExclusiveLocksnRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryReleaseManyExclusiveLocks,
            RequestId = requestId,
            TryReleaseManyExclusiveLocks = tryReleaseManyExclusiveLocksResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryPrepareMutationsDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryPrepareMutationsRequest tryPrepareMutationsRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryPrepareMutationsResponse tryPrepareMutationsResponse = await TryPrepareMutationsInternal(tryPrepareMutationsRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryPrepareMutations,
            RequestId = requestId,
            TryPrepareMutations = tryPrepareMutationsResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryPrepareManyMutationsDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryPrepareManyMutationsRequest tryPrepareManyMutationsRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryPrepareManyMutationsResponse tryPrepareManyMutationsResponse = await TryPrepareManyMutationsInternal(tryPrepareManyMutationsRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryPrepareManyMutations,
            RequestId = requestId,
            TryPrepareManyMutations = tryPrepareManyMutationsResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryCommitMutationsDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryCommitMutationsRequest tryCommitMutationsRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryCommitMutationsResponse tryCommitMutationsResponse = await TryCommitMutationsInternal(tryCommitMutationsRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryCommitMutations,
            RequestId = requestId,
            TryCommitMutations = tryCommitMutationsResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
    
    private async Task TryCommitManyMutationsDelayed(
        SemaphoreSlim semaphore, 
        int requestId, 
        GrpcTryCommitManyMutationsRequest tryPrepareManyMutationsRequest, 
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        GrpcTryCommitManyMutationsResponse tryCommitManyMutationsResponse = await TryCommitManyMutationsInternal(tryPrepareManyMutationsRequest, context);
        
        GrpcBatchServerKeyValueResponse response = new()
        {
            Type = GrpcServerBatchType.ServerTryCommitManyMutations,
            RequestId = requestId,
            TryCommitManyMutations = tryCommitManyMutationsResponse
        };

        try
        {
            await semaphore.WaitAsync(context.CancellationToken);

            await responseStream.WriteAsync(response);
        }
        finally
        {
            semaphore.Release();
        }
    }
}
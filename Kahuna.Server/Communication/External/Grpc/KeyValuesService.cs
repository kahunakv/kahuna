
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
using Kahuna.Shared.Communication.Rest;
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
    /// Attempts to set a key-value pair.
    /// </summary>
    /// <param name="request">The request containing the key-value data to be set.</param>
    /// <param name="context">The server call context providing information about the call.</param>
    /// <returns>A task representing the asynchronous operation, with a response indicating the result of the set operation.</returns>
    public override async Task<GrpcTrySetKeyValueResponse> TrySetKeyValue(GrpcTrySetKeyValueRequest request, ServerCallContext context)
    {
        return await TrySetKeyValueInternal(request, context);
    }

    /// <summary>
    /// Attempts to set a key-value pair within the key-value store with additional parameters such as expiration, flags, and durability.
    /// </summary>
    /// <param name="request">The request containing the key, value, comparison value, flags, expiration time, durability level, and transaction details.</param>
    /// <param name="context">The server call context associated with the operation, including cancellation tokens and call metadata.</param>
    /// <returns>A response indicating the result of the operation, including the response type, revision number, last modified timestamp, and elapsed time in milliseconds.</returns>
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
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
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
            LastModifiedNode = lastModified.N,
            LastModifiedPhysical = lastModified.L,
            LastModifiedCounter = lastModified.C,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }
    
    /// <summary>
    /// Attempts to set many key-value pairs.
    /// </summary>
    /// <param name="request">The request containing the key-value data to be set.</param>
    /// <param name="context">The server call context providing information about the call.</param>
    /// <returns>A task representing the asynchronous operation, with a response indicating the result of the set operation.</returns>
    public override async Task<GrpcTrySetManyKeyValueResponse> TrySetManyKeyValue(GrpcTrySetManyKeyValueRequest request, ServerCallContext context)
    {
        return await TrySetManyKeyValueInternal(request, context);
    }
    
    /// <summary>
    /// Attempts to set many key-value pairs within the key-value store with additional parameters such as expiration, flags, and durability.
    /// </summary>
    /// <param name="request">The request containing the key, value, comparison value, flags, expiration time, durability level, and transaction details.</param>
    /// <param name="context">The server call context associated with the operation, including cancellation tokens and call metadata.</param>
    /// <returns>A response indicating the result of the operation, including the response type, revision number, last modified timestamp, and elapsed time in milliseconds.</returns>
    internal async Task<GrpcTrySetManyKeyValueResponse> TrySetManyKeyValueInternal(GrpcTrySetManyKeyValueRequest request, ServerCallContext context)
    {       
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
   
        List<KahunaSetKeyValueResponseItem> responses = await keyValues.LocateAndTrySetManyKeyValue(
            GetRequestSetManyItems(request.Items),
            context.CancellationToken
        );
               
        GrpcTrySetManyKeyValueResponse response = new()        
        {
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };

        response.Items.AddRange(GetResponseSetManyItems(responses));

        return response;
    }    

    private static List<KahunaSetKeyValueRequestItem> GetRequestSetManyItems(RepeatedField<GrpcTrySetManyKeyValueRequestItem> items)
    {                 
        List<KahunaSetKeyValueRequestItem> requestItems = new(items.Count);
        
        foreach (GrpcTrySetManyKeyValueRequestItem item in items)
        {
            byte[]? value;
        
            if (MemoryMarshal.TryGetArray(item.Value.Memory, out ArraySegment<byte> segment))
                value = segment.Array;
            else
                value = item.Value.ToByteArray();
            
            byte[]? compareValue;
            
            if (MemoryMarshal.TryGetArray(item.CompareValue.Memory, out segment))
                compareValue = segment.Array;
            else
                compareValue = item.CompareValue.ToByteArray();
            
            requestItems.Add(new()
            {
                TransactionId = new(item.TransactionIdNode, item.TransactionIdPhysical, item.TransactionIdCounter),
                Key = item.Key,
                Value = value,
                CompareValue = compareValue,
                CompareRevision = item.CompareRevision,
                ExpiresMs = item.ExpiresMs,
                Flags = (KeyValueFlags)item.Flags,
                Durability = (KeyValueDurability)item.Durability
            });
        }

        return requestItems;
    }
    
    private static IEnumerable<GrpcTrySetManyKeyValueResponseItem> GetResponseSetManyItems(List<KahunaSetKeyValueResponseItem> responses)
    {
        foreach (KahunaSetKeyValueResponseItem response in responses)
        {
            yield return new()
            {
                Type = (GrpcKeyValueResponseType)response.Type,
                Key = response.Key,
                Revision = response.Revision,
                LastModifiedNode = response.LastModified.N,
                LastModifiedPhysical = response.LastModified.L,
                LastModifiedCounter = response.LastModified.C,
                Durability = (GrpcKeyValueDurability)response.Durability
            };
        }
    }

    /// <summary>
    /// Attempts to extend the expiration of a key-value pair in the store.
    /// </summary>
    /// <param name="request">The request containing key and expiration data to extend the key-value pair.</param>
    /// <param name="context">The context of the server call.</param>
    /// <returns>A task that represents the asynchronous operation, containing the response of the key-value extension attempt.</returns>
    public override async Task<GrpcTryExtendKeyValueResponse> TryExtendKeyValue(GrpcTryExtendKeyValueRequest request, ServerCallContext context)
    {
        return await TryExtendKeyValueInternal(request, context);
    }

    /// <summary>
    /// Attempts to extend the expiration time for a key-value pair.
    /// </summary>
    /// <param name="request">The request containing the key, expiration time, and other parameters for the operation.</param>
    /// <param name="context">The server call context for the gRPC operation.</param>
    /// <returns>A response containing the result of the extension operation, including the type, revision, and last modified timestamp.</returns>
    internal async Task<GrpcTryExtendKeyValueResponse> TryExtendKeyValueInternal(GrpcTryExtendKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key) || request.ExpiresMs < 0)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };

        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await keyValues.LocateAndTryExtendKeyValue(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key, 
            request.ExpiresMs,
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type,
            Revision = revision,
            LastModifiedNode = lastModified.N,
            LastModifiedPhysical = lastModified.L,
            LastModifiedCounter = lastModified.C,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }

    /// <summary>
    /// Attempts to delete the specified key-value pair.
    /// </summary>
    /// <param name="request">The request containing the key to be deleted.</param>
    /// <param name="context">The server call context for the gRPC call.</param>
    /// <returns>Returns a response indicating the success or failure of the deletion operation.</returns>
    public override async Task<GrpcTryDeleteKeyValueResponse> TryDeleteKeyValue(GrpcTryDeleteKeyValueRequest request, ServerCallContext context)
    {
        return await TryDeleteKeyValueInternal(request, context);
    }

    /// <summary>
    /// Attempts to delete a key-value pair based on the provided request and returns the outcome.
    /// </summary>
    /// <param name="request">The request containing the key to be deleted and associated parameters.</param>
    /// <param name="context">The gRPC server call context for the current operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a GrpcTryDeleteKeyValueResponse object which indicates the result of the delete operation.</returns>
    internal async Task<GrpcTryDeleteKeyValueResponse> TryDeleteKeyValueInternal(GrpcTryDeleteKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        (KeyValueResponseType type, long revision, HLCTimestamp lastModified) = await keyValues.LocateAndTryDeleteKeyValue(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Key, 
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type,
            Revision = revision,
            LastModifiedNode = lastModified.N,
            LastModifiedPhysical = lastModified.L,
            LastModifiedCounter = lastModified.C,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }

    /// <summary>
    /// Retrieves the value associated with the specified key if it exists.
    /// </summary>
    /// <param name="request">The request containing the key to get the associated value for.</param>
    /// <param name="context">The server call context for managing metadata, timing, and cancellation tokens.</param>
    /// <returns>A <see cref="GrpcTryGetKeyValueResponse"/> containing the retrieved value if the key exists, or an appropriate response indicating failure.</returns>
    public override async Task<GrpcTryGetKeyValueResponse> TryGetKeyValue(GrpcTryGetKeyValueRequest request, ServerCallContext context)
    {
        return await TryGetKeyValueInternal(request, context);
    }

    /// <summary>
    /// Attempts to retrieve a key-value pair based on the specified request.
    /// </summary>
    /// <param name="request">The request containing details such as key, transaction IDs, revision, and durability settings.</param>
    /// <param name="context">The gRPC server call context providing runtime information about the request.</param>
    /// <returns>Returns a <see cref="GrpcTryGetKeyValueResponse"/> object indicating the result of the retrieval operation.</returns>
    internal async Task<GrpcTryGetKeyValueResponse> TryGetKeyValueInternal(GrpcTryGetKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? keyValueContext) = await keyValues.LocateAndTryGetValue(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
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
                ExpiresNode = keyValueContext.Expires.N,
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
    /// Handles the gRPC request to check if a specific key exists in the key-value store.
    /// </summary>
    /// <param name="request">The request object containing the key information.</param>
    /// <param name="context">The server call context for the operation.</param>
    /// <returns>A task that represents the asynchronous operation, returning a response indicating whether the key exists.</returns>
    public override async Task<GrpcTryExistsKeyValueResponse> TryExistsKeyValue(GrpcTryExistsKeyValueRequest request, ServerCallContext context)
    {
        return await TryExistsKeyValueInternal(request, context);
    }

    /// <summary>
    /// Attempts to check the existence of a key-value entry within a specified context.
    /// </summary>
    /// <param name="request">The request containing the details required to check the key-value existence, such as the key, transaction identifiers, revision, and durability settings.</param>
    /// <param name="context">The server call context used for managing RPC communication and cancellation tokens.</param>
    /// <returns>Returns a <see cref="GrpcTryExistsKeyValueResponse"/> representing the result of the existence check, including the response type and time elapsed during execution.</returns>
    internal async Task<GrpcTryExistsKeyValueResponse> TryExistsKeyValueInternal(GrpcTryExistsKeyValueRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();
        
        (KeyValueResponseType type, ReadOnlyKeyValueContext? keyValueContext) = await keyValues.LocateAndTryExistsValue(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
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
                ExpiresNode = keyValueContext.Expires.N,
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
    /// Attempts to acquire an exclusive lock on a key-value resource.
    /// </summary>
    /// <param name="request">The request containing details about the lock acquisition operation.</param>
    /// <param name="context">The context of the server call.</param>
    /// <returns>A response indicating the result of the lock acquisition attempt.</returns>
    public override async Task<GrpcTryAcquireExclusiveLockResponse> TryAcquireExclusiveLock(GrpcTryAcquireExclusiveLockRequest request, ServerCallContext context)
    {
        return await TryAcquireExclusiveLockInternal(request, context);
    }

    /// <summary>
    /// Attempts to acquire an exclusive lock for a given key with the specified expiration and durability settings.
    /// </summary>
    /// <param name="request">The request containing the key, transaction details, expiration time, and durability level.</param>
    /// <param name="context">The server call context that provides access to information about the RPC call.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains a response indicating whether the lock acquisition attempt succeeded or failed.</returns>
    internal async Task<GrpcTryAcquireExclusiveLockResponse> TryAcquireExclusiveLockInternal(GrpcTryAcquireExclusiveLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        (KeyValueResponseType type, _, _) = await keyValues.LocateAndTryAcquireExclusiveLock(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
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
    /// Attempts to acquire multiple exclusive locks based on the provided request.
    /// </summary>
    /// <param name="request">The request containing the details for acquiring multiple exclusive locks.</param>
    /// <param name="context">The server call context for the gRPC method.</param>
    /// <returns>A response indicating the result of the attempt to acquire the exclusive locks.</returns>
    public override async Task<GrpcTryAcquireManyExclusiveLocksResponse> TryAcquireManyExclusiveLocks(GrpcTryAcquireManyExclusiveLocksRequest request, ServerCallContext context)
    {
        return await TryAcquireManyExclusiveLocksInternal(request, context);
    }

    internal async Task<GrpcTryAcquireManyExclusiveLocksResponse> TryAcquireManyExclusiveLocksInternal(GrpcTryAcquireManyExclusiveLocksRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = await keyValues.LocateAndTryAcquireManyExclusiveLocks(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
            GetRequestLocksItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryAcquireManyExclusiveLocksResponse response = new();
        
        response.Items.Add(GetResponseLocksItems(responses));

        return response;
    }

    /// <summary>
    /// Converts a collection of gRPC exclusive lock request items into a list of tuples
    /// containing key, expiration time in milliseconds, and durability type.
    /// </summary>
    /// <param name="items">A collection of gRPC exclusive lock request items to be processed.</param>
    /// <returns>A list of tuples with each tuple containing a key, expiration time in milliseconds,
    /// and durability type derived from the input collection.</returns>
    private static List<(string key, int expiresMs, KeyValueDurability durability)> GetRequestLocksItems(RepeatedField<GrpcTryAcquireManyExclusiveLocksRequestItem> items)
    {
        List<(string key, int expiresMs, KeyValueDurability durability)> rItems = new(items.Count);
        
        foreach (GrpcTryAcquireManyExclusiveLocksRequestItem item in items)
            rItems.Add((item.Key, item.ExpiresMs, (KeyValueDurability)item.Durability));
        
        return rItems;
    }

    /// <summary>
    /// Converts a list of key-value response tuples into a collection of gRPC response items.
    /// </summary>
    /// <param name="responses">A list of tuples containing the key-value response type, key, and durability information.</param>
    /// <returns>An enumerable collection of gRPC response items corresponding to the provided key-value responses.</returns>
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

    /// <summary>
    /// Attempts to release an exclusive lock for the specified key and transaction.
    /// </summary>
    /// <param name="request">The request containing the key, transaction details, and durability level.</param>
    /// <param name="context">The context for the server call.</param>
    /// <returns>A response indicating the result of the release lock attempt.</returns>
    internal async Task<GrpcTryReleaseExclusiveLockResponse> TryReleaseExclusiveLockInternal(GrpcTryReleaseExclusiveLockRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        (KeyValueResponseType type, string _) = await keyValues.LocateAndTryReleaseExclusiveLock(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
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

    /// <summary>
    /// Attempts to release multiple exclusive locks based on the provided request and context.
    /// </summary>
    /// <param name="request">The request containing transaction details and items to release.</param>
    /// <param name="context">The gRPC server call context for the current operation.</param>
    /// <returns>A task representing the asynchronous operation, containing a response with the release results.</returns>
    internal async Task<GrpcTryReleaseManyExclusiveLocksResponse> TryReleaseManyExclusiveLocksInternal(GrpcTryReleaseManyExclusiveLocksRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType, string, KeyValueDurability)> responses = await keyValues.LocateAndTryReleaseManyExclusiveLocks(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
            GetRequestReleaseItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryReleaseManyExclusiveLocksResponse response = new();
        
        response.Items.Add(GetResponseReleaseItems(responses));

        return response;
    }

    /// <summary>
    /// Converts a collection of gRPC request items into a list of tuples containing the key and its associated durability.
    /// </summary>
    /// <param name="items">The collection of gRPC request items containing the key and durability information.</param>
    /// <returns>A list of tuples, where each tuple contains a key as a string and a durability type.</returns>
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
    /// Handles the request to prepare mutations for a set of key-value changes.
    /// </summary>
    /// <param name="request">The incoming request containing key-value mutation details.</param>
    /// <param name="context">The context for the server call providing runtime details and metadata.</param>
    /// <returns>A task that represents the asynchronous operation, containing the response for the mutation preparation request.</returns>
    public override async Task<GrpcTryPrepareMutationsResponse> TryPrepareMutations(GrpcTryPrepareMutationsRequest request, ServerCallContext context)
    {
        return await TryPrepareMutationsInternal(request, context); 
    }

    /// <summary>
    /// Attempts to prepare mutations internally using the provided request and context.
    /// </summary>
    /// <param name="request">The request containing details about the mutations to be prepared.</param>
    /// <param name="context">The server call context associated with this operation.</param>
    /// <returns>A task representing the asynchronous operation, containing the response with the result of the preparation attempt.</returns>
    internal async Task<GrpcTryPrepareMutationsResponse> TryPrepareMutationsInternal(GrpcTryPrepareMutationsRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        (KeyValueResponseType type, HLCTimestamp proposalTicket, _, _) = await keyValues.LocateAndTryPrepareMutations(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            new(request.CommitIdNode, request.CommitIdPhysical, request.CommitIdCounter),
            request.Key, 
            (KeyValueDurability)request.Durability, 
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcKeyValueResponseType)type,
            ProposalTicketNode = proposalTicket.N,
            ProposalTicketPhysical = proposalTicket.L,
            ProposalTicketCounter = proposalTicket.C
        };
    }

    /// <summary>
    /// Attempts to prepare multiple mutations in a single operation.
    /// </summary>
    /// <param name="request">The request object containing details of the mutations to prepare.</param>
    /// <param name="context">The server call context for the operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the response object with the outcome of the mutation preparation.</returns>
    public override async Task<GrpcTryPrepareManyMutationsResponse> TryPrepareManyMutations(GrpcTryPrepareManyMutationsRequest request, ServerCallContext context)
    {
        return await TryPrepareManyMutationsInternal(request, context);
    }

    /// <summary>
    /// Attempts to prepare multiple mutations for processing internally.
    /// </summary>
    /// <param name="request">The request containing the transaction and mutation details.</param>
    /// <param name="context">The server call context for processing the request.</param>
    /// <returns>A task resolving to the response that contains the result of the mutation preparation.</returns>
    internal async Task<GrpcTryPrepareManyMutationsResponse> TryPrepareManyMutationsInternal(GrpcTryPrepareManyMutationsRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> responses = await keyValues.LocateAndTryPrepareManyMutations(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            new(request.CommitIdNode, request.CommitIdPhysical, request.CommitIdCounter),
            GetRequestPrepareItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryPrepareManyMutationsResponse response = new();
        
        response.Items.Add(GetResponsePrepareItems(responses));

        return response;
    }

    /// <summary>
    /// Converts a collection of gRPC request items into a list of key-durability pairs.
    /// </summary>
    /// <param name="requestItems">The collection of gRPC request items to process.</param>
    /// <returns>A list of tuples where each tuple contains a key and its associated durability.</returns>
    private static List<(string key, KeyValueDurability durability)> GetRequestPrepareItems(RepeatedField<GrpcTryPrepareManyMutationsRequestItem> requestItems)
    {
        List<(string key, KeyValueDurability durability)> rItems = new(requestItems.Count);
        
        foreach (GrpcTryPrepareManyMutationsRequestItem item in requestItems)
            rItems.Add((item.Key, (KeyValueDurability)item.Durability));

        return rItems;
    }

    /// <summary>
    /// Converts a list of response details into a collection of gRPC response items.
    /// </summary>
    /// <param name="responses">The list of tuples containing response details, including response type, ticket ID, key, and durability.</param>
    /// <returns>A collection of <see cref="GrpcTryPrepareManyMutationsResponseItem"/> objects representing the prepared response items.</returns>
    private static IEnumerable<GrpcTryPrepareManyMutationsResponseItem> GetResponsePrepareItems(List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> responses)
    {
        foreach ((KeyValueResponseType type, HLCTimestamp ticketId, string key, KeyValueDurability durability) in responses)
            yield return new()
            {
                Type = (GrpcKeyValueResponseType)type,
                ProposalTicketNode = ticketId.N,
                ProposalTicketPhysical = ticketId.L, 
                ProposalTicketCounter = ticketId.C, 
                Key = key,
                Durability = (GrpcKeyValueDurability)durability
            };
    }

    /// <summary>
    /// Attempts to commit a series of mutations represented by the request.
    /// </summary>
    /// <param name="request">The request containing the mutations to be committed.</param>
    /// <param name="context">The context for the server call.</param>
    /// <returns>A response indicating the result of the commit attempt.</returns>
    public override async Task<GrpcTryCommitMutationsResponse> TryCommitMutations(GrpcTryCommitMutationsRequest request, ServerCallContext context)
    {
        return await TryCommitMutationsInternal(request, context);
    }

    /// <summary>
    /// Attempts to commit mutations for the provided request with specified transaction details.
    /// </summary>
    /// <param name="request">The request containing mutation details, transaction identifiers, and durability settings.</param>
    /// <param name="context">The server call context providing metadata and cancellation token for the operation.</param>
    /// <returns>A task that represents the asynchronous operation, with a result of type <see cref="GrpcTryCommitMutationsResponse"/> indicating the outcome of the commit operation.</returns>
    internal async Task<GrpcTryCommitMutationsResponse> TryCommitMutationsInternal(GrpcTryCommitMutationsRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        (KeyValueResponseType type, long commitIndex) = await keyValues.LocateAndTryCommitMutations(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
            request.Key, 
            new(request.ProposalTicketNode, request.ProposalTicketPhysical, request.ProposalTicketCounter),
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
    /// Attempts to commit multiple mutations atomically.
    /// </summary>
    /// <param name="request">The request containing the mutations to be committed.</param>
    /// <param name="context">The server call context.</param>
    /// <returns>A task representing the asynchronous operation, with a response indicating the result of the commit.</returns>
    public override async Task<GrpcTryCommitManyMutationsResponse> TryCommitManyMutations(GrpcTryCommitManyMutationsRequest request, ServerCallContext context)
    {
        return await TryCommitManyMutationsInternal(request, context);
    }

    /// <summary>
    /// Attempts to commit multiple mutations internally.
    /// </summary>
    /// <param name="request">The request containing the mutations to be committed.</param>
    /// <param name="context">The context of the server call.</param>
    /// <returns>A task representing the asynchronous operation, containing the response of the mutations commit.</returns>
    internal async Task<GrpcTryCommitManyMutationsResponse> TryCommitManyMutationsInternal(GrpcTryCommitManyMutationsRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> responses = await keyValues.LocateAndTryCommitManyMutations(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
            GetRequestCommitItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryCommitManyMutationsResponse response = new();
        
        response.Items.Add(GetResponseCommitItems(responses));

        return response;
    }

    /// <summary>
    /// Converts a collection of gRPC request items into a list of tuples containing the key, timestamp, and durability.
    /// </summary>
    /// <param name="requestItems">A collection of gRPC request items to be converted.</param>
    /// <returns>A list of tuples, each containing a key, a high-level logical clock timestamp, and a durability value.</returns>
    private static List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> GetRequestCommitItems(RepeatedField<GrpcTryCommitManyMutationsRequestItem> requestItems)
    {
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> rItems = new(requestItems.Count);
        
        foreach (GrpcTryCommitManyMutationsRequestItem item in requestItems)
            rItems.Add((
                item.Key, 
                new(item.ProposalTicketNode, item.ProposalTicketPhysical, item.ProposalTicketCounter), 
                (KeyValueDurability)item.Durability
            ));

        return rItems;
    }

    /// <summary>
    /// Converts a list of response tuples into a collection of GrpcTryCommitManyMutationsResponseItem objects.
    /// </summary>
    /// <param name="responses">A list of tuples containing the response type, key, proposal index, and durability information.</param>
    /// <returns>An enumerable collection of GrpcTryCommitManyMutationsResponseItem objects.</returns>
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
        return await TryRollbackMutationsInternal(request, context);
    }

    /// <summary>
    /// Attempts to rollback mutations based on the provided request and context.
    /// </summary>
    /// <param name="request">The rollback mutations request containing transaction and key details.</param>
    /// <param name="context">The gRPC server call context for the operation.</param>
    /// <returns>A task representing the asynchronous operation, with a result of type <see cref="GrpcTryRollbackMutationsResponse"/> containing the response details.</returns>
    internal async Task<GrpcTryRollbackMutationsResponse> TryRollbackMutationsInternal(GrpcTryRollbackMutationsRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.Key))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        (KeyValueResponseType type, long rollbackIndex) = await keyValues.LocateAndTryRollbackMutations(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
            request.Key, 
            new(request.ProposalTicketNode, request.ProposalTicketPhysical, request.ProposalTicketCounter),
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
    /// Receives requests for the key/value "RollbackManyMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryRollbackManyMutationsResponse> TryRollbackManyMutations(GrpcTryRollbackManyMutationsRequest request, ServerCallContext context)
    {
        return await TryRollbackManyMutationsInternal(request, context);
    }
    
    /// <summary>
    /// Receives requests for the key/value "CommitManyMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    internal async Task<GrpcTryRollbackManyMutationsResponse> TryRollbackManyMutationsInternal(GrpcTryRollbackManyMutationsRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> responses = await keyValues.LocateAndTryRollbackManyMutations(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
            GetRequestRollbackItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryRollbackManyMutationsResponse response = new();
        
        response.Items.Add(GetResponseRollbackItems(responses));

        return response;
    }

    /// <summary>
    /// Converts a collection of rollback request items into a list of rollback details
    /// containing the key, proposal ticket ID, and durability.
    /// </summary>
    /// <param name="requestItems">The collection of rollback request items to process.</param>
    /// <returns>A list of tuples, each containing the key, proposal ticket ID, and durability.</returns>
    private static List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> GetRequestRollbackItems(RepeatedField<GrpcTryRollbackManyMutationsRequestItem> requestItems)
    {
        List<(string key, HLCTimestamp proposalTicketId, KeyValueDurability durability)> rItems = new(requestItems.Count);
        
        foreach (GrpcTryRollbackManyMutationsRequestItem item in requestItems)
            rItems.Add((
                item.Key, 
                new(item.ProposalTicketNode, item.ProposalTicketPhysical, item.ProposalTicketCounter),
                (KeyValueDurability)item.Durability
            ));

        return rItems;
    }

    /// <summary>
    /// Transforms a list of rollback response tuples into enumerable gRPC response items.
    /// </summary>
    /// <param name="responses">
    /// A list of tuples containing rollback response information.
    /// Each tuple includes the response type, key, proposal index, and durability.
    /// </param>
    /// <returns>
    /// An enumerable collection of gRPC response items
    /// represented as <see cref="GrpcTryRollbackManyMutationsResponseItem"/>.
    /// </returns>
    private static IEnumerable<GrpcTryRollbackManyMutationsResponseItem> GetResponseRollbackItems(List<(KeyValueResponseType, string, long, KeyValueDurability)> responses)
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
    /// Executes a transaction script on the key/value store
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryExecuteTransactionScriptResponse> TryExecuteTransactionScript(GrpcTryExecuteTransactionScriptRequest request, ServerCallContext context)
    {
        return await TryExecuteTransactionScriptInternal(request, context);
    }

    /// <summary>
    /// Attempts to execute a transaction script by processing the request and returning the result.
    /// </summary>
    /// <param name="request">The transaction script request containing the script, hash, and parameters.</param>
    /// <param name="context">The server call context for the request.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the response detailing the execution outcome, elapsed time, and any additional information.</returns>
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
                    ExpiresNode = value.Expires.N,
                    ExpiresPhysical = value.Expires.L,
                    ExpiresCounter = value.Expires.C,
                    LastModifiedNode = value.LastModified.N,
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
    /// Scans for key-value pairs based on a specified prefix.
    /// </summary>
    /// <param name="request">The request containing the prefix and relevant parameters for the scan operation.</param>
    /// <param name="context">The context for the server-side call, providing information such as deadlines and cancellation details.</param>
    /// <returns>A task representing the asynchronous operation, which resolves to a <see cref="GrpcScanByPrefixResponse"/> containing the scan results.</returns>
    public override async Task<GrpcScanByPrefixResponse> ScanByPrefix(GrpcScanByPrefixRequest request, ServerCallContext context)
    {
        return await ScanByPrefixInternal(request, context);
    }

    /// <summary>
    /// Handles the internal logic for scanning key-value items by a given prefix.
    /// </summary>
    /// <param name="request">The request containing the prefix key and other parameters for the scan operation.</param>
    /// <param name="context">The server call context for the gRPC operation.</param>
    /// <returns>A task representing the asynchronous operation, containing the response with the scanned key-value items.</returns>
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
    /// Retrieves key-value pairs matching a specific prefix.
    /// </summary>
    /// <param name="request">The request containing the prefix to match against.</param>
    /// <param name="context">The gRPC server call context.</param>
    /// <returns>A response containing the key-value pairs that match the specified prefix.</returns>
    public override async Task<GrpcGetByPrefixResponse> GetByPrefix(GrpcGetByPrefixRequest request, ServerCallContext context)
    {
        return await GetByPrefixInternal(request, context);
    }

    /// <summary>
    /// Handles the logic to retrieve key-value pairs based on a specified prefix.
    /// </summary>
    /// <param name="request">The request containing the prefix key and additional parameters for the operation.</param>
    /// <param name="context">The server call context for managing RPC settings and cancellation.</param>
    /// <returns>A task that represents the asynchronous operation, returning a response with the matching key-value pairs or an error type.</returns>
    internal async Task<GrpcGetByPrefixResponse> GetByPrefixInternal(GrpcGetByPrefixRequest request, ServerCallContext context)
    {
        if (request.PrefixKey is null)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
            
        KeyValueGetByPrefixResult result = await keyValues.LocateAndGetByPrefix(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
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

    /// <summary>
    /// Processes a request to scan all key-value pairs with a specified prefix internally.
    /// </summary>
    /// <param name="request">The gRPC request containing the prefix key and additional scan criteria.</param>
    /// <param name="context">The server call context providing details about the request context.</param>
    /// <returns>A task that represents the asynchronous operation, returning a response with the scan results or an error type.</returns>
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

    /// <summary>
    /// Initiates an interactive transaction
    /// </summary>
    /// <param name="request">The request object containing transaction details.</param>
    /// <param name="context">The server call context for the request.</param>
    /// <returns>A task that represents the asynchronous operation, containing the transaction response.</returns>
    public override async Task<GrpcStartTransactionResponse> StartTransaction(GrpcStartTransactionRequest request, ServerCallContext context)
    {
        return await StartTransactionInternal(request, context);
    }

    /// <summary>
    /// Handles the internal logic for starting a transaction with the provided request and context.
    /// </summary>
    /// <param name="request">The transaction request containing details such as unique identifier, locking type, timeout, and async release options.</param>
    /// <param name="context">The server call context associated with the gRPC request.</param>
    /// <returns>A response containing the transaction status and transaction ID (physical and counter).</returns>
    internal async Task<GrpcStartTransactionResponse> StartTransactionInternal(GrpcStartTransactionRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.UniqueId))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
            
        (KeyValueResponseType type, HLCTimestamp transactionId) = await keyValues.LocateAndStartTransaction(new()
        {
            UniqueId = request.UniqueId,
            Locking = (KeyValueTransactionLocking)request.LockingType,
            Timeout = request.Timeout,
            AsyncRelease = request.AsyncRelease,
            AutoCommit = request.AutoCommit,
        }, context.CancellationToken);

        GrpcStartTransactionResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)type,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
        
        //if (result.Items.Count > 0)
        //    response.Items.Add(GetKeyValueItems(result.Items));
        
        return response;
    }

    /// <summary>
    /// Commits a transaction in the key-value store.
    /// </summary>
    /// <param name="request">The request containing transaction details to be committed.</param>
    /// <param name="context">The server call context for the current request.</param>
    /// <returns>A response indicating the result of the commit operation.</returns>
    public override async Task<GrpcCommitTransactionResponse> CommitTransaction(GrpcCommitTransactionRequest request, ServerCallContext context)
    {
        return await CommitTransactionInternal(request, context);
    }

    /// <summary>
    /// Handles the internal logic for committing a transaction in the KeyValues service.
    /// </summary>
    /// <param name="request">The transaction commit request containing necessary details such as unique ID and keys.</param>
    /// <param name="context">The server call context providing metadata and call-specific context.</param>
    /// <returns>Returns a response indicating the outcome of the transaction commit operation.</returns>
    internal async Task<GrpcCommitTransactionResponse> CommitTransactionInternal(GrpcCommitTransactionRequest request, ServerCallContext context)
    {                
        if (string.IsNullOrEmpty(request.UniqueId))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
            
        KeyValueResponseType type = await keyValues.LocateAndCommitTransaction(
            request.UniqueId,
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            GetTransactionAcquiredOrModifiedKeys(request.AcquiredLocks).ToList(),
            GetTransactionAcquiredOrModifiedKeys(request.ModifiedKeys).ToList(),
            context.CancellationToken
        );               

        GrpcCommitTransactionResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)type,
        };
        
        //if (result.Items.Count > 0)
        //    response.Items.Add(GetKeyValueItems(result.Items));
        
        return response;
    }

    /// <summary>
    /// Rolls back a transaction in the key-value store operation.
    /// </summary>
    /// <param name="request">The rollback transaction request containing the operation details.</param>
    /// <param name="context">The server call context providing contextual information about the request.</param>
    /// <returns>A response indicating the outcome of the rollback transaction operation.</returns>
    public override async Task<GrpcRollbackTransactionResponse> RollbackTransaction(GrpcRollbackTransactionRequest request, ServerCallContext context)
    {
        return await RollbackTransactionInternal(request, context);
    }

    /// <summary>
    /// Handles the internal rollback of a transaction based on the provided request and context.
    /// </summary>
    /// <param name="request">The rollback transaction request containing details such as transaction ID and related keys.</param>
    /// <param name="context">The server call context for the RPC request.</param>
    /// <returns>A task representing the operation, returning a <see cref="GrpcRollbackTransactionResponse"/> indicating the result of the rollback operation.</returns>
    internal async Task<GrpcRollbackTransactionResponse> RollbackTransactionInternal(GrpcRollbackTransactionRequest request, ServerCallContext context)
    {                
        if (string.IsNullOrEmpty(request.UniqueId))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
            
        KeyValueResponseType type = await keyValues.LocateAndRollbackTransaction(
            request.UniqueId,
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            GetTransactionAcquiredOrModifiedKeys(request.AcquiredLocks).ToList(),
            GetTransactionAcquiredOrModifiedKeys(request.ModifiedKeys).ToList(),
            context.CancellationToken
        );        

        GrpcRollbackTransactionResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)type,
        };
        
        //if (result.Items.Count > 0)
        //    response.Items.Add(GetKeyValueItems(result.Items));
        
        return response;
    }

    private static IEnumerable<KeyValueTransactionModifiedKey> GetTransactionAcquiredOrModifiedKeys(RepeatedField<GrpcTransactionModifiedKey> requestModifiedKeys)
    {
        foreach (GrpcTransactionModifiedKey item in requestModifiedKeys)
        {
            yield return new()
            {
                Key = item.Key,
                Durability = (KeyValueDurability)item.Durability
            };
        }
    }

    /// <summary>
    /// Converts a list of key-value result items into a collection of gRPC key-value prefix item responses.
    /// </summary>
    /// <param name="resultItems">A list containing key-value pairs and their associated context.</param>
    /// <returns>A collection of gRPC key-value prefix item responses.</returns>
    private static IEnumerable<GrpcKeyValueByPrefixItemResponse> GetKeyValueItems(List<(string, ReadOnlyKeyValueContext)> resultItems)
    {
        foreach ((string key, ReadOnlyKeyValueContext context) in resultItems)
        {
            GrpcKeyValueByPrefixItemResponse response = new()
            {
                Key = key,
                Revision = context.Revision,
                ExpiresNode = context.Expires.N,
                ExpiresPhysical = context.Expires.L,
                ExpiresCounter = context.Expires.C,
            };
            
            if (context.Value is not null)
                response.Value = UnsafeByteOperations.UnsafeWrap(context.Value);
            
            yield return response;
        }
    }

    /// <summary>
    /// Converts a collection of gRPC key-value parameters into a list of KeyValueParameter objects.
    /// </summary>
    /// <param name="requestParameters">The collection of gRPC key-value parameters to be converted.</param>
    /// <returns>A list of KeyValueParameter objects derived from the given gRPC key-value parameters.</returns>
    private static List<KeyValueParameter> GetParameters(RepeatedField<GrpcKeyValueParameter> requestParameters)
    {
        List<KeyValueParameter> parameters = new(requestParameters.Count);
        
        foreach (GrpcKeyValueParameter? parameter in requestParameters)
            parameters.Add(new() { Key = parameter.Key, Value = parameter.Value });
        
        return parameters;
    }

    /// <summary>
    /// Processes a batch of client key-value requests using a gRPC bidirectional stream
    /// </summary>
    /// <param name="requestStream">The stream of incoming key-value requests from the client.</param>
    /// <param name="responseStream">The stream for sending key-value responses back to the client.</param>
    /// <param name="context">The context of the server call.</param>
    /// <returns>A task that represents the asynchronous processing of the batch of requests.</returns>
    public override async Task BatchClientKeyValueRequests(
        IAsyncStreamReader<GrpcBatchClientKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        await clientBatcher.BatchClientKeyValueRequests(requestStream, responseStream, context);
    }

    /// <summary>
    /// Processes a batch of server key-value requests using a gRPC bidirectional stream 
    /// </summary>
    /// <param name="requestStream"></param>
    /// <param name="responseStream"></param>
    /// <param name="context"></param>
    public override async Task BatchServerKeyValueRequests(
        IAsyncStreamReader<GrpcBatchServerKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        await serverBatcher.BatchServerKeyValueRequests(requestStream, responseStream, context);
    }
}
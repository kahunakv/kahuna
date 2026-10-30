
/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using Kahuna.Server.KeyValues.Data;
using Kommander.Time;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Grpc.Core;
using Kahuna.Communication.External.Grpc.KeyValues;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.Communication;
using Kahuna.Server.Configuration;
using Kahuna.Server.Communication.Internode;
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Routing;
using Kahuna.Shared.Routing;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Shared.Communication.Rest;
using Kahuna.Shared.KeyValue;
using Kommander.Diagnostics;
using static Kahuna.Communication.External.Grpc.KeyValues.KeyValueGrpcWire;

namespace Kahuna.Communication.External.Grpc.KeyValues;

/// <summary>
/// Prepare, commit and rollback of staged mutations for the two-phase commit path.
/// </summary>
internal sealed class KeyValueMutationHandler
{
    private readonly IKahuna keyValues;

    public KeyValueMutationHandler(IKahuna keyValues)
    {
        this.keyValues = keyValues;
    }

    /// <summary>
    /// Attempts to prepare mutations internally using the provided request and context.
    /// </summary>
    /// <param name="request">The request containing details about the mutations to be prepared.</param>
    /// <param name="context">The server call context associated with this operation.</param>
    /// <returns>A task representing the asynchronous operation, containing the response with the result of the preparation attempt.</returns>
    internal async Task<GrpcTryPrepareMutationsResponse> TryPrepareMutationsCore(GrpcTryPrepareMutationsRequest request, ServerCallContext context)
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
            context.CancellationToken,
            request.RoutedGeneration,
            request.HasRecordAnchorKey ? request.RecordAnchorKey : null
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
    /// Attempts to prepare multiple mutations for processing internally.
    /// </summary>
    /// <param name="request">The request containing the transaction and mutation details.</param>
    /// <param name="context">The server call context for processing the request.</param>
    /// <returns>A task resolving to the response that contains the result of the mutation preparation.</returns>
    internal async Task<GrpcTryPrepareManyMutationsResponse> TryPrepareManyMutationsCore(GrpcTryPrepareManyMutationsRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> responses = await keyValues.LocateAndTryPrepareManyMutations(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            new(request.CommitIdNode, request.CommitIdPhysical, request.CommitIdCounter),
            GetRequestPrepareItems(request.Items),
            context.CancellationToken,
            request.HasRecordAnchorKey ? request.RecordAnchorKey : null
        );

        GrpcTryPrepareManyMutationsResponse response = new();

        AddResponsePrepareItems(response.Items, responses);

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
    private static void AddResponsePrepareItems(RepeatedField<GrpcTryPrepareManyMutationsResponseItem> target, List<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> responses)
    {
        ReserveItemCapacity(target, responses.Count);

        foreach ((KeyValueResponseType type, HLCTimestamp ticketId, string key, KeyValueDurability durability) in responses)
            target.Add(new GrpcTryPrepareManyMutationsResponseItem
            {
                Type = (GrpcKeyValueResponseType)type,
                ProposalTicketNode = ticketId.N,
                ProposalTicketPhysical = ticketId.L,
                ProposalTicketCounter = ticketId.C,
                Key = key,
                Durability = (GrpcKeyValueDurability)durability
            });
    }

    /// <summary>
    /// Attempts to commit mutations for the provided request with specified transaction details.
    /// </summary>
    /// <param name="request">The request containing mutation details, transaction identifiers, and durability settings.</param>
    /// <param name="context">The server call context providing metadata and cancellation token for the operation.</param>
    /// <returns>A task that represents the asynchronous operation, with a result of type <see cref="GrpcTryCommitMutationsResponse"/> indicating the outcome of the commit operation.</returns>
    internal async Task<GrpcTryCommitMutationsResponse> TryCommitMutationsCore(GrpcTryCommitMutationsRequest request, ServerCallContext context)
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
    /// Attempts to commit multiple mutations internally.
    /// </summary>
    /// <param name="request">The request containing the mutations to be committed.</param>
    /// <param name="context">The context of the server call.</param>
    /// <returns>A task representing the asynchronous operation, containing the response of the mutations commit.</returns>
    internal async Task<GrpcTryCommitManyMutationsResponse> TryCommitManyMutationsCore(GrpcTryCommitManyMutationsRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> responses = await keyValues.LocateAndTryCommitManyMutations(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
            GetRequestCommitItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryCommitManyMutationsResponse response = new();

        AddResponseCommitItems(response.Items, responses);

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
    private static void AddResponseCommitItems(RepeatedField<GrpcTryCommitManyMutationsResponseItem> target, List<(KeyValueResponseType, string, long, KeyValueDurability)> responses)
    {
        ReserveItemCapacity(target, responses.Count);

        foreach ((KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability) in responses)
            target.Add(new GrpcTryCommitManyMutationsResponseItem
            {
                Type = (GrpcKeyValueResponseType)type,
                Key = key,
                ProposalIndex = proposalIndex,
                Durability = (GrpcKeyValueDurability)durability
            });
    }

    /// <summary>
    /// Attempts to rollback mutations based on the provided request and context.
    /// </summary>
    /// <param name="request">The rollback mutations request containing transaction and key details.</param>
    /// <param name="context">The gRPC server call context for the operation.</param>
    /// <returns>A task representing the asynchronous operation, with a result of type <see cref="GrpcTryRollbackMutationsResponse"/> containing the response details.</returns>
    internal async Task<GrpcTryRollbackMutationsResponse> TryRollbackMutationsCore(GrpcTryRollbackMutationsRequest request, ServerCallContext context)
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
    /// Receives requests for the key/value "CommitManyMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    internal async Task<GrpcTryRollbackManyMutationsResponse> TryRollbackManyMutationsCore(GrpcTryRollbackManyMutationsRequest request, ServerCallContext context)
    {
        List<(KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability)> responses = await keyValues.LocateAndTryRollbackManyMutations(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter), 
            GetRequestRollbackItems(request.Items), 
            context.CancellationToken
        );

        GrpcTryRollbackManyMutationsResponse response = new();

        AddResponseRollbackItems(response.Items, responses);

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
    private static void AddResponseRollbackItems(RepeatedField<GrpcTryRollbackManyMutationsResponseItem> target, List<(KeyValueResponseType, string, long, KeyValueDurability)> responses)
    {
        ReserveItemCapacity(target, responses.Count);

        foreach ((KeyValueResponseType type, string key, long proposalIndex, KeyValueDurability durability) in responses)
            target.Add(new GrpcTryRollbackManyMutationsResponseItem
            {
                Type = (GrpcKeyValueResponseType)type,
                Key = key,
                ProposalIndex = proposalIndex,
                Durability = (GrpcKeyValueDurability)durability
            });
    }
}

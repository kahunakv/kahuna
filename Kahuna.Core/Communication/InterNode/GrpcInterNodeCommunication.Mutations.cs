
using Kommander;
using Kahuna.Server.KeyValues.Data;
using Kommander.Time;
using Kommander.Communication.Grpc;
using Microsoft.Extensions.Logging;

using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using System.Collections.Concurrent;
using Google.Protobuf.Collections;

using Kahuna.Shared.Locks;
using Kahuna.Shared.Sequences;
using Kahuna.Shared.KeyValue;
using Kahuna.Shared.Communication.Grpc;
using Kahuna.Server.Configuration;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Locks;
using Kahuna.Server.Communication.Internode.Grpc;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues.Writes;
using Kahuna.Server.Locks.Data;
using Kahuna.Server.Persistence.Pitr;


namespace Kahuna.Server.Communication.Internode;

/// <summary>Two-phase commit forwarding: prepare, commit and rollback of staged mutations.</summary>
public partial class GrpcInterNodeCommunication
{
    public async Task<(KeyValueResponseType, HLCTimestamp, string, KeyValueDurability)> TryPrepareMutations(
        string node, HLCTimestamp transactionId,
        HLCTimestamp commitId,
        string key,
        KeyValueDurability durability,
        long routedGeneration,
        CancellationToken cancellationToken,
        string? recordAnchorKey = null
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcTryPrepareMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            CommitIdNode = commitId.N,
            CommitIdPhysical = commitId.L,
            CommitIdCounter = commitId.C,
            Key = key,
            Durability = (GrpcKeyValueDurability)durability,
            RoutedGeneration = routedGeneration,
        };

        if (recordAnchorKey is not null)
            request.RecordAnchorKey = recordAnchorKey;

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "TryPrepareMutations", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryPrepareMutations(key, durability);

        GrpcTryPrepareMutationsResponse remoteResponse = response.TryPrepareMutations!;
        
        
        return (
            (KeyValueResponseType)remoteResponse.Type, 
            new(remoteResponse.ProposalTicketNode, remoteResponse.ProposalTicketPhysical, remoteResponse.ProposalTicketCounter), 
            key, 
            durability
        );
    }

    public async Task TryPrepareNodeMutations(
        string node, 
        HLCTimestamp transactionId,
        HLCTimestamp commitId,
        List<(string key, KeyValueDurability durability)> xkeys, 
        Lock lockSync, 
        List<(KeyValueResponseType type, HLCTimestamp, string key, KeyValueDurability durability)> responses,
        CancellationToken cancellationToken,
        string? recordAnchorKey = null
    )
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcTryPrepareManyMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            CommitIdNode = commitId.N,
            CommitIdPhysical = commitId.L,
            CommitIdCounter = commitId.C,
        };

        if (recordAnchorKey is not null)
            request.RecordAnchorKey = recordAnchorKey;

        AddPrepareRequestItems(request.Items, xkeys);
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "TryPrepareNodeMutations", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            UnansweredResults.TryPrepareNodeMutations(xkeys, lockSync, responses);
            return;
        }

        GrpcTryPrepareManyMutationsResponse remoteResponse = response.TryPrepareManyMutations!;

        lock (lockSync)
        {
            foreach (GrpcTryPrepareManyMutationsResponseItem item in remoteResponse.Items)
                responses.Add((
                    (KeyValueResponseType)item.Type, 
                    new(item.ProposalTicketNode, item.ProposalTicketPhysical, item.ProposalTicketCounter), 
                    item.Key, 
                    (KeyValueDurability)item.Durability
                ));
        }
    }
    
    internal static void AddPrepareRequestItems(RepeatedField<GrpcTryPrepareManyMutationsRequestItem> target, List<(string key, KeyValueDurability durability)> xkeys)
    {
        ReserveItemCapacity(target, xkeys.Count);

        foreach ((string key, KeyValueDurability durability) key in xkeys)
            target.Add(new GrpcTryPrepareManyMutationsRequestItem
            {
                Key = key.key,
                Durability = (GrpcKeyValueDurability)key.durability
            });
    }

    public async Task<(KeyValueResponseType, long)> TryCommitMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcTryCommitMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ProposalTicketNode = ticketId.N,
            ProposalTicketPhysical = ticketId.L,
            ProposalTicketCounter = ticketId.C,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "TryCommitMutations", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryCommitMutations;

        GrpcTryCommitMutationsResponse remoteResponse = response.TryCommitMutations!;
        
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.ProposalIndex);
    }

    public async Task TryCommitNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
            
        GrpcTryCommitManyMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
            
        AddCommitRequestItems(request.Items, xkeys);
            
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "TryCommitNodeMutations", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            UnansweredResults.TryCommitNodeMutations(xkeys, lockSync, responses);
            return;
        }

        GrpcTryCommitManyMutationsResponse remoteResponse = response.TryCommitManyMutations!;

        lock (lockSync)
        {
            foreach (GrpcTryCommitManyMutationsResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, item.ProposalIndex, (KeyValueDurability)item.Durability));
        }
    }
    
    internal static void AddCommitRequestItems(RepeatedField<GrpcTryCommitManyMutationsRequestItem> target, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys)
    {
        ReserveItemCapacity(target, xkeys.Count);

        foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) key in xkeys)
            target.Add(new GrpcTryCommitManyMutationsRequestItem
            {
                Key = key.key,
                ProposalTicketNode = key.ticketId.N,
                ProposalTicketPhysical = key.ticketId.L,
                ProposalTicketCounter = key.ticketId.C,
                Durability = (GrpcKeyValueDurability)key.durability
            });
    }

    public async Task<(KeyValueResponseType, long)> TryRollbackMutations(string node, HLCTimestamp transactionId, string key, HLCTimestamp ticketId, KeyValueDurability durability, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
        
        GrpcTryRollbackMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Key = key,
            ProposalTicketNode = ticketId.N,
            ProposalTicketPhysical = ticketId.L,
            ProposalTicketCounter = ticketId.C,
            Durability = (GrpcKeyValueDurability)durability,
        };
        
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "TryRollbackMutations", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return UnansweredResults.TryRollbackMutations;

        GrpcTryRollbackMutationsResponse remoteResponse = response.TryRollbackMutations!;
        
        
        return ((KeyValueResponseType)remoteResponse.Type, remoteResponse.ProposalIndex);
    }
    
    public async Task TryRollbackNodeMutations(string node, HLCTimestamp transactionId, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys, Lock lockSync, List<(KeyValueResponseType type, string key, long, KeyValueDurability durability)> responses, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);
            
        GrpcTryRollbackManyMutationsRequest request = new()
        {
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };
            
        AddRollbackRequestItems(request.Items, xkeys);
            
        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "TryRollbackNodeMutations", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            UnansweredResults.TryRollbackNodeMutations(xkeys, lockSync, responses);
            return;
        }

        GrpcTryRollbackManyMutationsResponse remoteResponse = response.TryRollbackManyMutations!;

        lock (lockSync)
        {
            foreach (GrpcTryRollbackManyMutationsResponseItem item in remoteResponse.Items)
                responses.Add(((KeyValueResponseType)item.Type, item.Key, item.ProposalIndex, (KeyValueDurability)item.Durability));
        }
    }
    
    internal static void AddRollbackRequestItems(RepeatedField<GrpcTryRollbackManyMutationsRequestItem> target, List<(string key, HLCTimestamp ticketId, KeyValueDurability durability)> xkeys)
    {
        ReserveItemCapacity(target, xkeys.Count);

        foreach ((string key, HLCTimestamp ticketId, KeyValueDurability durability) key in xkeys)
            target.Add(new GrpcTryRollbackManyMutationsRequestItem
            {
                Key = key.key,
                ProposalTicketNode = key.ticketId.N,
                ProposalTicketPhysical = key.ticketId.L,
                ProposalTicketCounter = key.ticketId.C,
                Durability = (GrpcKeyValueDurability)key.durability
            });
    }
}

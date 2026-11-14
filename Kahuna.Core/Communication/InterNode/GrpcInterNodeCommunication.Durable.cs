
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

/// <summary>Durable decision and bundle forwarding, and transaction record lookup.</summary>
public partial class GrpcInterNodeCommunication
{
    public async Task<bool> DurableOperation(string node, int partitionId, int kind, string logType, byte[] payload, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcDurableOperationRequest request = new()
        {
            PartitionId = partitionId,
            Kind = kind,
            LogType = logType,
            Payload = UnsafeByteOperations.UnsafeWrap(payload)
        };

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        return batchResponse.DurableOperation?.Committed ?? false;
    }

    public async Task<DurableBundleWireReply?> DurableBundle(
        string node, int partitionId, IReadOnlyList<(string LogType, byte[] Payload)> entries,
        bool terminal, int stage, IReadOnlyList<RangeFence> fences, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcDurableBundleRequest request = new()
        {
            PartitionId = partitionId,
            AdmissionClass = terminal ? 1 : 0,
            Stage = stage
        };

        // Every fence rides the repeated field; the first one is mirrored into the single-fence fields an older
        // receiver reads, so it still fences one consistent key/generation pair.
        (request.FenceKey, request.FenceGeneration) = RangeFenceWire.Write(fences, request.Fences);

        for (int i = 0; i < entries.Count; i++)
            request.Entries.Add(new GrpcDurableBundleEntry { LogType = entries[i].LogType, Payload = UnsafeByteOperations.UnsafeWrap(entries[i].Payload) });

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        // No payload means the receiver did not answer this operation kind (an older node, or a refusal the
        // batch reader mapped to no type): the caller falls back to per-entry forwards.
        GrpcDurableBundleResponse? response = batchResponse.DurableBundle;
        if (response is null)
            return null;

        return new DurableBundleWireReply(response.BatchCommitted, response.PrepareAcknowledged, response.PrepareRejection);
    }

    public async Task<DurableDecisionWireReply?> DurableDecision(
        string node, int partitionId, byte[] decisionDelta, HLCTimestamp transactionId, long epoch,
        IReadOnlyList<RangeFence> fences, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcDurableDecisionRequest request = new()
        {
            PartitionId = partitionId,
            DecisionDelta = UnsafeByteOperations.UnsafeWrap(decisionDelta),
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Epoch = epoch
        };

        (request.FenceKey, request.FenceGeneration) = RangeFenceWire.Write(fences, request.Fences);

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcDurableDecisionResponse? response = batchResponse.DurableDecision;
        if (response is null)
            return null;

        return new DurableDecisionWireReply(response.Replicated, response.Known, response.Decision, response.AbortClass);
    }

    public async Task<DurableOnePhaseWireReply?> DurableOnePhase(
        string node, int partitionId, byte[] recordInitDelta, byte[] anchorPrepareDelta, byte[] decisionDelta,
        HLCTimestamp transactionId, long epoch, HLCTimestamp opId,
        IReadOnlyList<RangeFence> fences, long expectedTerm, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcDurableOnePhaseRequest request = new()
        {
            PartitionId = partitionId,
            RecordInitDelta = UnsafeByteOperations.UnsafeWrap(recordInitDelta),
            AnchorPrepareDelta = UnsafeByteOperations.UnsafeWrap(anchorPrepareDelta),
            DecisionDelta = UnsafeByteOperations.UnsafeWrap(decisionDelta),
            ExpectedTerm = expectedTerm,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Epoch = epoch,
            OpIdNode = opId.N,
            OpIdPhysical = opId.L,
            OpIdCounter = opId.C
        };

        (request.FenceKey, request.FenceGeneration) = RangeFenceWire.Write(fences, request.Fences);

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        // No payload means the receiver did not answer this operation kind (an older node): the caller falls
        // back to the two-phase flow.
        GrpcDurableOnePhaseResponse? response = batchResponse.DurableOnePhase;
        if (response is null)
            return null;

        return new DurableOnePhaseWireReply(
            response.BatchCommitted, response.PrepareAcknowledged, response.PrepareRejection,
            response.DecisionKnown, response.Decision, response.AbortClass, response.GatedVerdict,
            response.BundleMs);
    }

    public async Task<byte[]?> LookupTransactionRecord(string node, int partitionId, HLCTimestamp transactionId, long epoch, string anchorKey, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcLookupTransactionRecordRequest request = new()
        {
            PartitionId = partitionId,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Epoch = epoch,
            AnchorKey = anchorKey
        };

        GrpcServerBatcherResponse batchResponse;

        if (cancellationToken == CancellationToken.None)
            batchResponse = await batcher.Enqueue(request).ConfigureAwait(false);
        else
            batchResponse = await batcher.Enqueue(request).WaitAsync(cancellationToken).ConfigureAwait(false);

        GrpcLookupTransactionRecordResponse? response = batchResponse.LookupTransactionRecord;
        return response is { Found: true } ? response.Record.ToByteArray() : null;
    }
}


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
/// Durable transaction records: operations, bundles, decisions and one-phase commits.
/// </summary>
internal sealed class KeyValueDurableHandler
{
    private readonly IKahuna keyValues;

    public KeyValueDurableHandler(IKahuna keyValues)
    {
        this.keyValues = keyValues;
    }

    internal async Task<GrpcDurableOperationResponse> DurableOperationInternal(GrpcDurableOperationRequest request, ServerCallContext context)
    {
        bool committed = await keyValues.DurableOperationLocal(
            request.PartitionId, request.Kind, request.LogType, request.Payload.ToByteArray(), context.CancellationToken);
        return new GrpcDurableOperationResponse { Committed = committed };
    }

    internal async Task<GrpcLookupTransactionRecordResponse> LookupTransactionRecordInternal(GrpcLookupTransactionRecordRequest request, ServerCallContext context)
    {
        HLCTimestamp transactionId = new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter);

        // Runs on the anchor partition leader (the caller routed it here): the local canonical record store is
        // authoritative. The record crosses the wire serialized; an absent record answers Found=false.
        byte[]? record = await keyValues.LookupTransactionRecordLocal(
            request.PartitionId, transactionId, request.Epoch, request.AnchorKey, context.CancellationToken);

        return new GrpcLookupTransactionRecordResponse
        {
            Found = record is not null,
            Record = record is not null ? UnsafeByteOperations.UnsafeWrap(record) : ByteString.Empty
        };
    }

    internal async Task<GrpcDurableBundleResponse> DurableBundleInternal(GrpcDurableBundleRequest request, ServerCallContext context)
    {
        (string LogType, byte[] Payload)[] entries = new (string, byte[])[request.Entries.Count];
        for (int i = 0; i < request.Entries.Count; i++)
            entries[i] = (request.Entries[i].LogType, request.Entries[i].Payload.ToByteArray());

        DurableBundleWireReply? reply = await keyValues.DurableBundleLocal(
            request.PartitionId, entries, terminal: request.AdmissionClass == 1, stage: request.Stage,
            RangeFenceWire.Read(request.Fences, request.FenceKey, request.FenceGeneration), context.CancellationToken);

        // A null reply here means the receiver could not run the bundle at all (an unresolved leadership); the
        // origin reads a not-committed batch and retries.
        return new GrpcDurableBundleResponse
        {
            BatchCommitted = reply?.BatchCommitted ?? false,
            PrepareAcknowledged = reply?.PrepareAcknowledged ?? false,
            PrepareRejection = reply?.PrepareRejection ?? 0
        };
    }

    internal async Task<GrpcDurableDecisionResponse> DurableDecisionInternal(GrpcDurableDecisionRequest request, ServerCallContext context)
    {
        HLCTimestamp transactionId = new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter);

        DurableDecisionWireReply? reply = await keyValues.DurableDecisionLocal(
            request.PartitionId, request.DecisionDelta.ToByteArray(), transactionId, request.Epoch,
            RangeFenceWire.Read(request.Fences, request.FenceKey, request.FenceGeneration), context.CancellationToken);

        return new GrpcDurableDecisionResponse
        {
            Replicated = reply?.Replicated ?? false,
            Known = reply?.Known ?? false,
            Decision = reply?.Decision ?? 0,
            AbortClass = reply?.AbortClass ?? 0
        };
    }

    internal async Task<GrpcDurableOnePhaseResponse> DurableOnePhaseInternal(GrpcDurableOnePhaseRequest request, ServerCallContext context)
    {
        HLCTimestamp transactionId = new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter);
        HLCTimestamp opId = new(request.OpIdNode, request.OpIdPhysical, request.OpIdCounter);

        // A null reply here means the receiver could not run the bundle at all (an unresolved leadership); the
        // origin reads a not-committed batch and retries.
        DurableOnePhaseWireReply? reply = await keyValues.DurableOnePhaseLocal(
            request.PartitionId, request.RecordInitDelta.ToByteArray(), request.AnchorPrepareDelta.ToByteArray(),
            request.DecisionDelta.ToByteArray(), transactionId, request.Epoch, opId,
            RangeFenceWire.Read(request.Fences, request.FenceKey, request.FenceGeneration), request.ExpectedTerm, context.CancellationToken);

        return new GrpcDurableOnePhaseResponse
        {
            BatchCommitted = reply?.BatchCommitted ?? false,
            PrepareAcknowledged = reply?.PrepareAcknowledged ?? false,
            PrepareRejection = reply?.PrepareRejection ?? 0,
            DecisionKnown = reply?.DecisionKnown ?? false,
            Decision = reply?.Decision ?? 0,
            AbortClass = reply?.AbortClass ?? 0,
            GatedVerdict = reply?.GatedVerdict ?? 0,
            BundleMs = reply?.BundleMs ?? -1
        };
    }
}

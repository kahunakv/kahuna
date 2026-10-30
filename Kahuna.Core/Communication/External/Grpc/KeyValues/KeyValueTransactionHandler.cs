
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
/// Interactive transactions, transaction scripts, operation registration and the transaction working set.
/// </summary>
internal sealed class KeyValueTransactionHandler
{
    private readonly IKahuna keyValues;

    public KeyValueTransactionHandler(IKahuna keyValues)
    {
        this.keyValues = keyValues;
    }

    /// <summary>
    /// Attempts to execute a transaction script by processing the request and returning the result.
    /// </summary>
    /// <param name="request">The transaction script request containing the script, hash, and parameters.</param>
    /// <param name="context">The server call context for the request.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the response detailing the execution outcome, elapsed time, and any additional information.</returns>
    internal async Task<GrpcTryExecuteTransactionScriptResponse> TryExecuteTransactionScriptCore(GrpcTryExecuteTransactionScriptRequest request, ServerCallContext context)
    {
        if (request.Script is null)
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };
        
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        // Forward the protobuf payload memory directly; it carries offset and length and is
        // parsed synchronously downstream, so no owned copy is materialized here.
        KeyValueTransactionResult result = await keyValues.TryExecuteTransactionScript(
            request.Script.Memory,
            request.Hash,
            GetParameters(request.Parameters),
            TransactionPriorityWire.FromGrpc(request.Priority)
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
    /// Handles the internal logic for starting a transaction with the provided request and context.
    /// </summary>
    /// <param name="request">The transaction request containing details such as unique identifier, locking type, timeout, and async release options.</param>
    /// <param name="context">The server call context associated with the gRPC request.</param>
    /// <returns>A response containing the transaction status and transaction ID (physical and counter).</returns>
    internal async Task<GrpcStartTransactionResponse> StartTransactionCore(GrpcStartTransactionRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.CoordinatorKey))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };

        (KeyValueResponseType type, TransactionHandle handle) = await keyValues.LocateAndStartTransaction(new()
        {
            CoordinatorKey = request.CoordinatorKey,
            CoordinatorPartitionId = request.HasCoordinatorPartitionId ? request.CoordinatorPartitionId : null,
            Locking = (KeyValueTransactionLocking)request.LockingType,
            Timeout = request.Timeout,
            AsyncRelease = request.AsyncRelease,
            AutoCommit = request.AutoCommit,
            ReadValidation = (ReadValidation)request.ReadValidation,
            DecisionDurability = (DecisionDurability)request.DecisionDurability,
            Priority = TransactionPriorityWire.FromGrpc(request.Priority),
            ReadTimestamp = new HLCTimestamp(request.ReadTimestampNode, request.ReadTimestampPhysical, request.ReadTimestampCounter),
            AdmissionWaitMs = request.AdmissionWaitMs,
            ConflictPolicy = TransactionConflictPolicyWire.FromGrpc(request.ConflictPolicy),
            Origin = request.HasOrigin ? request.Origin : null,
        }, context.CancellationToken);

        GrpcStartTransactionResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)type,
            TransactionIdNode = handle.TransactionId.N,
            TransactionIdPhysical = handle.TransactionId.L,
            TransactionIdCounter = handle.TransactionId.C
        };

        if (!string.IsNullOrEmpty(handle.CoordinatorKey))
            response.CoordinatorKey = handle.CoordinatorKey;

        return response;
    }

    /// <summary>
    /// Handles the internal logic for committing a transaction in the KeyValues service.
    /// </summary>
    /// <param name="request">The transaction commit request containing necessary details such as unique ID and keys.</param>
    /// <param name="context">The server call context providing metadata and call-specific context.</param>
    /// <returns>Returns a response indicating the outcome of the transaction commit operation.</returns>
    internal async Task<GrpcCommitTransactionResponse> CommitTransactionCore(GrpcCommitTransactionRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.CoordinatorKey))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };

        HLCTimestamp transactionId = new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter);
        // Carry the caller's record anchor: after the coordinator session is gone, it is the only route to the
        // durable decision, so a commit retry that supplies it resolves the outcome instead of unknown Errored.
        TransactionHandle handle = new(transactionId, request.CoordinatorKey,
            request.HasRecordAnchorKey ? request.RecordAnchorKey : null);

        // Commit returns the coordinator's canonical record anchor captured from the frozen finalize
        // snapshot under the fence, so there is no race window between reading the anchor and freezing the
        // working set. The anchor is null when the transaction confirmed no persistent write.
        TransactionCommitAnswer answer = await keyValues.LocateAndCommitTransaction(handle, context.CancellationToken);

        GrpcCommitTransactionResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)answer.Type,
            Refusal = (int)answer.Refusal,
            CommitTimestampNode = answer.CommitTimestamp.N,
            CommitTimestampPhysical = answer.CommitTimestamp.L,
            CommitTimestampCounter = answer.CommitTimestamp.C
        };

        if (answer.RecordAnchorKey is not null)
            response.RecordAnchorKey = answer.RecordAnchorKey;

        if (answer.Reason is not null)
            response.Reason = answer.Reason;

        return response;
    }

    /// <summary>
    /// Handles the internal rollback of a transaction based on the provided request and context.
    /// </summary>
    /// <param name="request">The rollback transaction request containing details such as transaction ID and related keys.</param>
    /// <param name="context">The server call context for the RPC request.</param>
    /// <returns>A task representing the operation, returning a <see cref="GrpcRollbackTransactionResponse"/> indicating the result of the rollback operation.</returns>
    internal async Task<GrpcRollbackTransactionResponse> RollbackTransactionCore(GrpcRollbackTransactionRequest request, ServerCallContext context)
    {
        if (string.IsNullOrEmpty(request.CoordinatorKey))
            return new()
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput
            };

        TransactionHandle handle = new(
            new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.CoordinatorKey,
            // The anchor lets a rollback retry consult the durable decision (a decided commit cannot be undone).
            request.HasRecordAnchorKey ? request.RecordAnchorKey : null
        );

        KeyValueResponseType type = await keyValues.LocateAndRollbackTransaction(handle, context.CancellationToken);

        GrpcRollbackTransactionResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)type,
        };

        return response;
    }

    /// <summary>
    /// Inter-node landing point for a register-remote operation registration. Routed through the
    /// locator rather than straight into the node-local session table: under replica placement the
    /// sender may have picked a coordinator-partition replica that does not hold the session, and a
    /// hosting non-leader receiver must redirect once to its accurately-resolved local leader
    /// instead of answering RejectedSessionClosed from the wrong node.
    /// </summary>
    internal async Task<GrpcBeginOperationResponse> BeginOperationInternal(GrpcBeginOperationRequest request, ServerCallContext context)
    {
        HLCTimestamp transactionId = new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter);
        TransactionOperationId operationId = new(request.OperationIdHigh, request.OperationIdLow);
        byte[]? digest = request.HasPayloadDigest ? request.PayloadDigest.ToByteArray() : null;

        (OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey, TransactionConflictPolicy conflictPolicy) =
            await keyValues.LocateAndBeginOperation(request.CoordinatorKey, transactionId, operationId, (OperationKind)request.Kind, digest, context.CancellationToken);

        GrpcBeginOperationResponse response = new()
        {
            Outcome = (GrpcOperationRegistrationOutcome)outcome,
            CachedType = (GrpcKeyValueResponseType)cachedType,
            CachedRevision = cachedRevision,
            CachedTimestampNode = cachedTimestamp.N,
            CachedTimestampPhysical = cachedTimestamp.L,
            CachedTimestampCounter = cachedTimestamp.C,
            ConflictPolicy = TransactionConflictPolicyWire.ToGrpc(conflictPolicy)
        };

        if (recordAnchorKey is not null)
            response.RecordAnchorKey = recordAnchorKey;

        return response;
    }

    /// <summary>Inter-node landing point for a register-remote operation completion.</summary>
    internal async Task<GrpcCompleteOperationResponse> CompleteOperationInternal(GrpcCompleteOperationRequest request, ServerCallContext context)
    {
        HLCTimestamp transactionId = new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter);
        TransactionOperationId operationId = new(request.OperationIdHigh, request.OperationIdLow);

        OperationCompletionPayload payload = FromGrpcCompleteOperationRequest(request);

        (KeyValueResponseType outcome, string? recordAnchorKey) = await keyValues.CompleteOperationInbound(request.CoordinatorKey, transactionId, operationId, payload);

        // This landing point created the shell from the wire and nothing downstream retains it: the
        // fold copies what it keeps, and a redirect re-serializes before sending. Recycle it here.
        OperationCompletionPayloadPool.Return(payload);

        GrpcCompleteOperationResponse response = new()
        {
            Acknowledged = outcome == KeyValueResponseType.Set
        };
        if (recordAnchorKey is not null)
            response.RecordAnchorKey = recordAnchorKey;

        return response;
    }

    /// <summary>
    /// Restores the completion payload from the inter-node request — the inverse of the sender-side
    /// mapping. Every effect-bearing field must be restored here; a dropped field silently narrows the
    /// coordinator's working set. The shell is rented; the caller recycles it after the fold.
    /// </summary>
    internal static OperationCompletionPayload FromGrpcCompleteOperationRequest(GrpcCompleteOperationRequest request)
    {
        OperationCompletionPayload payload = OperationCompletionPayloadPool.Rent();
        payload.ModifiedKey = request.HasModifiedKey ? request.ModifiedKey : null;
        payload.AcquiredPointLock = request.HasAcquiredPointLock ? request.AcquiredPointLock : null;
        payload.ReleasedPointLock = request.HasReleasedPointLock ? request.ReleasedPointLock : null;
        payload.AcquiredPrefixLock = request.HasAcquiredPrefixLock ? request.AcquiredPrefixLock : null;
        payload.ReleasedPrefixLock = request.HasReleasedPrefixLock ? request.ReleasedPrefixLock : null;
        payload.AcquiredRangeLock = request.AcquiredRangeLock is null ? null : (ToRangeLockKey(request.AcquiredRangeLock), (RangeLockMode)request.AcquiredRangeLock.Mode);
        payload.ReleasedRangeLock = request.ReleasedRangeLock is null ? null : ToRangeLockKey(request.ReleasedRangeLock);
        payload.AcquiredRangeLocks = request.AcquiredRangeLocks.Count == 0 ? null : request.AcquiredRangeLocks.Select(r => (ToRangeLockKey(r), (RangeLockMode)r.Mode)).ToList();
        payload.Read = request.Read is null ? null : ToReadKey(request.Read);
        payload.ReadObservations = request.ReadObservations.Count == 0 ? null : request.ReadObservations.Select(ToReadKey).ToList();
        payload.ModifiedKeys = request.ModifiedKeys.Count == 0 ? null : request.ModifiedKeys.Select(m => (m.Key, (KeyValueDurability)m.Durability)).ToList();
        payload.StagedMutations = request.StagedMutations.Count == 0 ? null : request.StagedMutations.Select(FromGrpcStagedMutation).ToList();
        payload.AcquiredPointLocks = request.AcquiredPointLocks.Count == 0 ? null : request.AcquiredPointLocks.Select(l => (l.Key, (KeyValueDurability)l.Durability)).ToList();
        payload.LockGrantTerms = request.LockGrantTerms.Count == 0 ? null : request.LockGrantTerms.Select(g => new LockGrantTerm(g.PartitionId, g.Term, g.RoutingKey)).ToList();
        payload.LockExpiresMs = request.LockExpiresMs;
        payload.Durability = (KeyValueDurability)request.Durability;
        payload.CachedType = (KeyValueResponseType)request.CachedType;
        payload.CachedRevision = request.CachedRevision;
        payload.CachedTimestamp = new(request.CachedTimestampNode, request.CachedTimestampPhysical, request.CachedTimestampCounter);
        return payload;
    }

    /// <summary>Inter-node landing point for a working-set query against the node-local session.</summary>
    internal Task<GrpcGetTransactionWorkingSetResponse> GetTransactionWorkingSetInternal(GrpcGetTransactionWorkingSetRequest request, ServerCallContext context)
    {
        HLCTimestamp transactionId = new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter);

        TransactionWorkingSet? workingSet = keyValues.GetTransactionWorkingSet(transactionId);

        GrpcGetTransactionWorkingSetResponse response = new()
        {
            Found = workingSet is not null,
            WorkingSet = workingSet is null ? new GrpcTransactionWorkingSet() : ToGrpcWorkingSet(workingSet)
        };

        return Task.FromResult(response);
    }

    /// <summary>Inter-node landing point for a close-and-snapshot against the node-local session.</summary>
    internal async Task<GrpcCloseTransactionResponse> CloseTransactionCore(GrpcCloseTransactionRequest request, ServerCallContext context)
    {
        HLCTimestamp transactionId = new(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter);

        (KeyValueResponseType type, TransactionWorkingSet? workingSet) = await keyValues.CloseTransaction(transactionId, context.CancellationToken);

        GrpcCloseTransactionResponse response = new()
        {
            Type = (GrpcKeyValueResponseType)type,
            HasWorkingSet = workingSet is not null,
            WorkingSet = workingSet is null ? new GrpcTransactionWorkingSet() : ToGrpcWorkingSet(workingSet)
        };

        return response;
    }

    private static RangeLockKey ToRangeLockKey(GrpcTransactionRangeLock g) =>
        new(g.Prefix, g.HasStartKey ? g.StartKey : null, g.StartInclusive, g.HasEndKey ? g.EndKey : null, g.EndInclusive, (KeyValueDurability)g.Durability);

    private static KeyValueTransactionReadKey ToReadKey(GrpcTransactionReadKey g) =>
        new() { Key = g.Key, Durability = (KeyValueDurability)g.Durability, Exists = g.Exists, Revision = g.Revision };

    // Set-versus-delete comes from the explicit State field; an absent Value stays distinct from an empty one.
    // A sender from a build without the field sends STATE_UNDEFINED, so fall back to the old presence-derived
    // rule for exactly that case: absent value = deletion, present value — even empty — = set.
    internal static StagedMutationEffect FromGrpcStagedMutation(GrpcStagedMutationEffect g) =>
        new(g.Key,
            g.HasValue ? g.Value.ToByteArray() : null,
            g.State != GrpcKeyValueState.StateUndefined
                ? (KeyValueState)g.State
                : g.HasValue ? KeyValueState.Set : KeyValueState.Deleted,
            g.Revision, g.ExpiresMs, g.NoRevision,
            new HLCTimestamp(g.StagedAtNode, g.StagedAtPhysical, g.StagedAtCounter));

    private static GrpcTransactionWorkingSet ToGrpcWorkingSet(TransactionWorkingSet ws)
    {
        GrpcTransactionWorkingSet result = new()
        {
            PendingOperationCount = ws.PendingOperationCount
        };

        if (ws.RecordAnchorKey is not null)
            result.RecordAnchorKey = ws.RecordAnchorKey;

        foreach (KeyValueTransactionModifiedKey m in ws.ModifiedKeys)
            result.ModifiedKeys.Add(new GrpcTransactionModifiedKey { Key = m.Key ?? "", Durability = (GrpcKeyValueDurability)m.Durability });

        foreach (KeyValueTransactionModifiedKey m in ws.AcquiredLocks)
            result.AcquiredLocks.Add(new GrpcTransactionModifiedKey { Key = m.Key ?? "", Durability = (GrpcKeyValueDurability)m.Durability });

        foreach (KeyValueTransactionModifiedKey m in ws.AcquiredPrefixLocks)
            result.AcquiredPrefixLocks.Add(new GrpcTransactionModifiedKey { Key = m.Key ?? "", Durability = (GrpcKeyValueDurability)m.Durability });

        foreach (KeyValueTransactionRangeLock r in ws.AcquiredRangeLocks)
        {
            GrpcTransactionRangeLock grpcRange = new()
            {
                Prefix = r.Prefix ?? "",
                StartInclusive = r.StartInclusive,
                EndInclusive = r.EndInclusive,
                Durability = (GrpcKeyValueDurability)r.Durability,
                Mode = (GrpcRangeLockMode)r.Mode
            };
            if (r.StartKey is not null) grpcRange.StartKey = r.StartKey;
            if (r.EndKey is not null) grpcRange.EndKey = r.EndKey;
            result.AcquiredRangeLocks.Add(grpcRange);
        }

        foreach (KeyValueTransactionReadKey rk in ws.ReadKeys)
            result.ReadKeys.Add(new GrpcTransactionReadKey { Key = rk.Key ?? "", Durability = (GrpcKeyValueDurability)rk.Durability, Exists = rk.Exists, Revision = rk.Revision });

        return result;
    }
}


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

/// <summary>Interactive transaction session forwarding and its wire conversions.</summary>
public partial class GrpcInterNodeCommunication
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="node"></param>
    /// <param name="options"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public async Task<(KeyValueResponseType, TransactionHandle)> StartTransaction(string node, KeyValueTransactionOptions options, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcStartTransactionRequest request = new()
        {
            CoordinatorKey = options.CoordinatorKey,
            LockingType = (GrpcLockingType)options.Locking,
            Timeout = options.Timeout,
            AsyncRelease = options.AsyncRelease,
            AutoCommit = options.AutoCommit,
            ReadValidation = (GrpcReadValidation)options.ReadValidation,
            DecisionDurability = (GrpcDecisionDurability)options.DecisionDurability,
            Priority = TransactionPriorityWire.ToGrpc(options.Priority),
            ReadTimestampNode = options.ReadTimestamp.N,
            ReadTimestampPhysical = options.ReadTimestamp.L,
            ReadTimestampCounter = options.ReadTimestamp.C,
            // Carried across the hop so a Begin that lands on a follower keeps the caller's own door-wait
            // instead of silently falling back to the leader's server default.
            AdmissionWaitMs = options.AdmissionWaitMs,
            ConflictPolicy = TransactionConflictPolicyWire.ToGrpc(options.ConflictPolicy),
        };

        if (options.Origin is not null)
            request.Origin = options.Origin;

        if (options.CoordinatorPartitionId is { } coordinatorPartitionId)
            request.CoordinatorPartitionId = coordinatorPartitionId;

        request.MutationLimit = options.MutationLimit;

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "StartTransaction", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            // The leader was resolved but died or became unreachable before answering. No session
            // was opened, so a retry (ideally against another node) is unconditionally safe — tell
            // the caller that instead of leaking the transport failure as a server error.
            return (KeyValueResponseType.MustRetry, TransactionHandle.None);
        }

        GrpcStartTransactionResponse remoteResponse = response.StartTransaction!;


        HLCTimestamp transactionId = new(remoteResponse.TransactionIdNode, remoteResponse.TransactionIdPhysical, remoteResponse.TransactionIdCounter);

        // The leader's key is the one the session is routed by. An older leader does not return it; the key
        // sent is then the session's key, because this node already pinned it before forwarding.
        string coordinatorKey = remoteResponse.HasCoordinatorKey && !string.IsNullOrEmpty(remoteResponse.CoordinatorKey)
            ? remoteResponse.CoordinatorKey
            : options.CoordinatorKey;

        return (
            (KeyValueResponseType)remoteResponse.Type,
            new TransactionHandle(transactionId, coordinatorKey)
        );
    }

    public async Task<TransactionCommitAnswer> CommitTransaction(string node, TransactionHandle handle, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcCommitTransactionRequest request = new()
        {
            CoordinatorKey = handle.CoordinatorKey,
            TransactionIdNode = handle.TransactionId.N,
            TransactionIdPhysical = handle.TransactionId.L,
            TransactionIdCounter = handle.TransactionId.C
        };

        // Forward the record anchor so a commit routed to the anchor's node reaches the durable decision even
        // after the coordinating session was evicted or its node failed.
        if (handle.RecordAnchorKey is not null)
            request.RecordAnchorKey = handle.RecordAnchorKey;

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "CommitTransaction", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            // The commit may or may not have been applied before the transport died — MustRetry
            // preserves that indeterminacy: the commit path is idempotent and a retry (carrying the
            // record anchor when one exists) consults the session or the durable decision to
            // resolve the true outcome. Hand the caller's own anchor back so the retry keeps it.
            return new TransactionCommitAnswer(KeyValueResponseType.MustRetry, handle.RecordAnchorKey, CommitRefusalClass.Unresolved, "The forwarded commit was not answered");
        }

        GrpcCommitTransactionResponse remoteResponse = response.CommitTransaction!;

        return new TransactionCommitAnswer(
            (KeyValueResponseType)remoteResponse.Type,
            remoteResponse.HasRecordAnchorKey ? remoteResponse.RecordAnchorKey : null,
            (CommitRefusalClass)remoteResponse.Refusal,
            remoteResponse.HasReason ? remoteResponse.Reason : null,
            new HLCTimestamp(remoteResponse.CommitTimestampNode, remoteResponse.CommitTimestampPhysical, remoteResponse.CommitTimestampCounter));
    }

    public async Task<KeyValueResponseType> RollbackTransaction(string node, TransactionHandle handle, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcRollbackTransactionRequest request = new()
        {
            CoordinatorKey = handle.CoordinatorKey,
            TransactionIdNode = handle.TransactionId.N,
            TransactionIdPhysical = handle.TransactionId.L,
            TransactionIdCounter = handle.TransactionId.C
        };

        // Forward the record anchor so a rollback routed to the anchor's node can consult the durable decision.
        if (handle.RecordAnchorKey is not null)
            request.RecordAnchorKey = handle.RecordAnchorKey;

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "RollbackTransaction", node).ConfigureAwait(false);
        if (!response.IsAnswered)
        {
            // Same indeterminacy contract as commit: rollback is idempotent and a retry consults
            // the session or the durable decision (via the anchor), so MustRetry is safe and a
            // decided commit can never be undone by the retried rollback.
            return KeyValueResponseType.MustRetry;
        }

        GrpcRollbackTransactionResponse remoteResponse = response.RollbackTransaction!;


        return (KeyValueResponseType)remoteResponse.Type;
    }
    
    public async Task<(OperationRegistrationOutcome outcome, KeyValueResponseType cachedType, long cachedRevision, HLCTimestamp cachedTimestamp, string? recordAnchorKey, TransactionConflictPolicy conflictPolicy)> BeginOperation(string node, string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationKind kind, byte[]? payloadDigest, CancellationToken cancellationToken, int itemCount = 0)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcBeginOperationRequest request = new()
        {
            CoordinatorKey = coordinatorKey,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low,
            Kind = (GrpcOperationKind)kind,
            ItemCount = itemCount
        };

        if (payloadDigest is not null)
            request.PayloadDigest = ByteString.CopyFrom(payloadDigest);

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "BeginOperation", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return (OperationRegistrationOutcome.AlreadyPending, KeyValueResponseType.MustRetry, 0, HLCTimestamp.Zero, null, TransactionConflictPolicy.Normal);

        GrpcBeginOperationResponse remoteResponse = response.BeginOperation!;

        HLCTimestamp cachedTimestamp = new(remoteResponse.CachedTimestampNode, remoteResponse.CachedTimestampPhysical, remoteResponse.CachedTimestampCounter);

        return (
            (OperationRegistrationOutcome)remoteResponse.Outcome,
            (KeyValueResponseType)remoteResponse.CachedType,
            remoteResponse.CachedRevision,
            cachedTimestamp,
            remoteResponse.HasRecordAnchorKey ? remoteResponse.RecordAnchorKey : null,
            TransactionConflictPolicyWire.FromGrpc(remoteResponse.ConflictPolicy)
        );
    }

    public async Task<(KeyValueResponseType outcome, string? anchor)> CompleteOperation(string node, string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcCompleteOperationRequest request = ToGrpcCompleteOperationRequest(coordinatorKey, transactionId, operationId, payload);

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "CompleteOperation", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return (KeyValueResponseType.MustRetry, null);

        GrpcCompleteOperationResponse remoteResponse = response.CompleteOperation!;
        KeyValueResponseType outcome = remoteResponse.Acknowledged ? KeyValueResponseType.Set : KeyValueResponseType.MustRetry;
        return (outcome, remoteResponse.HasRecordAnchorKey ? remoteResponse.RecordAnchorKey : null);
    }

    /// <summary>
    /// Maps a completion payload onto the inter-node request. Every effect-bearing field of
    /// <see cref="OperationCompletionPayload"/> must be carried here and restored on the receiving node —
    /// a dropped field silently narrows the coordinator's working set (e.g. a missing held-lock set makes
    /// commit skip its mutations entirely).
    /// </summary>
    internal static GrpcCompleteOperationRequest ToGrpcCompleteOperationRequest(string coordinatorKey, HLCTimestamp transactionId, TransactionOperationId operationId, OperationCompletionPayload payload)
    {
        GrpcCompleteOperationRequest request = new()
        {
            CoordinatorKey = coordinatorKey,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            OperationIdHigh = operationId.High,
            OperationIdLow = operationId.Low,
            Durability = (GrpcKeyValueDurability)payload.Durability,
            LockExpiresMs = payload.LockExpiresMs,
            CachedType = (GrpcKeyValueResponseType)payload.CachedType,
            CachedRevision = payload.CachedRevision,
            CachedTimestampNode = payload.CachedTimestamp.N,
            CachedTimestampPhysical = payload.CachedTimestamp.L,
            CachedTimestampCounter = payload.CachedTimestamp.C
        };

        if (payload.ModifiedKey is not null) request.ModifiedKey = payload.ModifiedKey;
        if (payload.AcquiredPointLock is not null) request.AcquiredPointLock = payload.AcquiredPointLock;
        if (payload.ReleasedPointLock is not null) request.ReleasedPointLock = payload.ReleasedPointLock;
        if (payload.AcquiredPrefixLock is not null) request.AcquiredPrefixLock = payload.AcquiredPrefixLock;
        if (payload.ReleasedPrefixLock is not null) request.ReleasedPrefixLock = payload.ReleasedPrefixLock;
        if (payload.AcquiredRangeLock is { } acquiredRange) request.AcquiredRangeLock = ToGrpcRangeLock(acquiredRange.Range, acquiredRange.Mode);
        if (payload.ReleasedRangeLock is { } releasedRange) request.ReleasedRangeLock = ToGrpcRangeLock(releasedRange, RangeLockMode.Exclusive);
        if (payload.AcquiredRangeLocks is not null)
        {
            ReserveItemCapacity(request.AcquiredRangeLocks, payload.AcquiredRangeLocks.Count);
            foreach ((RangeLockKey Range, RangeLockMode Mode) acquired in payload.AcquiredRangeLocks)
                request.AcquiredRangeLocks.Add(ToGrpcRangeLock(acquired.Range, acquired.Mode));
        }
        if (payload.Read is not null) request.Read = ToGrpcReadKey(payload.Read);
        if (payload.ReadObservations is not null)
        {
            ReserveItemCapacity(request.ReadObservations, payload.ReadObservations.Count);
            foreach (KeyValueTransactionReadKey read in payload.ReadObservations)
                request.ReadObservations.Add(ToGrpcReadKey(read));
        }

        if (payload.ModifiedKeys is not null)
        {
            ReserveItemCapacity(request.ModifiedKeys, payload.ModifiedKeys.Count);
            foreach ((string Key, KeyValueDurability Durability) m in payload.ModifiedKeys)
                request.ModifiedKeys.Add(new GrpcTransactionModifiedKey { Key = m.Key, Durability = (GrpcKeyValueDurability)m.Durability });
        }

        if (payload.StagedMutations is not null)
        {
            ReserveItemCapacity(request.StagedMutations, payload.StagedMutations.Count);
            foreach (StagedMutationEffect effect in payload.StagedMutations)
                request.StagedMutations.Add(ToGrpcStagedMutation(effect));
        }

        if (payload.AcquiredPointLocks is not null)
        {
            ReserveItemCapacity(request.AcquiredPointLocks, payload.AcquiredPointLocks.Count);
            foreach ((string Key, KeyValueDurability Durability) l in payload.AcquiredPointLocks)
                request.AcquiredPointLocks.Add(new GrpcTransactionModifiedKey { Key = l.Key, Durability = (GrpcKeyValueDurability)l.Durability });
        }

        if (payload.LockGrantTerms is not null)
        {
            ReserveItemCapacity(request.LockGrantTerms, payload.LockGrantTerms.Count);
            foreach (LockGrantTerm grant in payload.LockGrantTerms)
                request.LockGrantTerms.Add(new GrpcLockGrantTerm { PartitionId = grant.PartitionId, Term = grant.Term, RoutingKey = grant.RoutingKey });
        }

        return request;
    }

    public async Task<TransactionWorkingSet?> GetTransactionWorkingSet(string node, string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcGetTransactionWorkingSetRequest request = new()
        {
            CoordinatorKey = coordinatorKey,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "GetTransactionWorkingSet", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return null;

        GrpcGetTransactionWorkingSetResponse remoteResponse = response.GetTransactionWorkingSet!;

        return remoteResponse.Found ? FromGrpcWorkingSet(remoteResponse.WorkingSet) : null;
    }

    public async Task<(KeyValueResponseType, TransactionWorkingSet?)> CloseTransaction(string node, string coordinatorKey, HLCTimestamp transactionId, CancellationToken cancellationToken)
    {
        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcCloseTransactionRequest request = new()
        {
            CoordinatorKey = coordinatorKey,
            TransactionIdNode = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C
        };

        GrpcServerBatcherResponse response = await ForwardAsync(batcher.Enqueue(request), "CloseTransaction", node).ConfigureAwait(false);
        if (!response.IsAnswered)
            return (KeyValueResponseType.MustRetry, null);

        GrpcCloseTransactionResponse remoteResponse = response.CloseTransaction!;

        return (
            (KeyValueResponseType)remoteResponse.Type,
            remoteResponse.HasWorkingSet ? FromGrpcWorkingSet(remoteResponse.WorkingSet) : null
        );
    }

    private static GrpcTransactionRangeLock ToGrpcRangeLock(RangeLockKey range, RangeLockMode mode)
    {
        GrpcTransactionRangeLock grpc = new()
        {
            Prefix = range.Prefix,
            StartInclusive = range.StartInclusive,
            EndInclusive = range.EndInclusive,
            Durability = (GrpcKeyValueDurability)range.Durability,
            Mode = (GrpcRangeLockMode)mode
        };

        if (range.StartKey is not null) grpc.StartKey = range.StartKey;
        if (range.EndKey is not null) grpc.EndKey = range.EndKey;

        return grpc;
    }

    private static GrpcTransactionReadKey ToGrpcReadKey(KeyValueTransactionReadKey read) => new()
    {
        Key = read.Key ?? "",
        Durability = (GrpcKeyValueDurability)read.Durability,
        Exists = read.Exists,
        Revision = read.Revision
    };

    // Set-versus-delete travels in the explicit State field — a set may carry a null value (the key exists and
    // holds nothing), so value presence cannot stand in for it. A null Value leaves the optional proto field
    // absent so the receiver can distinguish it from an empty value. The staged value is a committed, immutable
    // snapshot, so wrapping it without a copy is safe.
    internal static GrpcStagedMutationEffect ToGrpcStagedMutation(StagedMutationEffect effect)
    {
        GrpcStagedMutationEffect grpc = new()
        {
            Key = effect.Key,
            Revision = effect.Revision,
            ExpiresMs = effect.ExpiresMs,
            NoRevision = effect.NoRevision,
            State = (GrpcKeyValueState)effect.State,
            StagedAtNode = effect.StagedAt.N,
            StagedAtPhysical = effect.StagedAt.L,
            StagedAtCounter = effect.StagedAt.C
        };

        if (effect.Value is not null)
            grpc.Value = UnsafeByteOperations.UnsafeWrap(effect.Value);

        return grpc;
    }

    private static TransactionWorkingSet FromGrpcWorkingSet(GrpcTransactionWorkingSet grpc)
    {
        return new()
        {
            ModifiedKeys = grpc.ModifiedKeys.Select(FromGrpcModifiedKey).ToList(),
            AcquiredLocks = grpc.AcquiredLocks.Select(FromGrpcModifiedKey).ToList(),
            AcquiredPrefixLocks = grpc.AcquiredPrefixLocks.Select(FromGrpcModifiedKey).ToList(),
            AcquiredRangeLocks = grpc.AcquiredRangeLocks.Select(FromGrpcRangeLock).ToList(),
            ReadKeys = grpc.ReadKeys.Select(FromGrpcReadKey).ToList(),
            RecordAnchorKey = grpc.HasRecordAnchorKey ? grpc.RecordAnchorKey : null,
            PendingOperationCount = grpc.PendingOperationCount
        };
    }

    private static KeyValueTransactionModifiedKey FromGrpcModifiedKey(GrpcTransactionModifiedKey g) =>
        new() { Key = g.Key, Durability = (KeyValueDurability)g.Durability };

    private static KeyValueTransactionReadKey FromGrpcReadKey(GrpcTransactionReadKey g) =>
        new() { Key = g.Key, Durability = (KeyValueDurability)g.Durability, Exists = g.Exists, Revision = g.Revision };

    private static KeyValueTransactionRangeLock FromGrpcRangeLock(GrpcTransactionRangeLock g) =>
        new()
        {
            Prefix = g.Prefix,
            StartKey = g.HasStartKey ? g.StartKey : null,
            StartInclusive = g.StartInclusive,
            EndKey = g.HasEndKey ? g.EndKey : null,
            EndInclusive = g.EndInclusive,
            Durability = (KeyValueDurability)g.Durability,
            Mode = (RangeLockMode)g.Mode
        };
}


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

/// <summary>Snapshot hold, history hold, fingerprint, backup part and range quiesce forwarding.</summary>
public partial class GrpcInterNodeCommunication
{
    public async Task<(KeyValueResponseType Type, string HoldId, HLCTimestamp LeaseExpiry)>
        AcquireSnapshotHold(string node, string holderId, HLCTimestamp timestamp, int leaseMs, CancellationToken cancellationToken)
    {
        GrpcAcquireSnapshotHoldRequest request = new()
        {
            HolderId          = holderId,
            TimestampNode     = timestamp.N,
            TimestampPhysical = timestamp.L,
            TimestampCounter  = (uint)timestamp.C,
            LeaseMs           = leaseMs
        };

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse batchResponse = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "AcquireSnapshotHold", node).ConfigureAwait(false);
        if (!batchResponse.IsAnswered)
            return UnansweredResults.AcquireSnapshotHold;

        GrpcAcquireSnapshotHoldResponse r = batchResponse.AcquireSnapshotHold!;
        return (
            (KeyValueResponseType)r.Type,
            r.HoldId,
            new HLCTimestamp(r.LeaseExpiryNode, r.LeaseExpiryPhysical, r.LeaseExpiryCounter)
        );
    }

    public async Task<(KeyValueResponseType Type, HLCTimestamp LeaseExpiry)>
        RenewSnapshotHold(string node, string holdId, int leaseMs, CancellationToken cancellationToken)
    {
        GrpcRenewSnapshotHoldRequest request = new()
        {
            HoldId  = holdId,
            LeaseMs = leaseMs
        };

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse batchResponse = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "RenewSnapshotHold", node).ConfigureAwait(false);
        if (!batchResponse.IsAnswered)
            return UnansweredResults.RenewSnapshotHold;

        GrpcRenewSnapshotHoldResponse r = batchResponse.RenewSnapshotHold!;
        return (
            (KeyValueResponseType)r.Type,
            new HLCTimestamp(r.LeaseExpiryNode, r.LeaseExpiryPhysical, r.LeaseExpiryCounter)
        );
    }

    public async Task<KeyValueResponseType>
        ReleaseSnapshotHold(string node, string holdId, CancellationToken cancellationToken)
    {
        GrpcReleaseSnapshotHoldRequest request = new() { HoldId = holdId };

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse batchResponse = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "ReleaseSnapshotHold", node).ConfigureAwait(false);
        if (!batchResponse.IsAnswered)
            return UnansweredResults.ReleaseSnapshotHold;

        return (KeyValueResponseType)batchResponse.ReleaseSnapshotHold!.Type;
    }

    public async Task<(KeyValueResponseType Type, HLCTimestamp Floor, int LiveHolds)>
        GetSnapshotFloor(string node, CancellationToken cancellationToken)
    {
        GrpcGetSnapshotFloorRequest request = new();

        GrpcServerBatcher batcher = GetSharedBatcher(node);

        GrpcServerBatcherResponse batchResponse = await ForwardAsync(batcher.Enqueue(request).WaitAsync(cancellationToken), "GetSnapshotFloor", node).ConfigureAwait(false);
        if (!batchResponse.IsAnswered)
        {
            // The floor read is side-effect free, so a transport failure mid-forward is unconditionally
            // safe to answer with the endpoint's own typed refusal — the caller re-resolves the meta
            // leader and asks again — instead of escaping as an exception the external surfaces would
            // report as an unclassifiable server error.
            return UnansweredResults.GetSnapshotFloor;
        }

        GrpcGetSnapshotFloorResponse r = batchResponse.GetSnapshotFloor!;

        // A response from a server predating the Type field deserializes as TypeSet (the proto
        // default); it carried real data, so treat it as an authoritative answer.
        KeyValueResponseType type = (KeyValueResponseType)r.Type;
        if (type == KeyValueResponseType.Set)
            type = KeyValueResponseType.Get;

        return (
            type,
            new HLCTimestamp(r.EffectiveFloorNode, r.EffectiveFloorPhysical, r.EffectiveFloorCounter),
            r.LiveHolds
        );
    }

    /// <summary>
    /// Forwards a history hold request to the meta-partition leader as a plain unary call on the shared channel: it is
    /// rare control traffic with nothing for a batching lane to coalesce.
    /// </summary>
    public async Task<HistoryHoldAnswer> HistoryHold(string node, HistoryHoldRequest request, CancellationToken cancellationToken)
    {
        try
        {
            global::KeyValuer.KeyValuerClient client = new(SharedChannels.GetChannel(ResolveNodeUrl(node), transportSecurity));

            GrpcHistoryHoldResponse response = await client
                .HistoryHoldAsync(HistoryHoldWire.ToGrpc(request), headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return HistoryHoldWire.FromGrpc(response);
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure("HistoryHold", node, ex.StatusCode);

            return UnansweredResults.HistoryHold;
        }
    }

    /// <summary>
    /// Reads a peer's apply fingerprint for one partition as a plain unary call on the shared channel: the
    /// read is rare (a promotion, a split) and has nothing for a batching lane to coalesce.
    /// </summary>
    public async Task<(KeyValueResponseType Type, KeyValueApplyFingerprint Fingerprint)>
        GetPartitionApplyFingerprint(string node, int partitionId, CancellationToken cancellationToken)
    {
        GrpcGetPartitionApplyFingerprintRequest request = new() { PartitionId = partitionId };

        GrpcGetPartitionApplyFingerprintResponse response;

        try
        {
            global::KeyValuer.KeyValuerClient client = new(SharedChannels.GetChannel(ResolveNodeUrl(node), transportSecurity));

            response = await client
                .GetPartitionApplyFingerprintAsync(request, headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure("GetPartitionApplyFingerprint", node, ex.StatusCode);

            return UnansweredResults.GetPartitionApplyFingerprint;
        }

        return (
            (KeyValueResponseType)response.Type,
            new KeyValueApplyFingerprint(response.AppliedLogId, response.CommittedHeads, response.LiveIntents)
        );
    }

    /// <summary>
    /// Asks a node which replicated log types it applies, as a plain unary call on the shared channel: a partition leader
    /// asks when it takes the leadership or the replicas change, and has nothing to coalesce. A node of an older build
    /// answers that it does not implement the call, and any failure answers null: the leader then reads the node as one
    /// that does not apply a new type, and asks again later.
    /// </summary>
    public async Task<IReadOnlyList<string>?> GetAppliedLogTypes(string node, CancellationToken cancellationToken)
    {
        try
        {
            global::KeyValuer.KeyValuerClient client = new(SharedChannels.GetChannel(ResolveNodeUrl(node), transportSecurity));

            GrpcGetAppliedLogTypesResponse response = await client
                .GetAppliedLogTypesAsync(new GrpcGetAppliedLogTypesRequest(), headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return [.. response.LogTypes];
        }
        catch (RpcException ex)
        {
            if (ex.StatusCode != StatusCode.Unimplemented)
                LogForwardingTransportFailure("GetAppliedLogTypes", node, ex.StatusCode);

            return UnansweredResults.GetAppliedLogTypes;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException && !cancellationToken.IsCancellationRequested)
        {
            // The channel could not be opened (a stopped node, a bad address): no answer.
            return UnansweredResults.GetAppliedLogTypes;
        }
    }

    /// <summary>
    /// Asks a node for its share of a cluster-wide backup on a bidirectional stream: the node sends each artifact
    /// in chunks, a full share waits for Proceed once every share is captured, then the node sends the result. A transport failure throws (the composing node names the share); a cancellation
    /// the caller asked for surfaces as one, not as a transport failure.
    /// </summary>
    public async Task<BackupPartResult> CaptureBackupPart(
        string node, BackupPartRequest request, IBackupPartSink sink, CancellationToken cancellationToken)
    {
        global::Backups.BackupsClient client = new(SharedChannels.GetChannel(ResolveNodeUrl(node), transportSecurity));

        try
        {
            using AsyncDuplexStreamingCall<GrpcBackupPartControl, GrpcBackupPartChunk> call = client.CaptureBackupPart(
                headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken);

            await call.RequestStream.WriteAsync(
                new GrpcBackupPartControl { Request = BackupPartWire.ToGrpc(request) }).ConfigureAwait(false);

            BackupPartResult result = await BackupPartWire.ReceiveAsync(
                call.ResponseStream, call.RequestStream, sink, node, cancellationToken).ConfigureAwait(false);

            await call.RequestStream.CompleteAsync().ConfigureAwait(false);
            return result;
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException("The backup share capture was cancelled.", ex, cancellationToken);
        }
    }

    /// <summary>
    /// Asks a node whether it still holds a prepared intent for one transaction attempt on a key; a plain
    /// unary call, like the fingerprint read it accompanies.
    /// </summary>
    public async Task<(KeyValueResponseType Type, bool Held, long AppliedLogId)>
        GetPreparedIntentPresence(string node, int partitionId, HLCTimestamp transactionId, long epoch, string key, CancellationToken cancellationToken)
    {
        GrpcGetPreparedIntentPresenceRequest request = new()
        {
            PartitionId          = partitionId,
            TransactionIdNode    = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter = transactionId.C,
            Epoch                = epoch,
            Key                  = key
        };

        GrpcGetPreparedIntentPresenceResponse response;

        try
        {
            global::KeyValuer.KeyValuerClient client = new(SharedChannels.GetChannel(ResolveNodeUrl(node), transportSecurity));

            response = await client
                .GetPreparedIntentPresenceAsync(request, headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure("GetPreparedIntentPresence", node, ex.StatusCode);

            return UnansweredResults.GetPreparedIntentPresence;
        }

        return ((KeyValueResponseType)response.Type, response.Held, response.AppliedLogId);
    }

    /// <summary>
    /// Asks the leader of a range move's source partition whether it enforces the move's quiesce; a plain unary
    /// call, sent once or a few times per move. A node of an older version does not know the call and answers
    /// Unimplemented, which is reported as <see cref="KeyValueResponseType.DoesNotExist"/> so the move can go on
    /// as it did before the call existed.
    /// </summary>
    public async Task<KeyValueResponseType> ConfirmRangeQuiesce(
        string node, int partitionId, string keySpace, string? startKey, string? endKey, HLCTimestamp owner, CancellationToken cancellationToken)
    {
        GrpcConfirmRangeQuiesceRequest request = new()
        {
            PartitionId   = partitionId,
            KeySpace      = keySpace,
            OwnerNode     = owner.N,
            OwnerPhysical = owner.L,
            OwnerCounter  = owner.C
        };

        if (startKey is not null)
            request.StartKey = startKey;

        if (endKey is not null)
            request.EndKey = endKey;

        try
        {
            global::KeyValuer.KeyValuerClient client = new(SharedChannels.GetChannel(ResolveNodeUrl(node), transportSecurity));

            GrpcConfirmRangeQuiesceResponse response = await client
                .ConfirmRangeQuiesceAsync(request, headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return (KeyValueResponseType)response.Type;
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Unimplemented)
        {
            return KeyValueResponseType.DoesNotExist;
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure("ConfirmRangeQuiesce", node, ex.StatusCode);

            return UnansweredResults.ConfirmRangeQuiesce;
        }
    }

    /// <summary>
    /// Asks the leader of a refused partition to settle the decided blockers of a prepare. A plain unary call: it
    /// is sent only when a prepare was refused, and its handling waits for a Raft round of its own, which must not
    /// hold a place in the ordered stream that carries the durable operations. A node of an older version answers
    /// Unimplemented, reported as <see cref="KeyValueResponseType.DoesNotExist"/>.
    /// </summary>
    public async Task<(KeyValueResponseType Type, int Settled, int Decided, int Undecided)> SettleDecidedBlockers(
        string node, int partitionId, IReadOnlyList<string> keys, HLCTimestamp transactionId, long epoch, CancellationToken cancellationToken)
    {
        GrpcSettleDecidedBlockersRequest request = new()
        {
            PartitionId           = partitionId,
            TransactionIdNode     = transactionId.N,
            TransactionIdPhysical = transactionId.L,
            TransactionIdCounter  = transactionId.C,
            Epoch                 = epoch,
            ForwardHops           = NextForwardHop
        };

        request.Keys.Capacity = keys.Count;
        for (int i = 0; i < keys.Count; i++)
            request.Keys.Add(keys[i]);

        try
        {
            global::KeyValuer.KeyValuerClient client = new(SharedChannels.GetChannel(ResolveNodeUrl(node), transportSecurity));

            GrpcSettleDecidedBlockersResponse response = await client
                .SettleDecidedBlockersAsync(request, headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            return ((KeyValueResponseType)response.Type, response.Settled, response.Decided, response.Undecided);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Unimplemented)
        {
            return (KeyValueResponseType.DoesNotExist, 0, 0, 0);
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure("SettleDecidedBlockers", node, ex.StatusCode);

            return UnansweredResults.SettleDecidedBlockers;
        }
    }
}

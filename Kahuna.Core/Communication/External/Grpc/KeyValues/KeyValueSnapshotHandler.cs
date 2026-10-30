
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
/// Snapshot holds, the snapshot floor, history holds and partition fingerprints.
/// </summary>
internal sealed class KeyValueSnapshotHandler
{
    private readonly IKahuna keyValues;

    public KeyValueSnapshotHandler(IKahuna keyValues)
    {
        this.keyValues = keyValues;
    }

    internal async Task<GrpcAcquireSnapshotHoldResponse> AcquireSnapshotHoldCore(
        GrpcAcquireSnapshotHoldRequest request, ServerCallContext context)
    {
        if (request.LeaseMs <= 0)
            return new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

        (KeyValueResponseType type, string holdId, HLCTimestamp leaseExpiry) =
            await keyValues.LocateAndAcquireSnapshotHold(
                request.HolderId,
                new(request.TimestampNode, request.TimestampPhysical, request.TimestampCounter),
                request.LeaseMs,
                context.CancellationToken).ConfigureAwait(false);

        return new()
        {
            Type               = (GrpcKeyValueResponseType)type,
            HoldId             = holdId,
            LeaseExpiryNode     = leaseExpiry.N,
            LeaseExpiryPhysical = leaseExpiry.L,
            LeaseExpiryCounter  = leaseExpiry.C
        };
    }

    internal async Task<GrpcRenewSnapshotHoldResponse> RenewSnapshotHoldCore(
        GrpcRenewSnapshotHoldRequest request, ServerCallContext context)
    {
        if (request.LeaseMs <= 0)
            return new() { Type = GrpcKeyValueResponseType.TypeInvalidInput };

        (KeyValueResponseType type, HLCTimestamp leaseExpiry) =
            await keyValues.LocateAndRenewSnapshotHold(
                request.HoldId,
                request.LeaseMs,
                context.CancellationToken).ConfigureAwait(false);

        return new()
        {
            Type               = (GrpcKeyValueResponseType)type,
            LeaseExpiryNode     = leaseExpiry.N,
            LeaseExpiryPhysical = leaseExpiry.L,
            LeaseExpiryCounter  = leaseExpiry.C
        };
    }

    internal async Task<GrpcReleaseSnapshotHoldResponse> ReleaseSnapshotHoldCore(
        GrpcReleaseSnapshotHoldRequest request, ServerCallContext context)
    {
        KeyValueResponseType type =
            await keyValues.LocateAndReleaseSnapshotHold(
                request.HoldId,
                context.CancellationToken).ConfigureAwait(false);

        return new() { Type = (GrpcKeyValueResponseType)type };
    }

    internal async Task<GrpcGetSnapshotFloorResponse> GetSnapshotFloorCore(
        GrpcGetSnapshotFloorRequest request, ServerCallContext context)
    {
        (KeyValueResponseType type, HLCTimestamp floor, int liveHolds) =
            await keyValues.GetSnapshotFloor(context.CancellationToken).ConfigureAwait(false);

        return new()
        {
            Type                   = (GrpcKeyValueResponseType)type,
            EffectiveFloorNode     = floor.N,
            EffectiveFloorPhysical = floor.L,
            EffectiveFloorCounter  = floor.C,
            LiveHolds              = liveHolds
        };
    }

    internal async Task<GrpcHistoryHoldResponse> HistoryHoldCore(GrpcHistoryHoldRequest request, ServerCallContext context) =>
        HistoryHoldWire.ToGrpc(await keyValues.HistoryHold(HistoryHoldWire.FromGrpc(request), context.CancellationToken).ConfigureAwait(false));

    internal async Task<GrpcGetPartitionApplyFingerprintResponse> GetPartitionApplyFingerprintCore(
        GrpcGetPartitionApplyFingerprintRequest request, ServerCallContext context)
    {
        (KeyValueResponseType type, KeyValueApplyFingerprint fingerprint) =
            await keyValues.GetPartitionApplyFingerprint(request.PartitionId, context.CancellationToken).ConfigureAwait(false);

        return new()
        {
            Type           = (GrpcKeyValueResponseType)type,
            AppliedLogId   = fingerprint.AppliedLogId,
            CommittedHeads = fingerprint.CommittedHeads,
            LiveIntents    = fingerprint.LiveIntents
        };
    }

    internal async Task<GrpcGetPreparedIntentPresenceResponse> GetPreparedIntentPresenceCore(
        GrpcGetPreparedIntentPresenceRequest request, ServerCallContext context)
    {
        (KeyValueResponseType type, bool held, long appliedLogId) = await keyValues.GetPreparedIntentPresence(
            request.PartitionId,
            new HLCTimestamp(request.TransactionIdNode, request.TransactionIdPhysical, request.TransactionIdCounter),
            request.Epoch,
            request.Key,
            context.CancellationToken).ConfigureAwait(false);

        return new()
        {
            Type         = (GrpcKeyValueResponseType)type,
            Held         = held,
            AppliedLogId = appliedLogId
        };
    }
}

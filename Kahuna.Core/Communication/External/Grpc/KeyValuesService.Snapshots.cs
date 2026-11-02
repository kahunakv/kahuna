
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

namespace Kahuna.Communication.External.Grpc;

public sealed partial class KeyValuesService
{
    public override Task<GrpcAcquireSnapshotHoldResponse> AcquireSnapshotHold(GrpcAcquireSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.AcquireSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.AcquireSnapshotHold());

    public override Task<GrpcRenewSnapshotHoldResponse> RenewSnapshotHold(GrpcRenewSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.RenewSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.RenewSnapshotHold());

    public override Task<GrpcReleaseSnapshotHoldResponse> ReleaseSnapshotHold(GrpcReleaseSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.ReleaseSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.ReleaseSnapshotHold());

    public override Task<GrpcGetSnapshotFloorResponse> GetSnapshotFloor(GrpcGetSnapshotFloorRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.GetSnapshotFloorCore(r, c), static _ => KeyValueMustRetry.GetSnapshotFloor());

    public override Task<GrpcHistoryHoldResponse> HistoryHold(GrpcHistoryHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.HistoryHoldCore(r, c), static _ => KeyValueMustRetry.HistoryHold());

    public override Task<GrpcGetPartitionApplyFingerprintResponse> GetPartitionApplyFingerprint(GrpcGetPartitionApplyFingerprintRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.GetPartitionApplyFingerprintCore(r, c), static _ => KeyValueMustRetry.GetPartitionApplyFingerprint());

    public override Task<GrpcGetPreparedIntentPresenceResponse> GetPreparedIntentPresence(GrpcGetPreparedIntentPresenceRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.GetPreparedIntentPresenceCore(r, c), static _ => KeyValueMustRetry.GetPreparedIntentPresence());

    internal Task<GrpcAcquireSnapshotHoldResponse> AcquireSnapshotHoldInternal(GrpcAcquireSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.AcquireSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.AcquireSnapshotHold());

    internal Task<GrpcRenewSnapshotHoldResponse> RenewSnapshotHoldInternal(GrpcRenewSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.RenewSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.RenewSnapshotHold());

    internal Task<GrpcReleaseSnapshotHoldResponse> ReleaseSnapshotHoldInternal(GrpcReleaseSnapshotHoldRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.ReleaseSnapshotHoldCore(r, c), static _ => KeyValueMustRetry.ReleaseSnapshotHold());

    internal Task<GrpcGetSnapshotFloorResponse> GetSnapshotFloorInternal(GrpcGetSnapshotFloorRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.snapshots.GetSnapshotFloorCore(r, c), static _ => KeyValueMustRetry.GetSnapshotFloor());
}

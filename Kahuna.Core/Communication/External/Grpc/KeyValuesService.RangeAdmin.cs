
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
    /// <summary>
    /// Registers a key space as key-range routed and seeds its whole-space descriptor. Mirrors REST
    /// <c>POST /v1/ranges/register</c> 1:1.
    /// <para>
    /// The routing-mode flip is node-local, so this must be called on <b>every</b> node; the seed is
    /// a replicated meta-partition write that the manager forwards to that partition's leader, so a
    /// follower answering here succeeds rather than refusing.
    /// </para>
    /// <para>
    /// This used to flip the routing mode alone and answer <c>Success = true</c> unconditionally,
    /// which left the space routed by key range with no descriptor to route to — every subsequent
    /// write to it threw "no range descriptor covers key". Seeding is what makes the call usable.
    /// </para>
    /// </summary>
    public override async Task<GrpcRegisterKeyRangeResponse> RegisterKeyRange(
        GrpcRegisterKeyRangeRequest request,
        ServerCallContext context)
    {
        KahunaRegisterKeyRangeResponse outcome = await keyValues
            .RegisterKeyRangeWithOutcomeAsync(request.KeySpace, context.CancellationToken)
            .ConfigureAwait(false);

        return new()
        {
            Success = outcome.Success,
            Status = outcome.Status,
            Seeded = outcome.Seeded,
            RoutingMode = outcome.RoutingMode,
            DescriptorCount = outcome.DescriptorCount,
            Reason = outcome.Reason ?? ""
        };
    }

    /// <summary>
    /// Drops a key space's descriptors from the replicated range map — the inverse of
    /// <see cref="RegisterKeyRange"/>, used for teardown. Mirrors REST
    /// <c>POST /v1/ranges/unregister</c> 1:1, including the retryable refusal while a split holds
    /// the quiesce window open.
    /// </summary>
    public override async Task<GrpcRemoveKeyRangeResponse> RemoveKeyRange(
        GrpcRemoveKeyRangeRequest request,
        ServerCallContext context)
    {
        KahunaRemoveKeyRangeResponse outcome = await keyValues
            .RemoveKeyRangeWithOutcomeAsync(request.KeySpace, context.CancellationToken)
            .ConfigureAwait(false);

        return new()
        {
            Success = outcome.Success,
            Status = outcome.Status,
            RoutingMode = outcome.RoutingMode,
            DescriptorCount = outcome.DescriptorCount,
            Reason = outcome.Reason ?? ""
        };
    }

    internal Task<GrpcEnsureKeyRangeSeededResponse> EnsureKeyRangeSeededInternal(GrpcEnsureKeyRangeSeededRequest request, ServerCallContext context)
        => rangeAdmin.EnsureKeyRangeSeededInternal(request, context);

    internal Task<GrpcEnsureKeyRangeRemovedResponse> EnsureKeyRangeRemovedInternal(GrpcEnsureKeyRangeRemovedRequest request, ServerCallContext context)
        => rangeAdmin.EnsureKeyRangeRemovedInternal(request, context);

    internal Task<GrpcGetRangeLocksResponse> GetRangeLocksInternal(GrpcGetRangeLocksRequest request, ServerCallContext context)
        => rangeAdmin.GetRangeLocksInternal(request, context);

    internal Task<GrpcImportRangeLocksResponse> ImportRangeLocksInternal(GrpcImportRangeLocksRequest request, ServerCallContext context)
        => rangeAdmin.ImportRangeLocksInternal(request, context);

    internal Task<GrpcImportCompletionReceiptsResponse> ImportCompletionReceiptsInternal(GrpcImportCompletionReceiptsRequest request, ServerCallContext context)
        => rangeAdmin.ImportCompletionReceiptsInternal(request, context);

    internal Task<GrpcReplicateKeyValueRangePageResponse> ReplicateKeyValueRangePageInternal(GrpcReplicateKeyValueRangePageRequest request, ServerCallContext context)
        => rangeAdmin.ReplicateKeyValueRangePageInternal(request, context);

    internal Task<GrpcGetRangeTransactionStateResponse> GetRangeTransactionStateInternal(GrpcGetRangeTransactionStateRequest request, ServerCallContext context)
        => rangeAdmin.GetRangeTransactionStateInternal(request, context);

    internal Task<GrpcGetStagedBaseVerdictsResponse> GetStagedBaseVerdictsInternal(GrpcGetStagedBaseVerdictsRequest request, ServerCallContext context)
        => rangeAdmin.GetStagedBaseVerdictsInternal(request, context);

    internal Task<GrpcDurableOperationResponse> DurableOperationInternal(GrpcDurableOperationRequest request, ServerCallContext context)
        => durable.DurableOperationInternal(request, context);

    internal Task<GrpcLookupTransactionRecordResponse> LookupTransactionRecordInternal(GrpcLookupTransactionRecordRequest request, ServerCallContext context)
        => durable.LookupTransactionRecordInternal(request, context);

    internal Task<GrpcDurableBundleResponse> DurableBundleInternal(GrpcDurableBundleRequest request, ServerCallContext context)
        => durable.DurableBundleInternal(request, context);

    internal Task<GrpcDurableDecisionResponse> DurableDecisionInternal(GrpcDurableDecisionRequest request, ServerCallContext context)
        => durable.DurableDecisionInternal(request, context);

    internal Task<GrpcDurableOnePhaseResponse> DurableOnePhaseInternal(GrpcDurableOnePhaseRequest request, ServerCallContext context)
        => durable.DurableOnePhaseInternal(request, context);
}

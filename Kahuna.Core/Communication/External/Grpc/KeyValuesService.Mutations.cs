
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
    /// Handles the request to prepare mutations for a set of key-value changes.
    /// </summary>
    /// <param name="request">The incoming request containing key-value mutation details.</param>
    /// <param name="context">The context for the server call providing runtime details and metadata.</param>
    /// <returns>A task that represents the asynchronous operation, containing the response for the mutation preparation request.</returns>
    public override async Task<GrpcTryPrepareMutationsResponse> TryPrepareMutations(GrpcTryPrepareMutationsRequest request, ServerCallContext context)
    {
        // The unary 2PC participant methods are node-only: no client calls them, and they stay on the
        // wire only because other-language clients ship this proto.
        gate.RequirePeer(context);

        return await TryPrepareMutationsInternal(request, context); 
    }

    /// <summary>
    /// Attempts to prepare multiple mutations in a single operation.
    /// </summary>
    /// <param name="request">The request object containing details of the mutations to prepare.</param>
    /// <param name="context">The server call context for the operation.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the response object with the outcome of the mutation preparation.</returns>
    public override async Task<GrpcTryPrepareManyMutationsResponse> TryPrepareManyMutations(GrpcTryPrepareManyMutationsRequest request, ServerCallContext context)
    {
        gate.RequirePeer(context);

        return await TryPrepareManyMutationsInternal(request, context);
    }

    /// <summary>
    /// Attempts to commit a series of mutations represented by the request.
    /// </summary>
    /// <param name="request">The request containing the mutations to be committed.</param>
    /// <param name="context">The context for the server call.</param>
    /// <returns>A response indicating the result of the commit attempt.</returns>
    public override async Task<GrpcTryCommitMutationsResponse> TryCommitMutations(GrpcTryCommitMutationsRequest request, ServerCallContext context)
    {
        gate.RequirePeer(context);

        return await TryCommitMutationsInternal(request, context);
    }

    /// <summary>
    /// Attempts to commit multiple mutations atomically.
    /// </summary>
    /// <param name="request">The request containing the mutations to be committed.</param>
    /// <param name="context">The server call context.</param>
    /// <returns>A task representing the asynchronous operation, with a response indicating the result of the commit.</returns>
    public override async Task<GrpcTryCommitManyMutationsResponse> TryCommitManyMutations(GrpcTryCommitManyMutationsRequest request, ServerCallContext context)
    {
        gate.RequirePeer(context);

        return await TryCommitManyMutationsInternal(request, context);
    }

    /// <summary>
    /// Receives requests for the key/value "RollbackMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryRollbackMutationsResponse> TryRollbackMutations(GrpcTryRollbackMutationsRequest request, ServerCallContext context)
    {
        gate.RequirePeer(context);

        return await TryRollbackMutationsInternal(request, context);
    }

    /// <summary>
    /// Receives requests for the key/value "RollbackManyMutations" service 
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryRollbackManyMutationsResponse> TryRollbackManyMutations(GrpcTryRollbackManyMutationsRequest request, ServerCallContext context)
    {
        gate.RequirePeer(context);

        return await TryRollbackManyMutationsInternal(request, context);
    }

    internal Task<GrpcTryPrepareMutationsResponse> TryPrepareMutationsInternal(GrpcTryPrepareMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryPrepareMutationsCore(r, c), static _ => KeyValueMustRetry.TryPrepareMutations());

    internal Task<GrpcTryPrepareManyMutationsResponse> TryPrepareManyMutationsInternal(GrpcTryPrepareManyMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryPrepareManyMutationsCore(r, c), static r => KeyValueMustRetry.TryPrepareManyMutations(r));

    internal Task<GrpcTryCommitMutationsResponse> TryCommitMutationsInternal(GrpcTryCommitMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryCommitMutationsCore(r, c), static _ => KeyValueMustRetry.TryCommitMutations());

    internal Task<GrpcTryCommitManyMutationsResponse> TryCommitManyMutationsInternal(GrpcTryCommitManyMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryCommitManyMutationsCore(r, c), static r => KeyValueMustRetry.TryCommitManyMutations(r));

    internal Task<GrpcTryRollbackMutationsResponse> TryRollbackMutationsInternal(GrpcTryRollbackMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryRollbackMutationsCore(r, c), static _ => KeyValueMustRetry.TryRollbackMutations());

    internal Task<GrpcTryRollbackManyMutationsResponse> TryRollbackManyMutationsInternal(GrpcTryRollbackManyMutationsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.mutations.TryRollbackManyMutationsCore(r, c), static r => KeyValueMustRetry.TryRollbackManyMutations(r));
}

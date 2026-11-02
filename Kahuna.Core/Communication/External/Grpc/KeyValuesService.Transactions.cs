
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
    /// Executes a transaction script on the key/value store
    /// </summary>
    /// <param name="request"></param>
    /// <param name="context"></param>
    /// <returns></returns>
    public override async Task<GrpcTryExecuteTransactionScriptResponse> TryExecuteTransactionScript(GrpcTryExecuteTransactionScriptRequest request, ServerCallContext context)
    {
        return await TryExecuteTransactionScriptInternal(request, context);
    }

    /// <summary>
    /// Initiates an interactive transaction
    /// </summary>
    /// <param name="request">The request object containing transaction details.</param>
    /// <param name="context">The server call context for the request.</param>
    /// <returns>A task that represents the asynchronous operation, containing the transaction response.</returns>
    public override async Task<GrpcStartTransactionResponse> StartTransaction(GrpcStartTransactionRequest request, ServerCallContext context)
    {
        StampClientSession(request);
        return await StartTransactionInternal(request, context);
    }

    /// <summary>
    /// Sets the mutation limit of a session a client starts from this node's configuration, overwriting whatever the
    /// client sent: the field exists so a forwarded start carries the entry node's limit, and a client must not choose
    /// its own. Not called on the inter-node path, which carries the limit the entry node set.
    /// </summary>
    internal void StampClientSession(GrpcStartTransactionRequest request) =>
        request.MutationLimit = inputLimits.MaxTransactionMutations;

    /// <summary>
    /// Commits a transaction in the key-value store.
    /// </summary>
    /// <param name="request">The request containing transaction details to be committed.</param>
    /// <param name="context">The server call context for the current request.</param>
    /// <returns>A response indicating the result of the commit operation.</returns>
    public override async Task<GrpcCommitTransactionResponse> CommitTransaction(GrpcCommitTransactionRequest request, ServerCallContext context)
    {
        return await CommitTransactionInternal(request, context);
    }

    /// <summary>
    /// Rolls back a transaction in the key-value store operation.
    /// </summary>
    /// <param name="request">The rollback transaction request containing the operation details.</param>
    /// <param name="context">The server call context providing contextual information about the request.</param>
    /// <returns>A response indicating the outcome of the rollback transaction operation.</returns>
    public override async Task<GrpcRollbackTransactionResponse> RollbackTransaction(GrpcRollbackTransactionRequest request, ServerCallContext context)
    {
        return await RollbackTransactionInternal(request, context);
    }

    internal Task<GrpcBeginOperationResponse> BeginOperationInternal(GrpcBeginOperationRequest request, ServerCallContext context)
        => transactions.BeginOperationInternal(request, context);

    internal Task<GrpcCompleteOperationResponse> CompleteOperationInternal(GrpcCompleteOperationRequest request, ServerCallContext context)
        => transactions.CompleteOperationInternal(request, context);

    internal static OperationCompletionPayload FromGrpcCompleteOperationRequest(GrpcCompleteOperationRequest request)
        => KeyValueTransactionHandler.FromGrpcCompleteOperationRequest(request);

    internal Task<GrpcGetTransactionWorkingSetResponse> GetTransactionWorkingSetInternal(GrpcGetTransactionWorkingSetRequest request, ServerCallContext context)
        => transactions.GetTransactionWorkingSetInternal(request, context);

    internal static StagedMutationEffect FromGrpcStagedMutation(GrpcStagedMutationEffect g)
        => KeyValueTransactionHandler.FromGrpcStagedMutation(g);

    internal Task<GrpcTryExecuteTransactionScriptResponse> TryExecuteTransactionScriptInternal(GrpcTryExecuteTransactionScriptRequest request, ServerCallContext context)
        // Every path here is a client's: no node forwards a script to another, so each run takes the client limit.
        => Guard(request, context, static (s, r, c) => s.transactions.TryExecuteTransactionScriptCore(r, c, s.inputLimits.MaxTransactionMutations), static _ => KeyValueMustRetry.TryExecuteTransactionScript());

    internal Task<GrpcStartTransactionResponse> StartTransactionInternal(GrpcStartTransactionRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.transactions.StartTransactionCore(r, c), static _ => KeyValueMustRetry.StartTransaction());

    internal Task<GrpcCommitTransactionResponse> CommitTransactionInternal(GrpcCommitTransactionRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.transactions.CommitTransactionCore(r, c), static _ => KeyValueMustRetry.CommitTransaction());

    internal Task<GrpcRollbackTransactionResponse> RollbackTransactionInternal(GrpcRollbackTransactionRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.transactions.RollbackTransactionCore(r, c), static _ => KeyValueMustRetry.RollbackTransaction());

    internal Task<GrpcCloseTransactionResponse> CloseTransactionInternal(GrpcCloseTransactionRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.transactions.CloseTransactionCore(r, c), static _ => KeyValueMustRetry.CloseTransaction());
}

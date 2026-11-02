
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
    private static GrpcTryGetManyValuesResponse InvalidGetMany(GrpcTryGetManyValuesRequest request)
    {
        GrpcTryGetManyValuesResponse response = new();
        ReserveItemCapacity(response.Items, request.Items.Count);
        foreach (GrpcTryManyValuesRequestItem item in request.Items)
            response.Items.Add(new GrpcTryGetManyValuesResponseItem
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput,
                Key = item.Key,
                Durability = item.Durability
            });
        return response;
    }

    private static GrpcTryExistsManyValuesResponse InvalidExistsMany(GrpcTryExistsManyValuesRequest request)
    {
        GrpcTryExistsManyValuesResponse response = new();
        ReserveItemCapacity(response.Items, request.Items.Count);
        foreach (GrpcTryManyValuesRequestItem item in request.Items)
            response.Items.Add(new GrpcTryExistsManyValuesResponseItem
            {
                Type = GrpcKeyValueResponseType.TypeInvalidInput,
                Key = item.Key,
                Durability = item.Durability
            });
        return response;
    }

    /// <summary>
    /// Retrieves the value associated with the specified key if it exists.
    /// </summary>
    /// <param name="request">The request containing the key to get the associated value for.</param>
    /// <param name="context">The server call context for managing metadata, timing, and cancellation tokens.</param>
    /// <returns>A <see cref="GrpcTryGetKeyValueResponse"/> containing the retrieved value if the key exists, or an appropriate response indicating failure.</returns>
    public override async Task<GrpcTryGetKeyValueResponse> TryGetKeyValue(GrpcTryGetKeyValueRequest request, ServerCallContext context)
    {
        return await TryGetKeyValueInternal(request, context);
    }

    public override Task<GrpcTryGetManyValuesResponse> TryGetManyValues(GrpcTryGetManyValuesRequest request, ServerCallContext context)
    {
        if (!inputLimits.BatchFits(request.Items.Count))
            return Task.FromResult(InvalidGetMany(request));

        return Guard(request, context, static (s, r, c) => s.reads.TryGetManyValuesRoutedCore(r, c), static r => KeyValueMustRetry.TryGetManyValues(r));
    }

    /// <summary>
    /// Handles the gRPC request to check if a specific key exists in the key-value store.
    /// </summary>
    /// <param name="request">The request object containing the key information.</param>
    /// <param name="context">The server call context for the operation.</param>
    /// <returns>A task that represents the asynchronous operation, returning a response indicating whether the key exists.</returns>
    public override async Task<GrpcTryExistsKeyValueResponse> TryExistsKeyValue(GrpcTryExistsKeyValueRequest request, ServerCallContext context)
    {
        return await TryExistsKeyValueInternal(request, context);
    }

    public override Task<GrpcTryExistsManyValuesResponse> TryExistsManyValues(GrpcTryExistsManyValuesRequest request, ServerCallContext context)
    {
        if (!inputLimits.BatchFits(request.Items.Count))
            return Task.FromResult(InvalidExistsMany(request));

        return Guard(request, context, static (s, r, c) => s.reads.TryExistsManyValuesRoutedCore(r, c), static r => KeyValueMustRetry.TryExistsManyValues(r));
    }

    internal Task<GrpcTryGetKeyValueResponse> TryGetKeyValueInternal(GrpcTryGetKeyValueRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryGetKeyValueCore(r, c), static _ => KeyValueMustRetry.TryGetKeyValue());

    internal Task<GrpcTryGetManyValuesResponse> TryGetManyValuesInternal(GrpcTryGetManyValuesRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryGetManyValuesCore(r, c), static r => KeyValueMustRetry.TryGetManyValues(r));

    internal Task<GrpcTryProbeManyResponse> TryProbeManyInternal(GrpcTryProbeManyRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryProbeManyCore(r, c), static r => ProbeManyWire.MustRetry(r));

    internal Task<GrpcTryProbeManyRangesResponse> TryProbeManyRangesInternal(GrpcTryProbeManyRangesRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryProbeManyRangesCore(r, c), static r => ProbeManyRangesWire.MustRetry(r));

    internal Task<GrpcTryExistsKeyValueResponse> TryExistsKeyValueInternal(GrpcTryExistsKeyValueRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryExistsKeyValueCore(r, c), static _ => KeyValueMustRetry.TryExistsKeyValue());

    internal Task<GrpcTryExistsManyValuesResponse> TryExistsManyValuesInternal(GrpcTryExistsManyValuesRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryExistsManyValuesCore(r, c), static r => KeyValueMustRetry.TryExistsManyValues(r));

    internal Task<GrpcTryCheckWriteIntentResponse> TryCheckWriteIntentInternal(GrpcTryCheckWriteIntentRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryCheckWriteIntentCore(r, c), static _ => KeyValueMustRetry.TryCheckWriteIntent());

    internal Task<GrpcTryCheckManyWriteIntentsResponse> TryCheckManyWriteIntentsInternal(GrpcTryCheckManyWriteIntentsRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.reads.TryCheckManyWriteIntentsCore(r, c), static r => KeyValueMustRetry.TryCheckManyWriteIntents(r));
}

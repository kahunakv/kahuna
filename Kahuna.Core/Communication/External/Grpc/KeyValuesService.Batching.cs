
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
    /// Processes a batch of client key-value requests using a gRPC bidirectional stream
    /// </summary>
    /// <param name="requestStream">The stream of incoming key-value requests from the client.</param>
    /// <param name="responseStream">The stream for sending key-value responses back to the client.</param>
    /// <param name="context">The context of the server call.</param>
    /// <returns>A task that represents the asynchronous processing of the batch of requests.</returns>
    public override async Task BatchClientKeyValueRequests(
        IAsyncStreamReader<GrpcBatchClientKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchClientKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        await clientBatcher.BatchClientKeyValueRequests(requestStream, responseStream, context);
    }

    /// <summary>
    /// Processes a batch of server key-value requests using a gRPC bidirectional stream 
    /// </summary>
    /// <param name="requestStream"></param>
    /// <param name="responseStream"></param>
    /// <param name="context"></param>
    public override async Task BatchServerKeyValueRequests(
        IAsyncStreamReader<GrpcBatchServerKeyValueRequest> requestStream,
        IServerStreamWriter<GrpcBatchServerKeyValueResponse> responseStream,
        ServerCallContext context
    )
    {
        // Only peers open this stream: it carries forwarded key-value operations.
        gate.RequirePeer(context);

        await serverBatcher.BatchServerKeyValueRequests(requestStream, responseStream, context);
    }

    /// <summary>
    /// Bidirectional watch streams: one call carries any number of watches (see <see cref="KeyValueWatchGrpcSession"/>).
    /// A refused watch is answered with a typed refusal frame for its watch id. A retryable failure that ends the
    /// whole call is mapped to <see cref="StatusCode.Unavailable"/>, which a client retries, rather than to an
    /// unclassifiable <see cref="StatusCode.Unknown"/>.
    /// </summary>
    public override async Task Watch(
        IAsyncStreamReader<GrpcWatchRequest> requestStream,
        IServerStreamWriter<GrpcWatchResponse> responseStream,
        ServerCallContext context)
    {
        try
        {
            await new KeyValueWatchGrpcSession(keyValues, logger, maxWatchesPerCall).RunAsync(requestStream, responseStream, context.CancellationToken);
        }
        catch (Exception ex) when (RetryableFailureClassifier.IsRetryable(ex) && !context.CancellationToken.IsCancellationRequested)
        {
            logger.LogWarning("Mapping retryable {ExceptionType} on Watch to Unavailable: {Message}", ex.GetType().Name, ex.Message);
            throw new RpcException(new Status(StatusCode.Unavailable, ex.Message));
        }
    }
}

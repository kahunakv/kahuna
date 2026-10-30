
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

namespace Kahuna.Communication.External.Grpc.KeyValues;

/// <summary>
/// Wire-message helpers shared by the key-value gRPC handlers.
/// </summary>
internal static class KeyValueGrpcWire
{
    /// <summary>
    /// Reserves room for a batch of known size before its items are added one by one. Without the
    /// reservation the repeated field grows its backing array several times per batch.
    /// </summary>
    internal static void ReserveItemCapacity<T>(RepeatedField<T> target, int count)
    {
        if (count == 0)
            return;

        // The field is empty at every current call site, but a later caller may append to a
        // populated one, so the existing items are part of the requirement.
        int required = checked(target.Count + count);

        if (required > target.Capacity)
            target.Capacity = required;
    }

    /// <summary>
    /// Copies the leaderships the locks of the served request were granted under into its response, so the
    /// node that forwarded the request can hand them to the transaction's coordinator. Empty when the request
    /// was registered with a coordinator here, which already received them with the operation's completion.
    /// </summary>
    internal static void AddLockGrantTerms(RepeatedField<GrpcLockGrantTerm> target, LockGrantCapture grants)
    {
        if (grants.Take() is not { } taken)
            return;

        foreach (LockGrantTerm grant in taken)
            target.Add(new GrpcLockGrantTerm { PartitionId = grant.PartitionId, Term = grant.Term, RoutingKey = grant.RoutingKey });
    }

    /// <summary>
    /// Converts a list of key-value result items into a collection of gRPC key-value prefix item responses.
    /// </summary>
    /// <param name="resultItems">A list containing key-value pairs and their associated context.</param>
    /// <returns>A collection of gRPC key-value prefix item responses.</returns>
    internal static void AddKeyValueItems(RepeatedField<GrpcKeyValueByPrefixItemResponse> target, List<(string, ReadOnlyKeyValueEntry)> resultItems)
    {
        ReserveItemCapacity(target, resultItems.Count);

        foreach ((string key, ReadOnlyKeyValueEntry context) in resultItems)
        {
            GrpcKeyValueByPrefixItemResponse response = new()
            {
                Key = key,
                Revision = context.Revision,
                ExpiresNode = context.Expires.N,
                ExpiresPhysical = context.Expires.L,
                ExpiresCounter = context.Expires.C,
                LastUsedNode = context.LastUsed.N,
                LastUsedPhysical = context.LastUsed.L,
                LastUsedCounter = context.LastUsed.C,
                LastModifiedNode = context.LastModified.N,
                LastModifiedPhysical = context.LastModified.L,
                LastModifiedCounter = context.LastModified.C,
                State = (GrpcKeyValueState)context.State,
            };
            
            if (context.Value is not null)
                response.Value = UnsafeByteOperations.UnsafeWrap(context.Value);

            target.Add(response);
        }
    }

    /// <summary>
    /// Converts a collection of gRPC key-value parameters into a list of KeyValueParameter objects.
    /// </summary>
    /// <param name="requestParameters">The collection of gRPC key-value parameters to be converted.</param>
    /// <returns>A list of KeyValueParameter objects derived from the given gRPC key-value parameters.</returns>
    internal static List<KeyValueParameter> GetParameters(RepeatedField<GrpcKeyValueParameter> requestParameters)
    {
        List<KeyValueParameter> parameters = new(requestParameters.Count);
        
        foreach (GrpcKeyValueParameter? parameter in requestParameters)
            parameters.Add(new() { Key = parameter.Key, Value = parameter.Value });
        
        return parameters;
    }
}

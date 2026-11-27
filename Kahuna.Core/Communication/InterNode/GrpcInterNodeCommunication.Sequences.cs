
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

/// <summary>Sequence forwarding.</summary>
public partial class GrpcInterNodeCommunication
{
    // ── sequences ───────────────────────────────────────────────────────────────────────────────
    // Forwarded as plain unary calls on the shared channel rather than through the batcher. Sequence
    // traffic between nodes is sparse by construction — a node reserves a block of values with one
    // write and then serves the rest from memory — so there is nothing for a batching lane to coalesce.
    //
    // Each request carries the hop count of the chain it continues, read here on the forwarding
    // call's own async flow where the ambient marker is visible, so the receiver's forward budget
    // spans the whole chain instead of restarting at the process boundary.

    /// <summary>The hop count a request written now belongs at: the chain so far, plus this hop.</summary>
    private static int NextForwardHop => ForwardedRequestScope.ChainedHops + 1;

    /// <summary>Forwards a sequence create to the node that owns the sequence's partition.</summary>
    public async Task<(SequenceResponseType, long)> CreateSequence(
        string node,
        string name,
        long initialValue,
        long increment,
        long? maxValue,
        int? blockSize,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcCreateSequenceRequest request = new()
        {
            Name = name,
            InitialValue = initialValue,
            Increment = increment,
            Durability = (GrpcSequenceDurability)durability,
            ForwardHops = NextForwardHop
        };

        if (maxValue.HasValue)
            request.MaxValue = maxValue.Value;

        if (blockSize.HasValue)
            request.BlockSize = blockSize.Value;

        GrpcSequenceResponse response;

        try
        {
            response = await GetSequencerClient(node)
                .CreateSequenceAsync(request, headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure("CreateSequence", node, ex.StatusCode);

            return UnansweredResults.CreateSequence;
        }

        return ((SequenceResponseType)response.Type, response.Revision);
    }

    /// <summary>
    /// Forwards a sequence update to the node that owns the sequence's partition. The owner holds its
    /// answer back for one block lease, so this call is expected to take that long and carries no
    /// deadline of its own beyond the caller's token.
    /// </summary>
    public async Task<(SequenceResponseType, long)> UpdateSequence(
        string node,
        string name,
        SequenceUpdate update,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcUpdateSequenceRequest request = new()
        {
            Name = name,
            RemoveMaxValue = update.RemoveMaxValue,
            RemoveBlockSize = update.RemoveBlockSize,
            Durability = (GrpcSequenceDurability)durability,
            ForwardHops = NextForwardHop
        };

        if (update.CurrentValue.HasValue)
            request.CurrentValue = update.CurrentValue.Value;

        if (update.Increment.HasValue)
            request.Increment = update.Increment.Value;

        if (update.InitialValue.HasValue)
            request.InitialValue = update.InitialValue.Value;

        if (update.MaxValue.HasValue)
            request.MaxValue = update.MaxValue.Value;

        if (update.BlockSize.HasValue)
            request.BlockSize = update.BlockSize.Value;

        GrpcSequenceResponse response;

        try
        {
            response = await GetSequencerClient(node)
                .UpdateSequenceAsync(request, headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure("UpdateSequence", node, ex.StatusCode);

            return UnansweredResults.UpdateSequence;
        }

        return ((SequenceResponseType)response.Type, response.Revision);
    }

    /// <summary>Forwards a sequence read to the node that owns the sequence's partition.</summary>
    public async Task<(SequenceResponseType, ReadOnlySequenceEntry?)> GetSequence(
        string node,
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcGetSequenceRequest request = new()
        {
            Name = name,
            Durability = (GrpcSequenceDurability)durability,
            ForwardHops = NextForwardHop
        };

        GrpcSequenceResponse response;

        try
        {
            response = await GetSequencerClient(node)
                .GetSequenceAsync(request, headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure("GetSequence", node, ex.StatusCode);

            return UnansweredResults.GetSequence;
        }

        SequenceResponseType type = (SequenceResponseType)response.Type;

        if (type != SequenceResponseType.Success || response.Sequence is null)
            return (type, null);

        GrpcSequenceEntry entry = response.Sequence;

        return (type, new ReadOnlySequenceEntry(
            entry.Name,
            entry.CurrentValue,
            entry.InitialValue,
            entry.Increment,
            entry.HasMaxValue ? entry.MaxValue : null,
            entry.Revision,
            (SequenceDurability)entry.Durability,
            new HLCTimestamp(entry.CreatedAtNode, entry.CreatedAtPhysical, entry.CreatedAtCounter),
            new HLCTimestamp(entry.UpdatedAtNode, entry.UpdatedAtPhysical, entry.UpdatedAtCounter),
            entry.HasBlockSize ? entry.BlockSize : null,
            entry.Incarnation
        ));
    }

    /// <summary>Forwards a single-value allocation to the node that owns the sequence's partition.</summary>
    public async Task<(SequenceResponseType, SequenceAllocation)> NextSequenceValue(
        string node,
        string name,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcNextSequenceRequest request = new()
        {
            Name = name,
            Durability = (GrpcSequenceDurability)durability,
            ForwardHops = NextForwardHop
        };

        if (idempotencyKey is not null)
            request.IdempotencyKey = idempotencyKey;

        GrpcSequenceAllocationResponse response;

        try
        {
            response = await GetSequencerClient(node)
                .NextSequenceValueAsync(request, headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure("NextSequenceValue", node, ex.StatusCode);

            return UnansweredResults.NextSequenceValue;
        }

        return ((SequenceResponseType)response.Type, ToAllocation(response.Allocation));
    }

    /// <summary>Forwards a multi-value allocation to the node that owns the sequence's partition.</summary>
    public async Task<(SequenceResponseType, SequenceAllocation)> ReserveSequenceRange(
        string node,
        string name,
        int count,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcReserveSequenceRangeRequest request = new()
        {
            Name = name,
            Count = count,
            Durability = (GrpcSequenceDurability)durability,
            ForwardHops = NextForwardHop
        };

        if (idempotencyKey is not null)
            request.IdempotencyKey = idempotencyKey;

        GrpcSequenceAllocationResponse response;

        try
        {
            response = await GetSequencerClient(node)
                .ReserveSequenceRangeAsync(request, headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure("ReserveSequenceRange", node, ex.StatusCode);

            return UnansweredResults.ReserveSequenceRange;
        }

        return ((SequenceResponseType)response.Type, ToAllocation(response.Allocation));
    }

    /// <summary>Forwards a sequence delete to the node that owns the sequence's partition.</summary>
    public async Task<SequenceResponseType> DeleteSequence(
        string node,
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        GrpcDeleteSequenceRequest request = new()
        {
            Name = name,
            Durability = (GrpcSequenceDurability)durability,
            ForwardHops = NextForwardHop
        };

        GrpcSequenceResponse response;

        try
        {
            response = await GetSequencerClient(node)
                .DeleteSequenceAsync(request, headers: InterNodeHeaders.ForwardedCall, cancellationToken: cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex) when (IsRetryableTransportFailure(ex))
        {
            LogForwardingTransportFailure("DeleteSequence", node, ex.StatusCode);

            return UnansweredResults.DeleteSequence;
        }

        return (SequenceResponseType)response.Type;
    }

    private static SequenceAllocation ToAllocation(GrpcSequenceAllocation? allocation)
    {
        return allocation is null
            ? default
            : new SequenceAllocation(allocation.Name, allocation.Start, allocation.End, allocation.Count, allocation.Revision);
    }
}

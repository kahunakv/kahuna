/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;
using Kahuna.Server.Communication;
using Kahuna.Shared.Sequences;
using Kommander.Diagnostics;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Provides gRPC services for distributed sequence management.
///
/// <para>Serves two kinds of callers. A client request resolves the sequence's owner and follows it
/// (the <c>LocateAnd*</c> path). A request another node already routed here carries the forwarded
/// marker and is served directly against this node, whose entry points re-check leadership once and
/// answer <c>MustRetry</c> when stale — it is never forwarded again, so nodes with disagreeing
/// leadership views cannot bounce one request between each other.</para>
/// </summary>
public sealed class SequencesService : Sequencer.SequencerBase
{
    private readonly IKahuna sequences;

    public SequencesService(IKahuna sequences)
    {
        this.sequences = sequences;
    }

    public override async Task<GrpcSequenceResponse> CreateSequence(GrpcCreateSequenceRequest request, ServerCallContext context)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        if (string.IsNullOrWhiteSpace(request.Name))
            return new() { Type = GrpcSequenceResponseType.SequenceInvalidInput, TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds() };

        (SequenceResponseType response, long revision) = await (InterNodeHeaders.IsForwarded(context)
            ? sequences.CreateSequence(
                request.Name,
                request.InitialValue,
                request.Increment,
                request.HasMaxValue ? request.MaxValue : null,
                (SequenceDurability)request.Durability,
                context.CancellationToken)
            : sequences.LocateAndCreateSequence(
                request.Name,
                request.InitialValue,
                request.Increment,
                request.HasMaxValue ? request.MaxValue : null,
                (SequenceDurability)request.Durability,
                context.CancellationToken));

        return new()
        {
            Type = (GrpcSequenceResponseType)response,
            Revision = revision,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }

    public override async Task<GrpcSequenceResponse> GetSequence(GrpcGetSequenceRequest request, ServerCallContext context)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        if (string.IsNullOrWhiteSpace(request.Name))
            return new() { Type = GrpcSequenceResponseType.SequenceInvalidInput, TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds() };

        (SequenceResponseType response, ReadOnlySequenceEntry? sequence) = await (InterNodeHeaders.IsForwarded(context)
            ? sequences.GetSequence(request.Name, (SequenceDurability)request.Durability, context.CancellationToken)
            : sequences.LocateAndGetSequence(request.Name, (SequenceDurability)request.Durability, context.CancellationToken));

        GrpcSequenceResponse grpcResponse = new()
        {
            Type = (GrpcSequenceResponseType)response,
            Revision = sequence?.Revision ?? -1,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };

        if (sequence is not null)
            grpcResponse.Sequence = ToGrpcSequenceEntry(sequence);

        return grpcResponse;
    }

    public override async Task<GrpcSequenceAllocationResponse> NextSequenceValue(GrpcNextSequenceRequest request, ServerCallContext context)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        if (string.IsNullOrWhiteSpace(request.Name))
            return new() { Type = GrpcSequenceResponseType.SequenceInvalidInput, TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds() };

        (SequenceResponseType response, SequenceAllocation allocation) = await (InterNodeHeaders.IsForwarded(context)
            ? sequences.NextSequenceValue(
                request.Name,
                request.HasIdempotencyKey ? request.IdempotencyKey : null,
                (SequenceDurability)request.Durability,
                context.CancellationToken)
            : sequences.LocateAndNextSequenceValue(
                request.Name,
                request.HasIdempotencyKey ? request.IdempotencyKey : null,
                (SequenceDurability)request.Durability,
                context.CancellationToken));

        return BuildAllocationResponse(response, allocation, stopwatch);
    }

    public override async Task<GrpcSequenceAllocationResponse> ReserveSequenceRange(GrpcReserveSequenceRangeRequest request, ServerCallContext context)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        if (string.IsNullOrWhiteSpace(request.Name) || request.Count <= 0)
            return new() { Type = GrpcSequenceResponseType.SequenceInvalidInput, TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds() };

        (SequenceResponseType response, SequenceAllocation allocation) = await (InterNodeHeaders.IsForwarded(context)
            ? sequences.ReserveSequenceRange(
                request.Name,
                request.Count,
                request.HasIdempotencyKey ? request.IdempotencyKey : null,
                (SequenceDurability)request.Durability,
                context.CancellationToken)
            : sequences.LocateAndReserveSequenceRange(
                request.Name,
                request.Count,
                request.HasIdempotencyKey ? request.IdempotencyKey : null,
                (SequenceDurability)request.Durability,
                context.CancellationToken));

        return BuildAllocationResponse(response, allocation, stopwatch);
    }

    public override async Task<GrpcSequenceResponse> DeleteSequence(GrpcDeleteSequenceRequest request, ServerCallContext context)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        if (string.IsNullOrWhiteSpace(request.Name))
            return new() { Type = GrpcSequenceResponseType.SequenceInvalidInput, TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds() };

        SequenceResponseType response = await (InterNodeHeaders.IsForwarded(context)
            ? sequences.DeleteSequence(request.Name, (SequenceDurability)request.Durability, context.CancellationToken)
            : sequences.LocateAndDeleteSequence(request.Name, (SequenceDurability)request.Durability, context.CancellationToken));

        return new()
        {
            Type = (GrpcSequenceResponseType)response,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }

    private static GrpcSequenceEntry ToGrpcSequenceEntry(ReadOnlySequenceEntry sequence)
    {
        GrpcSequenceEntry entry = new()
        {
            Name = sequence.Name,
            CurrentValue = sequence.CurrentValue,
            InitialValue = sequence.InitialValue,
            Increment = sequence.Increment,
            Revision = sequence.Revision,
            Durability = (GrpcSequenceDurability)sequence.Durability,
            CreatedAtNode = sequence.CreatedAt.N,
            CreatedAtPhysical = sequence.CreatedAt.L,
            CreatedAtCounter = sequence.CreatedAt.C,
            UpdatedAtNode = sequence.UpdatedAt.N,
            UpdatedAtPhysical = sequence.UpdatedAt.L,
            UpdatedAtCounter = sequence.UpdatedAt.C
        };

        if (sequence.MaxValue.HasValue)
            entry.MaxValue = sequence.MaxValue.Value;

        return entry;
    }

    /// <summary>
    /// Every failure reply carries a <c>default</c> allocation, whose <c>Name</c> is null. Protobuf string
    /// fields reject null, so an allocation must only be attached when one was actually produced — otherwise
    /// a plain NotFound/MustRetry answer would throw here and reach the caller as an opaque transport error
    /// instead of its response type.
    /// </summary>
    internal static GrpcSequenceAllocationResponse BuildAllocationResponse(SequenceResponseType response, SequenceAllocation allocation, ValueStopwatch stopwatch)
    {
        GrpcSequenceAllocationResponse grpcResponse = new()
        {
            Type = (GrpcSequenceResponseType)response,
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };

        if (allocation.Name is not null)
            grpcResponse.Allocation = ToGrpcSequenceAllocation(allocation);

        return grpcResponse;
    }

    private static GrpcSequenceAllocation ToGrpcSequenceAllocation(SequenceAllocation allocation)
    {
        return new()
        {
            // An unnamed allocation cannot be expressed on the wire; empty is protobuf's own default.
            Name = allocation.Name ?? "",
            Start = allocation.Start,
            End = allocation.End,
            Count = allocation.Count,
            Revision = allocation.Revision
        };
    }
}

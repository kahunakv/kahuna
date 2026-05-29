/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Grpc.Core;
using Kahuna.Shared.Sequences;
using Kommander.Diagnostics;

namespace Kahuna.Communication.External.Grpc;

/// <summary>
/// Provides gRPC services for distributed sequence management.
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

        (SequenceResponseType response, long revision) = await sequences.LocateAndCreateSequence(
            request.Name,
            request.InitialValue,
            request.Increment,
            request.HasMaxValue ? request.MaxValue : null,
            (SequenceDurability)request.Durability,
            context.CancellationToken
        );

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

        (SequenceResponseType response, ReadOnlySequenceEntry? sequence) = await sequences.LocateAndGetSequence(
            request.Name,
            (SequenceDurability)request.Durability,
            context.CancellationToken
        );

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

        (SequenceResponseType response, SequenceAllocation allocation) = await sequences.LocateAndNextSequenceValue(
            request.Name,
            request.HasIdempotencyKey ? request.IdempotencyKey : null,
            (SequenceDurability)request.Durability,
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcSequenceResponseType)response,
            Allocation = ToGrpcSequenceAllocation(allocation),
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }

    public override async Task<GrpcSequenceAllocationResponse> ReserveSequenceRange(GrpcReserveSequenceRangeRequest request, ServerCallContext context)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        if (string.IsNullOrWhiteSpace(request.Name) || request.Count <= 0)
            return new() { Type = GrpcSequenceResponseType.SequenceInvalidInput, TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds() };

        (SequenceResponseType response, SequenceAllocation allocation) = await sequences.LocateAndReserveSequenceRange(
            request.Name,
            request.Count,
            request.HasIdempotencyKey ? request.IdempotencyKey : null,
            (SequenceDurability)request.Durability,
            context.CancellationToken
        );

        return new()
        {
            Type = (GrpcSequenceResponseType)response,
            Allocation = ToGrpcSequenceAllocation(allocation),
            TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds()
        };
    }

    public override async Task<GrpcSequenceResponse> DeleteSequence(GrpcDeleteSequenceRequest request, ServerCallContext context)
    {
        ValueStopwatch stopwatch = ValueStopwatch.StartNew();

        if (string.IsNullOrWhiteSpace(request.Name))
            return new() { Type = GrpcSequenceResponseType.SequenceInvalidInput, TimeElapsedMs = (int)stopwatch.GetElapsedMilliseconds() };

        SequenceResponseType response = await sequences.LocateAndDeleteSequence(
            request.Name,
            (SequenceDurability)request.Durability,
            context.CancellationToken
        );

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

    private static GrpcSequenceAllocation ToGrpcSequenceAllocation(SequenceAllocation allocation)
    {
        return new()
        {
            Name = allocation.Name,
            Start = allocation.Start,
            End = allocation.End,
            Count = allocation.Count,
            Revision = allocation.Revision
        };
    }
}

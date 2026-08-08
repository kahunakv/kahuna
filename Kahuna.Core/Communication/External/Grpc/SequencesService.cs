/**
 * This file is part of Kahuna
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

using Grpc.Core;
using Kahuna.Server.Communication;
using Kahuna.Server.Communication.Internode;
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

    private readonly ILogger<IKahuna> logger;

    public SequencesService(IKahuna sequences, ILogger<IKahuna> logger)
    {
        this.sequences = sequences;
        this.logger = logger;
    }

    public override Task<GrpcSequenceResponse> CreateSequence(GrpcCreateSequenceRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.CreateSequenceCore(r, c), static _ => SequenceMustRetry.Sequence());

    private async Task<GrpcSequenceResponse> CreateSequenceCore(GrpcCreateSequenceRequest request, ServerCallContext context)
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

    public override Task<GrpcSequenceResponse> GetSequence(GrpcGetSequenceRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.GetSequenceCore(r, c), static _ => SequenceMustRetry.Sequence());

    private async Task<GrpcSequenceResponse> GetSequenceCore(GrpcGetSequenceRequest request, ServerCallContext context)
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

    public override Task<GrpcSequenceAllocationResponse> NextSequenceValue(GrpcNextSequenceRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.NextSequenceValueCore(r, c), static _ => SequenceMustRetry.Allocation());

    private async Task<GrpcSequenceAllocationResponse> NextSequenceValueCore(GrpcNextSequenceRequest request, ServerCallContext context)
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

    public override Task<GrpcSequenceAllocationResponse> ReserveSequenceRange(GrpcReserveSequenceRangeRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.ReserveSequenceRangeCore(r, c), static _ => SequenceMustRetry.Allocation());

    private async Task<GrpcSequenceAllocationResponse> ReserveSequenceRangeCore(GrpcReserveSequenceRangeRequest request, ServerCallContext context)
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

    public override Task<GrpcSequenceResponse> DeleteSequence(GrpcDeleteSequenceRequest request, ServerCallContext context)
        => Guard(request, context, static (s, r, c) => s.DeleteSequenceCore(r, c), static _ => SequenceMustRetry.Sequence());

    private async Task<GrpcSequenceResponse> DeleteSequenceCore(GrpcDeleteSequenceRequest request, ServerCallContext context)
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

    // ── Retryable-failure guards ────────────────────────────────────────────────────────────────
    //
    // A retry loop must always receive a classifiable answer. A Raft resolution failure or an
    // inter-node transport failure means no definitive answer was produced; left unguarded it
    // reaches the caller as gRPC status Unknown, which clients do not retry. Genuine bugs are not
    // retryable and keep propagating.

    private async Task<TResponse> Guard<TRequest, TResponse>(
        TRequest request,
        ServerCallContext context,
        Func<SequencesService, TRequest, ServerCallContext, Task<TResponse>> handler,
        Func<TRequest, TResponse> refusal,
        [CallerMemberName] string handlerName = ""
    )
    {
        try
        {
            return await handler(this, request, context);
        }
        catch (Exception ex) when (RetryableFailureClassifier.IsRetryable(ex))
        {
            logger.LogWarning(
                "Mapping retryable {ExceptionType} on {Handler} to MustRetry: {Message}",
                ex.GetType().Name, handlerName, ex.Message);

            return refusal(request);
        }
    }
}

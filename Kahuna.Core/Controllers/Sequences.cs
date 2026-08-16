
using Kahuna.Shared.Sequences;

namespace Kahuna;

/// <summary>
/// Sequencer surface: delegation to the sequencer subsystem.
/// </summary>
public sealed partial class KahunaManager
{
    public Task<(SequenceResponseType, ReadOnlySequenceEntry?)> LocateAndGetSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.LocateAndGetSequence(name, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, long)> LocateAndCreateSequence(
        string name,
        long initialValue,
        long increment,
        long? maxValue,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.LocateAndCreateSequence(name, initialValue, increment, maxValue, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, SequenceAllocation)> LocateAndNextSequenceValue(
        string name,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.LocateAndNextSequenceValue(name, idempotencyKey, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, SequenceAllocation)> LocateAndReserveSequenceRange(
        string name,
        int count,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.LocateAndReserveSequenceRange(name, count, idempotencyKey, durability, cancellationToken);
    }

    public Task<SequenceResponseType> LocateAndDeleteSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.LocateAndDeleteSequence(name, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, ReadOnlySequenceEntry?)> GetSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.GetSequence(name, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, long)> CreateSequence(
        string name,
        long initialValue,
        long increment,
        long? maxValue,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.CreateSequence(name, initialValue, increment, maxValue, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, SequenceAllocation)> NextSequenceValue(
        string name,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.NextSequenceValue(name, idempotencyKey, durability, cancellationToken);
    }

    public Task<(SequenceResponseType, SequenceAllocation)> ReserveSequenceRange(
        string name,
        int count,
        string? idempotencyKey,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.ReserveSequenceRange(name, count, idempotencyKey, durability, cancellationToken);
    }

    public Task<SequenceResponseType> DeleteSequence(
        string name,
        SequenceDurability durability,
        CancellationToken cancellationToken
    )
    {
        return sequencer.DeleteSequence(name, durability, cancellationToken);
    }
}

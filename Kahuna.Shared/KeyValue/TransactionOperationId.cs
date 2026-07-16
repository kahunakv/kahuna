
using System.Buffers.Binary;

namespace Kahuna.Shared.KeyValue;

/// <summary>
/// 128-bit identifier that uniquely names a single operation within a transaction.
/// Callers supply one per operation to allow idempotent re-submission if a network
/// timeout leaves the outcome unknown.
/// </summary>
public readonly record struct TransactionOperationId(ulong High, ulong Low)
{
    public static readonly TransactionOperationId None;

    /// <summary>Returns true when this ID carries no meaningful identity.</summary>
    public bool IsEmpty => High == 0 && Low == 0;

    /// <summary>Converts a <see cref="Guid"/> to a <see cref="TransactionOperationId"/>.</summary>
    public static TransactionOperationId FromGuid(Guid g)
    {
        Span<byte> bytes = stackalloc byte[16];
        g.TryWriteBytes(bytes);
        ulong high = BinaryPrimitives.ReadUInt64LittleEndian(bytes[..8]);
        ulong low  = BinaryPrimitives.ReadUInt64LittleEndian(bytes[8..]);
        return new(high, low);
    }

    /// <summary>Allocates a new random <see cref="TransactionOperationId"/>.</summary>
    public static TransactionOperationId NewRandom() => FromGuid(Guid.NewGuid());

    /// <summary>
    /// Deterministically derives a distinct operation id for an indexed sub-step of this operation — for
    /// example one page of a paged scan. The result is a pure function of this id and <paramref name="index"/>,
    /// so a retried sub-step re-derives the same id and replays idempotently. Distinct indices always yield
    /// distinct ids (the mix is a bijection of the index for a fixed base), and the well-mixed output makes a
    /// clash with an unrelated random operation id in the same transaction negligible.
    /// </summary>
    public TransactionOperationId Derive(int index)
    {
        ulong seed = Mix(High) ^ Mix(Low + 0x9E3779B97F4A7C15UL) ^ (uint)index;
        return new(Mix(seed), Mix(~seed));
    }

    // splitmix64 finalizer — a bijection over ulong, so it preserves the injectivity of the index mix.
    private static ulong Mix(ulong z)
    {
        z += 0x9E3779B97F4A7C15UL;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
        return z ^ (z >> 31);
    }
}

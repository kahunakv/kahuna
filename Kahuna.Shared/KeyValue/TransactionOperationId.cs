
using System.Buffers.Binary;

namespace Kahuna.Shared.KeyValue;

/// <summary>
/// 128-bit identifier that uniquely names a single operation within a transaction.
/// Callers supply one per operation to allow idempotent re-submission if a network
/// timeout leaves the outcome unknown.
/// </summary>
public readonly record struct TransactionOperationId(ulong High, ulong Low)
{
    public static readonly TransactionOperationId None = default;

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
}

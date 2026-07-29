using Kahuna.Shared.KeyValue;

namespace Kahuna.Shared.Communication.Grpc;

/// <summary>
/// Converts <see cref="TransactionPriority"/> to and from its gRPC representation.
///
/// <para>The wire enum is deliberately offset by one so its zero value means "unspecified". A peer built
/// before the field existed sends nothing, which decodes as zero, and that must resolve to
/// <see cref="TransactionPriority.Normal"/> — the pre-existing behavior — rather than to
/// <see cref="TransactionPriority.Background"/>, which is what a straight cast would produce. Every
/// conversion goes through here so that offset lives in exactly one place.</para>
/// </summary>
public static class TransactionPriorityWire
{
    /// <summary>Encodes a priority for transmission. Never emits <c>Unspecified</c>: an explicit value is
    /// always sent, so a peer can distinguish "the sender chose Normal" from "the sender is too old to have
    /// an opinion" if it ever needs to.</summary>
    public static GrpcTransactionPriority ToGrpc(TransactionPriority priority)
        => (GrpcTransactionPriority)((int)priority + 1);

    /// <summary>Decodes a received priority, resolving <c>Unspecified</c> — and any value this build does not
    /// recognize — to <see cref="TransactionPriority.Normal"/>. An unknown ordinal from a newer peer is
    /// treated as ordinary work rather than rejected, since priority only affects start ordering and a
    /// misread ordinal must never fail an otherwise valid transaction.</summary>
    public static TransactionPriority FromGrpc(GrpcTransactionPriority priority)
    {
        int value = (int)priority - 1;

        return Enum.IsDefined((TransactionPriority)value) ? (TransactionPriority)value : TransactionPriority.Normal;
    }
}

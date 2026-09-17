using Kahuna.Shared.KeyValue;

namespace Kahuna.Shared.Communication.Grpc;

/// <summary>
/// Converts <see cref="TransactionConflictPolicy"/> to and from its gRPC and REST representations.
///
/// <para>The wire enum is offset by one so its zero value means "unspecified". A peer built before the field
/// existed sends nothing, which decodes as zero, and that must resolve to
/// <see cref="TransactionConflictPolicy.Normal"/> — the pre-existing behaviour. An unknown ordinal from a newer
/// or malformed peer also resolves to <see cref="TransactionConflictPolicy.Normal"/>: a misread value must never
/// make a transaction's intents stealable, so <see cref="TransactionConflictPolicy.Yield"/> is only ever the
/// result of an explicit, recognised value.</para>
/// </summary>
public static class TransactionConflictPolicyWire
{
    /// <summary>Encodes a policy for transmission. Never emits <c>Unspecified</c>.</summary>
    public static GrpcTransactionConflictPolicy ToGrpc(TransactionConflictPolicy policy)
        => (GrpcTransactionConflictPolicy)((int)Normalize(policy) + 1);

    /// <summary>Decodes a received policy, resolving <c>Unspecified</c> and any unrecognised value to
    /// <see cref="TransactionConflictPolicy.Normal"/>.</summary>
    public static TransactionConflictPolicy FromGrpc(GrpcTransactionConflictPolicy policy)
        => Normalize((TransactionConflictPolicy)((int)policy - 1));

    /// <summary>Resolves an out-of-range value (for example a raw number from a REST body) to
    /// <see cref="TransactionConflictPolicy.Normal"/>.</summary>
    public static TransactionConflictPolicy Normalize(TransactionConflictPolicy policy)
        => policy == TransactionConflictPolicy.Yield ? TransactionConflictPolicy.Yield : TransactionConflictPolicy.Normal;
}

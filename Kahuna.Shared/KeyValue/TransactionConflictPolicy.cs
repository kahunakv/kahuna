namespace Kahuna.Shared.KeyValue;

/// <summary>
/// How an interactive transaction behaves when another transaction meets one of its live write intents.
///
/// <para><see cref="Normal"/> is today's behaviour and the default at every layer: a live intent denies every
/// other writer until it is released, prepared or expired. <see cref="Yield"/> marks maintenance work (bulk
/// rewrites, backfills, compaction-like sweeps) that must never make foreground work fail: a
/// <see cref="Normal"/> transaction, or a plain write with no transaction, that meets an un-pinned yielding
/// intent takes the key over instead of being denied, and the yielding transaction aborts at its next touch
/// of the key or at commit. Two yielding transactions conflict exactly as two normal ones do.</para>
///
/// <para>The option is deliberately separate from <see cref="TransactionPriority"/>, which governs admission
/// order only and promises never to change locking semantics. A yielding transaction may not hold prefix
/// or range locks: the takeover rule covers point-key intents only, so those acquires are refused rather
/// than kept with silently different semantics. The option is accepted on interactive sessions only; a
/// script transaction has no way to express it.</para>
/// </summary>
public enum TransactionConflictPolicy
{
    /// <summary>A live write intent denies every other writer until it is released, prepared or expired.</summary>
    Normal = 0,

    /// <summary>
    /// The transaction loses every point-key write-intent conflict with a <see cref="Normal"/> transaction or
    /// a plain write: the other writer takes the key over, and this transaction aborts at its next touch of
    /// the key or at commit. It never commits a write to a key it lost.
    /// </summary>
    Yield = 1
}

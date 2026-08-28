namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Lock compatibility: S ∩ S coexist; S ∩ X, X ∩ S, X ∩ X conflict.
/// WriteFence coexists with S in both directions, conflicts with X and with another WriteFence.
/// Exclusive = 0 so an unset proto field defaults to exclusive, preserving all existing callers.
/// </summary>
public enum RangeLockMode
{
    Exclusive = 0,
    Shared    = 1,

    /// <summary>
    /// Blocks writes into the range without blocking readers. The write path refuses a mutation
    /// under any foreign range lock regardless of mode, so holding a WriteFence is enough to fence
    /// every writer; unlike <see cref="Exclusive"/>, acquisition tolerates foreign Shared holders
    /// and places no per-key write intents, so scans of the range keep flowing while it is held.
    /// This is the quiesce a range split/merge needs: a long-lived Serializable scanner's Shared
    /// range lock must not starve the split (the lock is clamped onto the children at cutover),
    /// while a foreign Exclusive holder — a writer mid-flight — still defers the fence.
    /// </summary>
    WriteFence = 2,
}

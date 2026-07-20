namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Represents the possible response types for key-value operations in the system.
/// </summary>
public enum KeyValueResponseType
{
    Set = 0,
    
    NotSet = 1,

    Extended = 2,

    Get = 3,

    Deleted = 4,

    Locked = 5,

    Unlocked = 6,

    Prepared = 7,

    Committed = 8,

    RolledBack = 9,

    Exists = 10,

    WaitingForReplication = 11,

    Errored = 99,

    InvalidInput = 100,
    /// <summary>
    /// Overloaded for two distinct cases — callers should handle both:
    /// <list type="bullet">
    ///   <item><description>
    ///     <b>Stale route</b>: the target partition moved or split since the request was routed;
    ///     re-resolve the key-space descriptor and retry on the new partition.
    ///   </description></item>
    ///   <item><description>
    ///     <b>Safe-time not reached</b>: a snapshot read at T is blocked by a live write intent whose
    ///     <c>CommitTimestamp</c> is undetermined (Zero) or ≤ T, but the retrying node is not the
    ///     partition leader (so the exponential back-off loop in <c>KeyValuesManager.TryGetValue</c>
    ///     cannot run there). The coordinator should retry the same read with the same T — no
    ///     re-routing is required.
    ///   </description></item>
    /// </list>
    /// If the distinction matters for a caller, inspect the accompanying context (e.g. whether a
    /// generation fence changed) or prefer checking for <see cref="WaitingForReplication"/> first
    /// (the transparent back-off path that never surfaces here when the leader handles the retry).
    /// </summary>
    MustRetry = 101,

    Aborted = 102,

    DoesNotExist = 103,

    AlreadyLocked = 104,

    /// <summary>
    /// The exclusive <b>prefix</b> lock is not supported over a key space that has been key-range
    /// split: a prefix lock is a single-partition bucket lock, but a split space spans several
    /// partitions. This is a deliberate deprecation — use the per-range exclusive <i>range</i> lock
    /// (<c>TryAcquireExclusiveRangeLock</c>) instead, which locks exactly the descriptors a scan
    /// touches. Returned instead of a generic <see cref="Errored"/> so callers can
    /// migrate deliberately rather than treating it as a transient failure.
    /// </summary>
    PrefixLockUnsupportedOnRangedSpace = 105,

    /// <summary>
    /// Returned by <c>GetRangeLocks</c> actor messages; the payload is in
    /// <see cref="KeyValueResponse.RangeLockList"/>. Not surfaced to external callers.
    /// </summary>
    RangeLocks = 106,

    /// <summary>
    /// Returned by <c>GetSafeTimestamp</c> actor messages; the payload is in
    /// <see cref="KeyValueResponse.Ticket"/>. When <c>Ticket</c> equals
    /// <c>HLCTimestamp.Zero</c> the shard has no in-flight prepared transactions.
    /// </summary>
    SafeTimestamp = 107,
}

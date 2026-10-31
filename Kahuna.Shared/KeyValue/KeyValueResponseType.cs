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

    /// <summary>
    /// The node refused to admit the transaction: either its admission queue was full or the caller's
    /// admission wait budget expired before a slot came free. Nothing was started — no identity minted, no
    /// lock taken, no write proposed — so retrying is always safe.
    /// <para>
    /// <b>Retryable, but back off first.</b> This is deliberately distinct from <see cref="MustRetry"/>,
    /// which signals a transient condition (a leader still being elected, a partition catching up) that
    /// clears on its own and should be retried promptly. Here the node is shedding load, so an immediate
    /// retry re-queues against the very saturation the gate exists to relieve. Callers should back off —
    /// or shed the work themselves — rather than spin.
    /// </para>
    /// </summary>
    AdmissionRefused = 108,

    /// <summary>
    /// A watch was requested over ephemeral keys. The refusal is permanent: an ephemeral write never reaches
    /// the replicated log, so no replica has a committed event to report for it, and a retry can never
    /// succeed. Returned instead of <see cref="InvalidInput"/> so a caller can tell this permanent refusal
    /// from a malformed request and move the keys it watches to persistent durability deliberately.
    /// </summary>
    WatchUnsupportedOnEphemeral = 109,

    /// <summary>
    /// A history hold was refused because a bound of the registry was reached: the holder already holds the
    /// maximum number of history holds, or the cluster does. Nothing was registered. Release holds the holder no
    /// longer needs, or raise the bound.
    /// </summary>
    HistoryHoldLimitReached = 110,

    /// <summary>
    /// A watch was refused because a node limit was reached: the node already has its maximum number of open
    /// watches, or the gRPC call already holds its maximum. Nothing was registered. Close watches that are no
    /// longer needed, open the watch on another call or node, or raise the limit.
    /// </summary>
    WatchLimitReached = 111,

    /// <summary>
    /// A snapshot hold was refused because a bound of the registry was reached: the holder already holds the
    /// maximum number of snapshot holds, or the cluster does. Nothing was registered. Release holds the holder no
    /// longer needs, or raise the bound.
    /// </summary>
    SnapshotHoldLimitReached = 112,

    /// <summary>
    /// A transaction operation was refused because the session already writes its maximum number of keys, or holds
    /// its maximum number of locks (<c>MaxTransactionMutations</c>). Nothing was applied, and the session stays open:
    /// it can still commit what it holds, or roll back. Not retryable; split the work into smaller transactions.
    /// </summary>
    TransactionMutationLimitReached = 113,
}

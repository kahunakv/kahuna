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
    MustRetry = 101,
    Aborted = 102,
    DoesNotExist = 103,
    AlreadyLocked = 104,

    /// <summary>
    /// The exclusive <b>prefix</b> lock is not supported over a key space that has been key-range
    /// split: a prefix lock is a single-partition bucket lock, but a split space spans several
    /// partitions. This is a deliberate deprecation — use the per-range exclusive <i>range</i> lock
    /// (<c>TryAcquireExclusiveRangeLock</c>) instead, which locks exactly the descriptors a scan
    /// touches (design §8). Returned instead of a generic <see cref="Errored"/> so callers can
    /// migrate deliberately rather than treating it as a transient failure.
    /// </summary>
    PrefixLockUnsupportedOnRangedSpace = 105
}

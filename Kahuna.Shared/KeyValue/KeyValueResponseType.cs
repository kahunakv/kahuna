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
    Errored = 99,
    InvalidInput = 100,
    MustRetry = 101,
    Aborted = 102,
    DoesNotExist = 103,
    AlreadyLocked = 104
}
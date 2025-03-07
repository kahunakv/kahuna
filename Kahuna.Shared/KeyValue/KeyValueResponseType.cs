namespace Kahuna.Shared.KeyValue;

public enum KeyValueResponseType
{
    Set = 0,
    NotSet = 1,
    Extended = 2,
    Get = 3,
    Deleted = 4,
    Errored = 99,
    InvalidInput = 100,
    MustRetry = 101,
    DoesNotExist = 102
}
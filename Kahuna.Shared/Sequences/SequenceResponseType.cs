namespace Kahuna.Shared.Sequences;

public enum SequenceResponseType
{
    Success = 0,
    NotFound = 1,
    AlreadyExists = 2,
    InvalidInput = 3,
    MaxValueExceeded = 4,
    MustRetry = 5,
    Aborted = 6,
    Error = 99
}

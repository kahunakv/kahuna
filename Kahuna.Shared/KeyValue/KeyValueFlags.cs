
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Represents the set of flags used to determine the behavior of key-value "set" operations.
/// </summary>
public enum KeyValueFlags
{
    None = 0,
    Set = 1,
    SetIfExists = 2,
    SetIfNotExists = 3,
    SetIfEqualToValue = 4,
    SetIfEqualToRevision = 5,
}
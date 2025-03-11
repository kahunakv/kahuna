
namespace Kahuna.Shared.KeyValue;

public enum KeyValueFlags
{
    None = 0,
    Set = 1,
    SetIfExists = 2,
    SetIfNotExists = 3,
    SetIfEqualToValue = 4,
    SetIfEqualToRevision = 5,
}
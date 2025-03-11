namespace Kahuna.KeyValues;

public enum KeyValueFlags
{
    None,
    Set,
    SetIfExists,
    SetIfNotExists,
    SetIfEqualToValue,
    SetIfEqualToRevision,
}
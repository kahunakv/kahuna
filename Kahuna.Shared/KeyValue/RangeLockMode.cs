namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Lock compatibility: S ∩ S coexist; S ∩ X, X ∩ S, X ∩ X conflict.
/// Exclusive = 0 so an unset proto field defaults to exclusive, preserving all existing callers.
/// </summary>
public enum RangeLockMode
{
    Exclusive = 0,
    Shared    = 1,
}

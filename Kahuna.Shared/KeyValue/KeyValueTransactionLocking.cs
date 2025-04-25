namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Represents the locking strategy used in a key-value transaction.
/// </summary>
public enum KeyValueTransactionLocking
{
    Pessimistic,
    Optimistic
}
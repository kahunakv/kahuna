
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Represents a key that has been modified within a transactional operation.
/// It includes the key value and its associated durability level.
/// </summary>
public class KeyValueTransactionModifiedKey
{
    public string? Key { get; set; }
    
    public KeyValueDurability Durability { get; set; }
}
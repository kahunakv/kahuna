
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Represents a key read within a transactional operation.
/// </summary>
public class KeyValueTransactionReadKey
{
    public string? Key { get; set; }

    public KeyValueDurability Durability { get; set; }

    public bool Exists { get; set; }

    public long Revision { get; set; }
}


namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Represents a parameter (placeholder) with a key and a value, used in key-value transaction operations.
/// </summary>
public sealed class KeyValueParameter
{
    public string? Key { get; set; }
    
    public string? Value { get; set; }
}
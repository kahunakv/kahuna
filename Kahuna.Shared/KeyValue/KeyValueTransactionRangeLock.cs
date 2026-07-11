namespace Kahuna.Shared.KeyValue;

/// <summary>
/// A range lock held within a transaction: its logical bounds, durability, and current mode.
/// Part of the consumer-facing working-set view returned by the coordinator.
/// </summary>
public sealed class KeyValueTransactionRangeLock
{
    public string? Prefix { get; set; }

    public string? StartKey { get; set; }

    public bool StartInclusive { get; set; }

    public string? EndKey { get; set; }

    public bool EndInclusive { get; set; }

    public KeyValueDurability Durability { get; set; }

    public RangeLockMode Mode { get; set; }
}

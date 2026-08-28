
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Selects which durable-transaction state kinds a range transaction-state gather carries:
/// completion receipts, canonical transaction records, prepared intents, or any combination.
/// The values mirror the wire bitmask, so a flag set here is the flag set on the request.
/// A paged gather (a positive item cap) requires exactly one flag — a shared cursor cannot
/// page heterogeneous kinds.
/// </summary>
[Flags]
public enum KeyValueRangeStateKinds
{
    Receipts = 1,
    Records = 2,
    Intents = 4,
    All = Receipts | Records | Intents,
}

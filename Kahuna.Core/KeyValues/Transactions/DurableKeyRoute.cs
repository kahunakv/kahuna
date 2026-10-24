namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Where a key routes when a durable finalize input is frozen: its data partition, the range-descriptor
/// generation it resolved through (zero for a hash-routed key), and for a key-range key the descriptor's start
/// bound. The key's own space together with <see cref="RangeStartKey"/> identifies the descriptor, so the freeze
/// can keep one dispatch fence per descriptor a partition's keys touch instead of one per key. A hash-routed key
/// has no descriptor and carries a null start bound; so does a whole-space seed descriptor, and the two never
/// coexist in one space.
/// </summary>
internal readonly record struct DurableKeyRoute(int PartitionId, long Generation, string? RangeStartKey);

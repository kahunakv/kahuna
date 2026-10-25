namespace Kahuna.Server.KeyValues.Transactions;

/// <summary>
/// Where a key routes when a durable finalize input is frozen: its data partition, the range-descriptor
/// generation it resolved through (zero for a hash-routed key), for a key-range key the descriptor's start
/// bound, and whether the key is key-range routed at all. A key-range key is fenced on its own at dispatch — a
/// range move quiesces a sub-interval of a descriptor, so whether a group may be appended is a per-key question —
/// while a hash-routed key is fenced once per space, since a hash space has no descriptor to move or quiesce. The
/// start bound is kept for the descriptor identity a reader of the route may still need; a hash-routed key and a
/// whole-space seed descriptor both carry null, which is why <see cref="IsKeyRange"/> is explicit.
/// </summary>
internal readonly record struct DurableKeyRoute(int PartitionId, long Generation, string? RangeStartKey, bool IsKeyRange);

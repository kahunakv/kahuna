namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// One key and the range-descriptor generation it resolved to when a durable submission's partition was frozen.
/// A fenced submission carries one of these for every range descriptor its keys touched on the partition (and
/// one per hash-routed key space, at generation zero), and the scheduler re-resolves each key at dispatch: when
/// any descriptor moved — a split, merge or move since freeze, or a hash space registered as key-range meanwhile —
/// the submission is released retryably instead of appending to a partition the range no longer routes to.
/// </summary>
/// <remarks>
/// The pair must always come from the same key: a fence that names one key and another key's generation reads as
/// stale against a live map that has not changed, and the submission is refused on every round.
/// </remarks>
public readonly record struct RangeFence(string Key, long Generation);


using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// An archived historical revision of a key. Unlike the previous {revision → byte[]} map,
/// each archived revision now carries the metadata needed to serve snapshot-isolated reads:
/// the value as it was, plus the HLC timestamp at which that revision was committed
/// (<see cref="LastModified"/>), its expiry and its state.
///
/// This lets range/point reads resolve "the value as of snapshot T" by selecting the highest
/// revision whose <see cref="LastModified"/> ≤ T, instead of dropping the key when the current
/// revision is newer than the snapshot.
/// </summary>
internal readonly record struct KeyValueRevisionEntry(
    byte[]? Value,
    HLCTimestamp LastModified,
    HLCTimestamp Expires,
    KeyValueState State);

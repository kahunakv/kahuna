namespace Kahuna.Server.KeyValues;

/// <summary>
/// One bounded seek of a batched range probe: the keys of the probe's prefix between the two bounds, at most
/// <see cref="Limit"/> of them, in key order. A null bound is open on that side, as on a single range page.
/// </summary>
public readonly record struct KeyValueProbeRange(
    string? StartKey,
    bool StartInclusive,
    string? EndKey,
    bool EndInclusive,
    int Limit);

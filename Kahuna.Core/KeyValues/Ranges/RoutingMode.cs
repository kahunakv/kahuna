namespace Kahuna.Server.KeyValues.Ranges;

/// <summary>
/// How a key space routes a key to a Raft partition.
/// "hash-routed vs key-routed coexistence"). A key space is unambiguously one mode.
/// </summary>
internal enum RoutingMode
{
    /// <summary>
    /// Hash routing (the default, and the system / <c>{db}/meta</c> mode): the key's prefix is
    /// hashed onto a fixed partition slice via <c>IRaft.GetPartitionKey</c>. No key locality.
    /// </summary>
    Hash,

    /// <summary>
    /// Key-range routing (opted in by row/index spaces): the key is resolved through the replicated
    /// <see cref="RangeMap"/> to the descriptor whose half-open ordinal interval contains it.
    /// Contiguous keys co-locate and the descriptor's generation fences stale routing.
    /// </summary>
    KeyRange
}

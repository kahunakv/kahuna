namespace Kahuna.Server.KeyValues;

/// <summary>How a batched probe locks each key, or each seek's bounds, just before it reads it.</summary>
public enum KeyValueProbeLock
{
    /// <summary>Read only.</summary>
    None,

    /// <summary>
    /// Take a Shared range lock on each key (<c>[k,k]</c>) or over each seek's bounds, in request order, and read it
    /// in the same turn. The first refusal stops the batch: the keys or seeks after it are neither locked nor read.
    /// </summary>
    Shared,

    /// <summary>
    /// Replay of a probe whose locks a completed coordinator operation already recorded: a key or seek is read only
    /// when this transaction still holds its Shared lock over exactly those bounds, whose lease is then renewed. No
    /// lock is taken, so a replay can never grant a lock the coordinator does not know about. A key or seek without
    /// its lock answers <c>MustRetry</c>, and the batch does not stop at it.
    /// </summary>
    SharedReentryOnly,
}

namespace Kahuna.Server.KeyValues;

/// <summary>How a batched probe locks each key just before it reads it.</summary>
public enum KeyValueProbeLock
{
    /// <summary>Read only.</summary>
    None,

    /// <summary>
    /// Take a Shared <c>[k,k]</c> range lock on each key, in key order, and read the key in the same turn. The
    /// first refusal stops the batch: the keys after it are neither locked nor read.
    /// </summary>
    Shared,

    /// <summary>
    /// Replay of a probe whose locks a completed coordinator operation already recorded: a key is read only when
    /// this transaction still holds its Shared <c>[k,k]</c> lock, whose lease is then renewed. No lock is taken,
    /// so a replay can never grant a lock the coordinator does not know about. A key without its lock answers
    /// <c>MustRetry</c>, and the batch does not stop at it.
    /// </summary>
    SharedReentryOnly,
}

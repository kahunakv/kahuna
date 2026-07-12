
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// The confirmed outcome of a transaction-scoped operation, carried from the partition leader that
/// executed it back to the coordinator node so the coordinator can fold the effect into its
/// server-owned working set and cache the response for idempotent replay. A single structured payload
/// spans every operation shape (point write, point/prefix/range lock acquire and release, point read,
/// scan) so the completion contract does not fan out into a separate method per shape.
/// </summary>
public sealed record OperationCompletionPayload
{
    /// <summary>A key written or deleted by the operation.</summary>
    public string? ModifiedKey { get; init; }

    /// <summary>
    /// The keys a batch operation wrote or deleted, in canonical request order (each with its own durability,
    /// independent of <see cref="Durability"/>). The coordinator folds them in list order so the first
    /// persistent key deterministically anchors the transaction record.
    /// </summary>
    public IReadOnlyList<(string Key, KeyValueDurability Durability)>? ModifiedKeys { get; init; }

    /// <summary>A point lock the operation acquired.</summary>
    public string? AcquiredPointLock { get; init; }

    /// <summary>A point lock the operation released.</summary>
    public string? ReleasedPointLock { get; init; }

    /// <summary>A prefix lock the operation acquired.</summary>
    public string? AcquiredPrefixLock { get; init; }

    /// <summary>A prefix lock the operation released.</summary>
    public string? ReleasedPrefixLock { get; init; }

    /// <summary>A range lock (bounds + mode) the operation acquired, upgraded, or renewed.</summary>
    public (RangeLockKey Range, RangeLockMode Mode)? AcquiredRangeLock { get; init; }

    /// <summary>A range lock the operation released.</summary>
    public RangeLockKey? ReleasedRangeLock { get; init; }

    /// <summary>A single key observed by a point read.</summary>
    public KeyValueTransactionReadKey? Read { get; init; }

    /// <summary>The items observed by a scan, recorded with point-read-set semantics.</summary>
    public IReadOnlyList<KeyValueTransactionReadKey>? ReadObservations { get; init; }

    /// <summary>Durability shared by the effect keys/locks above.</summary>
    public KeyValueDurability Durability { get; init; }

    /// <summary>The response to cache for an idempotent replay of this operation id.</summary>
    public KeyValueResponseType CachedType { get; init; }

    public long CachedRevision { get; init; }

    public HLCTimestamp CachedTimestamp { get; init; }
}

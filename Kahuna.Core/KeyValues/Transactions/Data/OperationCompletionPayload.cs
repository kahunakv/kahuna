
using Kommander.Time;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>The actor-confirmed committed value of one modified key, echoed from the participant to the coordinator
/// so the coordinator can stage it losslessly and finalize the transaction through the durable-intent path (rather
/// than the manual ticket path). A null <see cref="Value"/> is a deletion. The TTL is relative in ms (0 = none),
/// resolved to an absolute expiry at freeze. <see cref="NoRevision"/> carries whether the write suppressed history
/// retention so the durable materialization matches a direct <c>SET NOREV</c>.</summary>
public readonly record struct StagedMutationEffect(string Key, byte[]? Value, long Revision, long ExpiresMs, bool NoRevision);

/// <summary>
/// The confirmed outcome of a transaction-scoped operation, carried from the partition leader that
/// executed it back to the coordinator node so the coordinator can fold the effect into its
/// server-owned working set and cache the response for idempotent replay. A single structured payload
/// spans every operation shape (point write, point/prefix/range lock acquire and release, point read,
/// scan) so the completion contract does not fan out into a separate method per shape.
///
/// <para>
/// Instances are pooled through <see cref="OperationCompletionPayloadPool"/>: the creator rents,
/// fills, and recycles the shell only while it provably holds the sole reference. A shell handed to
/// the participant retry cache is never recycled, because recovery can hand its reference to
/// concurrent retries. The coordinator fold copies every item it retains, so recycling the shell
/// never disturbs folded state. Every property MUST be reset in <see cref="Clear"/> — a field that
/// survives a recycle leaks one operation's effect into another transaction.
/// </para>
/// </summary>
public sealed class OperationCompletionPayload
{
    /// <summary>A key written or deleted by the operation.</summary>
    public string? ModifiedKey { get; set; }

    /// <summary>
    /// The keys a batch operation wrote or deleted, in canonical request order (each with its own durability,
    /// independent of <see cref="Durability"/>). The coordinator folds them in list order so the first
    /// persistent key deterministically anchors the transaction record.
    /// </summary>
    public IReadOnlyList<(string Key, KeyValueDurability Durability)>? ModifiedKeys { get; set; }

    /// <summary>A point lock the operation acquired.</summary>
    public string? AcquiredPointLock { get; set; }

    /// <summary>
    /// The point locks a batch lock-acquire operation acquired (each with its own durability, independent of
    /// <see cref="Durability"/>). The coordinator folds every one into the held-lock set so commit/rollback
    /// release them all.
    /// </summary>
    public IReadOnlyList<(string Key, KeyValueDurability Durability)>? AcquiredPointLocks { get; set; }

    /// <summary>A point lock the operation released.</summary>
    public string? ReleasedPointLock { get; set; }

    /// <summary>A prefix lock the operation acquired.</summary>
    public string? AcquiredPrefixLock { get; set; }

    /// <summary>A prefix lock the operation released.</summary>
    public string? ReleasedPrefixLock { get; set; }

    /// <summary>A range lock (bounds + mode) the operation acquired, upgraded, or renewed.</summary>
    public (RangeLockKey Range, RangeLockMode Mode)? AcquiredRangeLock { get; set; }

    /// <summary>A range lock the operation released.</summary>
    public RangeLockKey? ReleasedRangeLock { get; set; }

    /// <summary>A single key observed by a point read.</summary>
    public KeyValueTransactionReadKey? Read { get; set; }

    /// <summary>The items observed by a scan, recorded with point-read-set semantics.</summary>
    public IReadOnlyList<KeyValueTransactionReadKey>? ReadObservations { get; set; }

    /// <summary>
    /// The actor-confirmed committed values for the persistent modified keys, echoed so the coordinator can stage
    /// them and take the durable-intent finalize path. Present only for persistent writes; a transaction with an
    /// unstaged persistent modified key falls back to the manual ticket path (never incorrect, just not batched).
    /// </summary>
    public IReadOnlyList<StagedMutationEffect>? StagedMutations { get; set; }

    /// <summary>Durability shared by the effect keys/locks above.</summary>
    public KeyValueDurability Durability { get; set; }

    /// <summary>The response to cache for an idempotent replay of this operation id.</summary>
    public KeyValueResponseType CachedType { get; set; }

    public long CachedRevision { get; set; }

    public HLCTimestamp CachedTimestamp { get; set; }

    /// <summary>
    /// Resets every property to its post-construction default so a recycled shell is
    /// indistinguishable from a fresh <c>new()</c>. Only the shell's references are dropped: the
    /// objects they pointed to may live on in the coordinator working set. A property added to this
    /// class MUST also be reset here (the payload-reset test enforces the pairing).
    /// </summary>
    internal void Clear()
    {
        ModifiedKey = null;
        ModifiedKeys = null;
        AcquiredPointLock = null;
        AcquiredPointLocks = null;
        ReleasedPointLock = null;
        AcquiredPrefixLock = null;
        ReleasedPrefixLock = null;
        AcquiredRangeLock = null;
        ReleasedRangeLock = null;
        Read = null;
        ReadObservations = null;
        StagedMutations = null;
        Durability = default;
        CachedType = default;
        CachedRevision = 0;
        CachedTimestamp = default;
    }
}

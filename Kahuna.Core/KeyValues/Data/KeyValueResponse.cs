
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// Represents a response object for the KeyValueActor operations
/// </summary>
/// <remarks>
/// The <see cref="KeyValueResponse"/> class encapsulates the result of operations performed within the
/// key-value actor. It is designed to handle various response scenarios, including success, errors,
/// locks, and other specific outcomes. The response may include additional metadata such as the revision
/// number, timestamp, or context items depending on the operation performed.
/// </remarks>
/// <summary>
/// What transiently denied a write, when the actor names it. Tells settlement lag — a wait the caller can give
/// a long budget, because the holder is already past the point of waiting on anyone — from a lock conflict
/// that a client bounds with its deadlock budget.
/// </summary>
public enum WriteDenialCause
{
    /// <summary>The denial names no cause (a lock conflict, a stale route, or an answer that is not a denial).</summary>
    None = 0,

    /// <summary>A foreign durable prepared intent whose canonical decision is still pending covers the key: its
    /// coordinator is inside its decision window and holds no further waits, so the write only has to wait.</summary>
    DurableIntent = 1,

    /// <summary>A foreign write intent whose transaction is already durably decided still covers the key: only
    /// its deferred settlement has to run to clear it.</summary>
    SettlingWriteIntent = 2
}

public sealed class KeyValueResponse
{
    /// <summary>
    /// Gets the response type of the key-value operation.
    /// </summary>
    /// <remarks>
    /// The <see cref="Type"/> property indicates the result or response type of the operation performed
    /// against the key-value system. It provides information on whether the operation was successful,
    /// encountered errors, involved locks, or resulted in other specific states. The value is derived from
    /// the <see cref="KeyValueResponseType"/> enumeration, which defines all possible response types.
    /// </remarks>
    public KeyValueResponseType Type { get; }

    /// <summary>
    /// Gets the revision number associated with the key-value operation.
    /// </summary>
    /// <remarks>
    /// The <see cref="Revision"/> property represents a unique, incremental value that indicates
    /// the specific version or state of the key-value store after a particular operation.
    /// It is used for concurrency control and tracking changes within the system, ensuring that
    /// modifications can be identified with precision based on their order of execution.
    /// </remarks>
    public long Revision { get; }

    /// <summary>
    /// Gets the hybrid logical clock (HLC) timestamp associated with preparation of the mutation
    /// when the key-value operation was last modified or processed as part of a transaction.
    /// </summary>
    public HLCTimestamp Ticket { get; }

    /// <summary>
    /// Gets the read-only key-value context associated with the response. Used in the 'get' operation.
    /// </summary>
    /// <remarks>
    /// The <see cref="Entry"/> property provides additional context about the key-value operation if available.
    /// It encapsulates metadata or supplementary information relevant to the request, such as conditions
    /// or detailed operation state. This property can return null if no context is associated with the response.
    /// </remarks>
    public ReadOnlyKeyValueEntry? Entry { get; }
    
    /// <summary>
    /// Used in the 'get by bucket' operation to return all the found values.
    /// </summary>
    public List<(string, ReadOnlyKeyValueEntry)>? Items { get; }

    /// <summary>
    /// Used in the 'get by range' operation to return a paged result with cursor.
    /// </summary>
    public KeyValueGetByRangeResult? RangeResult { get; }

    /// <summary>
    /// Used by GetRangeLocks to return the live range-lock entries for a key space.
    /// </summary>
    public List<KeyValueRangeLock>? RangeLockList { get; private set; }

    /// <summary>
    /// Transaction id of the conflicting lock holder at denial time.
    /// Advisory only — may be stale by the time the caller acts.
    /// <see cref="HLCTimestamp.Zero"/> means no holder info (non-denial outcomes, or denials
    /// where no single holder applies such as <c>WaitingForReplication</c> / infrastructural
    /// <c>Errored</c>). Never gate correctness on this field; the actual lock is the sole
    /// safety mechanism.
    /// </summary>
    public HLCTimestamp HolderTransactionId { get; private set; }

    /// <summary>
    /// Factory for a denial response that carries the conflicting holder's transaction id.
    /// Use instead of <see cref="KeyValueStaticResponses.AlreadyLockedResponse"/> at sites
    /// where the holder is known.
    /// </summary>
    public static KeyValueResponse Denied(KeyValueResponseType type, HLCTimestamp holder) =>
        new(type) { HolderTransactionId = holder };

    /// <summary>
    /// What transiently denied a write, when the actor names it; <see cref="WriteDenialCause.None"/> otherwise.
    /// The manager's write loop routes the holder's decision for a named cause before it answers the caller.
    /// </summary>
    public WriteDenialCause DenialCause { get; private set; }

    /// <summary>A transient denial that names the holder and what kind of wait it is.</summary>
    public static KeyValueResponse Denied(KeyValueResponseType type, HLCTimestamp holder, WriteDenialCause cause) =>
        new(type) { HolderTransactionId = holder, DenialCause = cause };

    /// <summary>
    /// The covered keys a range-lock acquire could not take, when the actor knows them. Advisory, like
    /// <see cref="HolderTransactionId"/>: the acquire loop uses them to resolve or settle the foreign
    /// transaction that holds them, never to decide whether the lock was granted. Null when the answer
    /// names no key (a replication intent in flight, or a non-range acquire).
    /// </summary>
    public List<string>? BlockingKeys { get; private set; }

    /// <summary>
    /// A denial or wait that names the holder and the covered keys it holds, so the caller can act on that
    /// transaction (route its decision, or settle its already-decided intents) before it retries.
    /// </summary>
    public static KeyValueResponse Blocked(KeyValueResponseType type, HLCTimestamp holder, List<string> blockingKeys) =>
        new(type) { HolderTransactionId = holder, BlockingKeys = blockingKeys };

    /// <summary>
    /// True on the answer of a release that could only acknowledge what the node holds
    /// (<c>KeyValueRequest.ReleaseOnlyIfHeld</c>) and found nothing of the transaction on the key, prefix or
    /// range. The type is <see cref="KeyValueResponseType.MustRetry"/>, as for any answer the node cannot vouch
    /// for; the flag tells this cause from a transient one (a busy actor, a replication intent in flight), so the
    /// locator can carry the release to the other replicas instead of giving up on it.
    /// </summary>
    internal bool NothingHeld { get; private init; }

    /// <summary>A retryable answer that says nothing of the transaction is held here; see <see cref="NothingHeld"/>.</summary>
    internal static KeyValueResponse NothingHeldHere() => new(KeyValueResponseType.MustRetry) { NothingHeld = true };

    /// <summary>
    /// On the clean answer of a commit-time probe that asked <see cref="KeyValueConflictChecks.CommitFloor"/>: the
    /// highest snapshot timestamp a read was served the committed head at over the caller's own staged intent on
    /// the key (<see cref="KeyValueWriteIntent.CommitFloor"/>). The caller mints its commit timestamp strictly
    /// above it. Zero when no snapshot read passed over the intent, and on every other answer.
    /// </summary>
    internal HLCTimestamp CommitFloor { get; private init; }

    /// <summary>The clean probe answer that also reports the commit floor of the caller's intent on the key.</summary>
    internal static KeyValueResponse HeldWithCommitFloor(HLCTimestamp commitFloor) =>
        new(KeyValueResponseType.DoesNotExist, KeyValueStaticResponses.DoesNotExistContextResponse.Entry) { CommitFloor = commitFloor };

    /// <summary>
    /// Construtor
    /// </summary>
    /// <param name="type"></param>
    public KeyValueResponse(KeyValueResponseType type)
    {
        Type = type;
    }
    
    public KeyValueResponse(KeyValueResponseType type, long revision)
    {
        Type = type;
        Revision = revision;
    }
    
    public KeyValueResponse(KeyValueResponseType type, long revision, HLCTimestamp lastModified)
    {
        Type = type;
        Revision = revision;
        Ticket = lastModified;
    }
    
    public KeyValueResponse(KeyValueResponseType type, HLCTimestamp ticket)
    {
        Type = type;
        Ticket = ticket;
    }
    
    public KeyValueResponse(KeyValueResponseType type, ReadOnlyKeyValueEntry? entry)
    {
        Type = type;
        Entry = entry;
    }
    
    public KeyValueResponse(KeyValueResponseType type, List<(string, ReadOnlyKeyValueEntry)> items)
    {
        Type = type;
        Items = items;
    }

    public KeyValueResponse(KeyValueResponseType type, KeyValueGetByRangeResult rangeResult)
    {
        Type = type;
        RangeResult = rangeResult;
    }

    public static KeyValueResponse ForRangeLocks(List<KeyValueRangeLock> locks) =>
        new(KeyValueResponseType.RangeLocks) { RangeLockList = locks };

    /// <summary>
    /// The per-key answers of a batched probe, in request order. Null on every other response, and on a probe
    /// that was answered as a whole (refused, or expired before its backend read returned): <see cref="Type"/>
    /// is then the answer for every key.
    /// </summary>
    public KeyValueProbeResult[]? ProbeResults { get; private set; }

    /// <summary>The answer of a batched probe that reached its keys: one result per key, in request order.</summary>
    public static KeyValueResponse ForProbe(KeyValueProbeResult[] results) =>
        new(KeyValueResponseType.Get) { ProbeResults = results };

    /// <summary>
    /// The per-seek answers of a batched range probe, in request order. Null on every other response, and on a range
    /// probe that was answered as a whole (refused, or expired before its disk pages returned): <see cref="Type"/> is
    /// then the answer for every seek.
    /// </summary>
    public KeyValueProbeRangeResult[]? ProbeRangeResults { get; private set; }

    /// <summary>The answer of a batched range probe that reached its seeks: one result per seek, in request order.</summary>
    public static KeyValueResponse ForProbeRanges(KeyValueProbeRangeResult[] results) =>
        new(KeyValueResponseType.Get) { ProbeRangeResults = results };
}
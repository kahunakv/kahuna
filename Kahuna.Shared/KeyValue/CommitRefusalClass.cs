
namespace Kahuna.Shared.KeyValue;

/// <summary>
/// Why a transaction commit was refused, carried beside the <see cref="KeyValueResponseType"/> of the commit
/// answer. The response type says whether the refusal is terminal (<see cref="KeyValueResponseType.Aborted"/>)
/// or retryable (<see cref="KeyValueResponseType.MustRetry"/>); this class says which arm of the coordinator
/// refused, so a client can report it and choose its retry budget without reading server logs or counters.
/// </summary>
public enum CommitRefusalClass
{
    /// <summary>The commit was not refused, or the answer came from a node that does not report a class.</summary>
    None = 0,

    /// <summary>Another transaction's write or lock conflicts with this transaction's read set or written keys
    /// (a write-skew probe hit, a stale read, a foreign range lock over a written key).</summary>
    Conflict = 1,

    /// <summary>A validated base of a staged write was overtaken by another transaction's commit before this
    /// transaction's prepare was durable; committing would have discarded that write.</summary>
    StagedBaseMoved = 2,

    /// <summary>The frozen decision deadline passed before the commit decision could be recorded, so the
    /// transaction yielded to the presumed-abort protocol.</summary>
    DecisionDeadline = 3,

    /// <summary>The prepare retry budget ran out while other transactions' intents held the written keys.</summary>
    PrepareExhausted = 4,

    /// <summary>A point, prefix or range lock the transaction was granted could not be proven still held: a
    /// partition leader change dropped it.</summary>
    LostLock = 5,

    /// <summary>The write intent of a staged write lapsed or was dropped before the commit probe ran.</summary>
    LostStagedIntent = 6,

    /// <summary>A confirmed staging was lost to a partition leader change and a later write staged over the
    /// committed head as if the key were untouched.</summary>
    StagedChainBreak = 7,

    /// <summary>The canonical record was aborted by recovery, a reaper or a rollback fence before this attempt
    /// could commit.</summary>
    PresumedAbort = 8,

    /// <summary>The transaction was rolled back explicitly.</summary>
    ExplicitRollback = 9,

    /// <summary>The working set cannot be committed as shaped: modified keys without a lock set, an ephemeral key
    /// in a durable transaction, a durable transaction without a record anchor, or a mutation that cannot be
    /// staged for a durable commit.</summary>
    InvalidWorkingSet = 10,

    /// <summary>The finalize threw before anything durable was proposed.</summary>
    Faulted = 11,

    /// <summary>Retryable: decided-but-unsettled predecessors still held the written keys when the decision
    /// deadline arrived. Nothing was decided; the same commit can be retried once settlement catches up.</summary>
    SettlementLag = 12,

    /// <summary>Retryable: the attempt reached no record-backed outcome (replication unavailable, a drain
    /// timeout, a probe that proved nothing). The same commit must be retried under the same handle.</summary>
    Unresolved = 13,

    /// <summary>Retryable after a backoff: the node refused to admit the finalize at capacity.</summary>
    AdmissionCapacity = 14,

    /// <summary>The coordinator does not know the transaction and no durable record could be consulted.</summary>
    UnknownTransaction = 15,

    /// <summary>Retryable: the commit timestamp fell outside the commit admission window of a participant's log
    /// entry. It was minted too long before its prepare reached the log, or the coordinator's clock ran more than
    /// 5 s ahead of the participant's leader. Nothing was committed; a retry mints a new commit timestamp.</summary>
    CommitTimestampOutsideWindow = 16
}

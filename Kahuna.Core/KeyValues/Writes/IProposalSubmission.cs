using Kommander.Data;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// The neutral, producer-agnostic unit the partition write scheduler queues, batches, and completes. The
/// scheduler owns only queueing and Raft completion; each submission carries its own ordered typed log entries,
/// its byte cost, its flush-time staleness rule, and its exactly-once completion adapter, so different producers
/// (direct key/value writes today; durable prepare/decision/resolution records later) can share one per-partition
/// proposal without the scheduler knowing their internals.
///
/// <para>A submission is the scheduler's indivisible unit — it is added to a batch whole, released whole when
/// stale/expired, and completed whole — so a submission carrying more than one entry <b>is</b> an atomic ordered
/// bundle: the entries reach Raft in one proposal, in <see cref="Entries"/> order, and are never split across
/// proposals. Direct writes carry a single entry; a durable anchor init+prepare or decision+resolution carries an
/// ordered group.</para>
/// </summary>
internal interface IProposalSubmission
{
    /// <summary>The physical Raft partition these records replicate to.</summary>
    int PartitionId { get; }

    /// <summary>Which admission budget this submission draws on. Ordinary work (direct writes, durable
    /// initialize/prepare) is bounded strictly by the base budget; terminal work (decision, settle,
    /// materialization, recovery, transfer) may draw on the reserve headroom so it is never starved by an
    /// ordinary-write burst.</summary>
    WriteAdmissionClass AdmissionClass { get; }

    /// <summary>Serialized byte cost (sum over <see cref="Entries"/>), for byte-budgeted batching and admission.</summary>
    int ByteLength { get; }

    /// <summary>The ordered, indivisible log entries this submission contributes to its partition's proposal. Every
    /// entry is auto-commit with generation zero; the order is preserved and never split across proposals.</summary>
    IReadOnlyList<RaftProposalEntry> Entries { get; }

    /// <summary>Millisecond admission tick, stamped by the scheduler from its clock for linger/age math.</summary>
    long EnqueueTicks { get; set; }

    /// <summary>Producer-owned flush-time staleness check: true releases the submission retryably instead of
    /// proposing it (e.g. a key-range write whose descriptor moved since admission). Producers with no fence
    /// return false.</summary>
    bool IsStale(IWriteRangeFence fence);

    /// <summary>Exactly-once terminal completion — the batch committed; apply the record's effect and resolve the
    /// producer's caller.</summary>
    void Complete();

    /// <summary>Exactly-once terminal unwind — the batch did not commit (or the submission was released before
    /// dispatch). <paramref name="transient"/> true asks the producer's caller to retry (MustRetry).</summary>
    void Release(bool transient);
}

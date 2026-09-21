using Kommander.Data;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>
/// A durable-intent 2PC record (transaction-record init/decision, prepared-intent prepare, or resolution) submitted
/// to the shared partition write scheduler so it coalesces with concurrent transactions' records to the same
/// partition into one <c>ReplicateEntries</c> proposal. The producer adapter for the durable finalizer: its
/// completion resolves a task the finalizer awaits — committed means the batch reached Raft, released means it did
/// not (retryable), and the finalizer maps that to the transaction outcome. Multiple entries form an atomic ordered
/// bundle (e.g. anchor init+prepare). Durable records target a partition resolved at freeze and replicate at
/// generation zero, so they carry no key-range fence.
/// </summary>
internal sealed class DurableProposalSubmission : IProposalSubmission
{
    private readonly TaskCompletionSource<bool> completion;

    // Runs on Complete() — the scheduler's per-partition completion path — once the batch committed. It does NOT
    // apply the submission's durable records: the record/intent stores are mutated only by the ordered consumer
    // apply that Raft drives in log order, and a completion can run before that apply (the quorum-durable fast
    // path releases the proposal ahead of the leader's own consumer) or long after it. Instead it waits for the
    // ordered apply of each entry — identified by the committed Raft log index the executor reported per entry —
    // and resolves whether every PREPARE in the bundle was acknowledged (took ownership of its key); a rejected
    // prepare resolves the producer's Committed task false so the finalizer aborts instead of committing an
    // unrecoverable mutation. Null in tests that only assert scheduling, not store state.
    private readonly Func<int, IReadOnlyList<RaftProposalEntry>, IReadOnlyList<long>?, Task<DurableCompletionAnswer>>? applyOnCommit;

    // Written before the producer's task resolves (the TrySetResult publishes it), read by the producer after.
    private DurableCompletionAnswer answer = DurableCompletionAnswer.NotCommitted;

    // The logical key + range-descriptor generation this submission's partition was resolved against at freeze. A
    // null key opts out of the fence (post-decision settle/materialize, recovery, and state-transfer imports,
    // which must apply regardless of topology); a non-null key re-fences at dispatch so a split/merge since freeze
    // releases the submission retryably instead of appending to a partition the range no longer routes to.
    private readonly string? fenceKey;

    private readonly long fenceGeneration;

    public int PartitionId { get; }

    public WriteAdmissionClass AdmissionClass { get; }

    /// <summary>The stage that produced this record, fixed at the creation site (or carried over the wire from
    /// the origin for a forwarded bundle); dispatch tags the queue-delay histogram with it.</summary>
    public WriteSubmissionStage Stage { get; }

    public int ByteLength { get; }

    public IReadOnlyList<RaftProposalEntry> Entries { get; }

    public long EnqueueTicks { get; set; }

    public DurableProposalSubmission(
        int partitionId,
        IReadOnlyList<RaftProposalEntry> entries,
        TaskCompletionSource<bool> completion,
        WriteAdmissionClass admissionClass,
        WriteSubmissionStage stage,
        Func<int, IReadOnlyList<RaftProposalEntry>, IReadOnlyList<long>?, Task<DurableCompletionAnswer>>? applyOnCommit = null,
        string? fenceKey = null,
        long fenceGeneration = 0)
    {
        PartitionId = partitionId;
        AdmissionClass = admissionClass;
        Stage = stage;
        Entries = entries;
        this.completion = completion;
        this.applyOnCommit = applyOnCommit;
        this.fenceKey = fenceKey;
        this.fenceGeneration = fenceGeneration;

        int bytes = 0;
        for (int i = 0; i < entries.Count; i++)
            bytes += entries[i].Data.Length;
        ByteLength = bytes;
    }

    /// <summary>Resolves true once the batch carrying this record committed to Raft and every prepare it carried
    /// took ownership of its key; false when the batch did not commit, a prepare was refused, or this node did not
    /// observe the ordered apply (<see cref="Answer"/> tells which).</summary>
    public Task<bool> Committed => completion.Task;

    /// <summary>How the submission ended, valid once <see cref="Committed"/> has resolved. A producer that must tell a
    /// durable batch with a refused prepare (drive the truthful outcome against the record) from one whose apply
    /// this node never observed (retry as if nothing were durable) reads this rather than the folded bool.</summary>
    public DurableCompletionAnswer Answer => answer;

    /// <summary>Whether the batch is known durable on this node's account: the ordered apply was observed, whatever
    /// it said about the prepares. False for a released submission and for an unobserved apply, both of which the
    /// producer treats as a clean retry.</summary>
    public bool BatchObserved => answer is DurableCompletionAnswer.Acknowledged or DurableCompletionAnswer.Refused;

    public bool IsStale(IWriteRangeFence fence) => fenceKey is not null && fence.IsStale(fenceKey, fenceGeneration, PartitionId);

    /// <summary>The batch committed: wait for the ordered apply of this submission's records (never applying them
    /// here), then resolve the producer with the answer. Never blocks the scheduler's lane: a wait that is not
    /// already satisfied resolves the producer from its continuation. An adapter that faults resolves
    /// <see cref="DurableCompletionAnswer.Unobserved"/>: the batch is durable but this node cannot say how its apply
    /// went, and a retry against the current leader is idempotent.</summary>
    public void Complete(IReadOnlyList<long>? entryLogIndices)
    {
        if (applyOnCommit is null)
        {
            Resolve(DurableCompletionAnswer.Acknowledged);
            return;
        }

        Task<DurableCompletionAnswer> awaited;
        try
        {
            awaited = applyOnCommit(PartitionId, Entries, entryLogIndices);
        }
        catch (Exception)
        {
            Resolve(DurableCompletionAnswer.Unobserved);
            return;
        }

        if (awaited.IsCompleted)
        {
            Resolve(awaited.IsCompletedSuccessfully ? awaited.Result : DurableCompletionAnswer.Unobserved);
            return;
        }

        _ = awaited.ContinueWith(
            static (task, state) => ((DurableProposalSubmission)state!).Resolve(task.IsCompletedSuccessfully ? task.Result : DurableCompletionAnswer.Unobserved),
            this,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private void Resolve(DurableCompletionAnswer outcome)
    {
        answer = outcome;
        completion.TrySetResult(outcome == DurableCompletionAnswer.Acknowledged);
    }

    public void Release(bool transient) => Resolve(DurableCompletionAnswer.NotCommitted);
}

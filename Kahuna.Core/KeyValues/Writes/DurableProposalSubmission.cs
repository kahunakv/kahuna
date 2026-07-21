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

    // The single ordered apply of this submission's durable records to their partition-scoped stores. It runs on
    // Complete() — the scheduler's per-partition, Raft-commit-ordered completion path — so every durable
    // transition (record init/decision, prepared-intent prepare/resolve/remove) applies in one place, in log
    // order, with no unordered producer-side apply that could let a losing transition overwrite the winner. It
    // returns whether every PREPARE in the bundle was acknowledged (took ownership of its key); a rejected prepare
    // resolves the producer's Committed task false so the finalizer aborts instead of committing an unrecoverable
    // mutation. Null in tests that only assert scheduling, not store state.
    private readonly Func<IReadOnlyList<RaftProposalEntry>, bool>? applyOnCommit;

    public int PartitionId { get; }

    public int ByteLength { get; }

    public IReadOnlyList<RaftProposalEntry> Entries { get; }

    public long EnqueueTicks { get; set; }

    public DurableProposalSubmission(
        int partitionId,
        IReadOnlyList<RaftProposalEntry> entries,
        TaskCompletionSource<bool> completion,
        Func<IReadOnlyList<RaftProposalEntry>, bool>? applyOnCommit = null)
    {
        PartitionId = partitionId;
        Entries = entries;
        this.completion = completion;
        this.applyOnCommit = applyOnCommit;

        int bytes = 0;
        for (int i = 0; i < entries.Count; i++)
            bytes += entries[i].Data.Length;
        ByteLength = bytes;
    }

    /// <summary>Resolves true once the batch carrying this record committed to Raft and every prepare it carried
    /// took ownership of its key; false when the batch did not commit or a prepare was rejected.</summary>
    public Task<bool> Committed => completion.Task;

    public bool IsStale(IWriteRangeFence fence) => false;

    /// <summary>The batch committed: apply this submission's records to their stores in Raft-commit order, then
    /// resolve the producer with whether every prepare was acknowledged.</summary>
    public void Complete() => completion.TrySetResult(applyOnCommit is null || applyOnCommit(Entries));

    public void Release(bool transient) => completion.TrySetResult(false);
}

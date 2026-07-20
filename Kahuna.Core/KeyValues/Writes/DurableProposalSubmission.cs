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

    public int PartitionId { get; }

    public int ByteLength { get; }

    public IReadOnlyList<RaftProposalEntry> Entries { get; }

    public long EnqueueTicks { get; set; }

    public DurableProposalSubmission(int partitionId, IReadOnlyList<RaftProposalEntry> entries, TaskCompletionSource<bool> completion)
    {
        PartitionId = partitionId;
        Entries = entries;
        this.completion = completion;

        int bytes = 0;
        for (int i = 0; i < entries.Count; i++)
            bytes += entries[i].Data.Length;
        ByteLength = bytes;
    }

    /// <summary>Resolves true once the batch carrying this record committed to Raft.</summary>
    public Task<bool> Committed => completion.Task;

    public bool IsStale(IWriteRangeFence fence) => false;

    public void Complete() => completion.TrySetResult(true);

    public void Release(bool transient) => completion.TrySetResult(false);
}

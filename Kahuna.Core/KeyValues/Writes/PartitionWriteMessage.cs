using Kommander.Data;

namespace Kahuna.Server.KeyValues.Writes;

internal enum PartitionWriteMessageKind
{
    /// <summary>An admitted write to enqueue (ordinary; subject to the lane inbox bound).</summary>
    Submit,

    /// <summary>A scheduled wake deadline elapsed for a partition (priority control).</summary>
    TimerWake,

    /// <summary>A dispatched partition batch settled with its Raft outcome (priority control).</summary>
    BatchComplete,

    /// <summary>Shutdown began: reject/drain pending admissions, let in-flight batches settle (priority control).</summary>
    Stop
}

/// <summary>The settled outcome of one submission in a dispatched batch, mapped from that submission's own
/// contiguous entry slice in the index-aligned per-entry Raft result. A submission whose every entry committed is
/// <see cref="Committed"/>; a submission with any failed entry is released, retryably per <see cref="Transient"/>,
/// carrying the failing entry's <see cref="Status"/> for diagnostics.</summary>
internal readonly record struct BatchSubmissionOutcome(
    IProposalSubmission Item,
    bool Committed,
    bool Transient,
    RaftOperationStatus Status,
    // Committed Raft log index of each of the submission's own entries, in order; null unless it committed.
    IReadOnlyList<long>? EntryLogIndices = null);

/// <summary>
/// The single lane-actor message. Submit is an ordinary admission; TimerWake and BatchComplete are priority
/// control messages so a hot inbox can never strand a linger flush or a completed batch's release.
/// </summary>
internal sealed class PartitionWriteMessage
{
    public PartitionWriteMessageKind Kind { get; }

    public int PartitionId { get; }

    public IProposalSubmission? Item { get; }

    /// <summary>Per-submission settled outcomes for a completed batch, one per submission dispatched.</summary>
    public IReadOnlyList<BatchSubmissionOutcome>? Outcomes { get; }

    /// <summary>For <see cref="PartitionWriteMessageKind.BatchComplete"/>: the <see cref="System.Diagnostics.Stopwatch"/>
    /// timestamp taken when the detached Raft round trip returned, so the lane can attribute the mailbox wait
    /// and the ordered completion turn separately from the Raft call itself.</summary>
    public long RaftReturnedTimestamp { get; }

    private PartitionWriteMessage(
        PartitionWriteMessageKind kind,
        int partitionId,
        IProposalSubmission? item,
        IReadOnlyList<BatchSubmissionOutcome>? outcomes,
        long raftReturnedTimestamp = 0)
    {
        Kind = kind;
        PartitionId = partitionId;
        Item = item;
        Outcomes = outcomes;
        RaftReturnedTimestamp = raftReturnedTimestamp;
    }

    public static PartitionWriteMessage Submit(IProposalSubmission item) =>
        new(PartitionWriteMessageKind.Submit, item.PartitionId, item, null);

    public static PartitionWriteMessage TimerWake(int partitionId) =>
        new(PartitionWriteMessageKind.TimerWake, partitionId, null, null);

    public static PartitionWriteMessage BatchComplete(int partitionId, IReadOnlyList<BatchSubmissionOutcome> outcomes, long raftReturnedTimestamp) =>
        new(PartitionWriteMessageKind.BatchComplete, partitionId, null, outcomes, raftReturnedTimestamp);

    public static readonly PartitionWriteMessage StopSignal =
        new(PartitionWriteMessageKind.Stop, 0, null, null);
}

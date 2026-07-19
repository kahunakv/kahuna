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

/// <summary>
/// The single lane-actor message. Submit is an ordinary admission; TimerWake and BatchComplete are priority
/// control messages so a hot inbox can never strand a linger flush or a completed batch's release.
/// </summary>
internal sealed class PartitionWriteMessage
{
    public PartitionWriteMessageKind Kind { get; }

    public int PartitionId { get; }

    public KeyValueProposalRequest? Item { get; }

    public IReadOnlyList<KeyValueProposalRequest>? Batch { get; }

    public bool Success { get; }

    /// <summary>For a failed batch, whether the caller should retry (MustRetry) rather than see a terminal
    /// error — classified once at settle time by the lane's single outcome mapper.</summary>
    public bool Transient { get; }

    public RaftOperationStatus Status { get; }

    private PartitionWriteMessage(
        PartitionWriteMessageKind kind,
        int partitionId,
        KeyValueProposalRequest? item,
        IReadOnlyList<KeyValueProposalRequest>? batch,
        bool success,
        bool transient,
        RaftOperationStatus status)
    {
        Kind = kind;
        PartitionId = partitionId;
        Item = item;
        Batch = batch;
        Success = success;
        Transient = transient;
        Status = status;
    }

    public static PartitionWriteMessage Submit(KeyValueProposalRequest item) =>
        new(PartitionWriteMessageKind.Submit, item.PartitionId, item, null, false, false, default);

    public static PartitionWriteMessage TimerWake(int partitionId) =>
        new(PartitionWriteMessageKind.TimerWake, partitionId, null, null, false, false, default);

    public static PartitionWriteMessage BatchComplete(int partitionId, IReadOnlyList<KeyValueProposalRequest> batch, bool success, bool transient, RaftOperationStatus status) =>
        new(PartitionWriteMessageKind.BatchComplete, partitionId, null, batch, success, transient, status);

    public static readonly PartitionWriteMessage StopSignal =
        new(PartitionWriteMessageKind.Stop, 0, null, null, false, false, default);
}


using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The outcome of an off-mailbox two-phase-commit Raft call, carried on a <c>CompletePhaseTwo</c>
/// <see cref="KeyValueRequest"/> from <see cref="KeyValuePhaseTwoActor"/> back to the originating
/// participant actor. Kept intentionally thin: the heavy per-op context needed to apply the result
/// lives with the originating actor, keyed by <see cref="PhaseTwoId"/>.
/// </summary>
internal sealed class PhaseTwoCompletionData
{
    /// <summary>Correlation id matching the originating <see cref="KeyValuePhaseTwoRequest"/>.</summary>
    public int PhaseTwoId { get; }

    /// <summary>Which Raft round trip produced this outcome.</summary>
    public PhaseTwoOpKind OpKind { get; }

    /// <summary>True when the Raft call succeeded.</summary>
    public bool Success { get; }

    /// <summary>The Raft status, used to classify a failure as transient (retry) or terminal.</summary>
    public RaftOperationStatus Status { get; }

    /// <summary>The commit/rollback log index. Meaningful only for commit/rollback success.</summary>
    public long CommitIndex { get; }

    /// <summary>The proposal ticket produced by a successful prepare. Zero for commit/rollback.</summary>
    public HLCTimestamp TicketId { get; }

    public PhaseTwoCompletionData(
        int phaseTwoId,
        PhaseTwoOpKind opKind,
        bool success,
        RaftOperationStatus status,
        long commitIndex,
        HLCTimestamp ticketId
    )
    {
        PhaseTwoId = phaseTwoId;
        OpKind = opKind;
        Success = success;
        Status = status;
        CommitIndex = commitIndex;
        TicketId = ticketId;
    }
}

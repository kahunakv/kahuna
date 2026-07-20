
using Kommander.Time;
using Kahuna.Shared.KeyValue;
using Kahuna.Server.KeyValues.Transactions.Data;

namespace Kahuna.Server.KeyValues;

/// <summary>
/// The per-participant context a phase-two dispatch parks on the owning actor while its Raft round
/// trip runs off the mailbox. Keyed by a monotonic <c>phaseTwoId</c> in
/// <see cref="KeyValueContext.PendingPhaseTwos"/>, it carries everything the after-Raft apply needs so
/// the completion handler can finish the commit/rollback without re-deriving it. Removed by the first
/// completion for its id — the primary duplicate-completion guard.
/// </summary>
internal sealed class PendingPhaseTwo
{
    /// <summary>Which Raft round trip this entry is waiting on.</summary>
    public PhaseTwoOpKind OpKind { get; }

    /// <summary>The transaction whose participant mutation this commits or rolls back.</summary>
    public HLCTimestamp TxId { get; }

    /// <summary>The participant key.</summary>
    public string Key { get; }

    /// <summary>The participant's durability tier.</summary>
    public KeyValueDurability Durability { get; }

    /// <summary>The committed mutation to apply. Null for a prepare (no state apply on completion).</summary>
    public KeyValueProposal? Proposal { get; }

    /// <summary>The HLC captured when the participant handler dispatched, used for MVCC trimming.</summary>
    public HLCTimestamp CurrentTime { get; }

    /// <summary>The proposal ticket the commit/rollback targets.</summary>
    public HLCTimestamp TicketId { get; }

    /// <summary>The Raft partition the mutation belongs to (for the background-write enqueue).</summary>
    public int PartitionId { get; }

    /// <summary>Transaction record anchor, carried into the completion receipt on a persistent commit.</summary>
    public string? RecordAnchorKey { get; }

    /// <summary>The caller's ask promise, retained so a deadline sweep can resolve it (retryable) if the
    /// off-mailbox worker dies or its completion message is dropped and never arrives. Set by the
    /// dispatching handler; null in bare-context tests that drive the completion directly.</summary>
    public TaskCompletionSource<KeyValueResponse?>? Promise { get; set; }

    /// <summary>Absolute dispatch deadline (<see cref="Environment.TickCount64"/> ms) mirrored from the
    /// worker request, so the sweep can tell an orphaned entry from one whose completion is still due.
    /// <see cref="KeyValuePhaseTwoRequest.NoDeadline"/> disables the sweep for this entry.</summary>
    public long DeadlineTicks { get; set; } = KeyValuePhaseTwoRequest.NoDeadline;

    private PendingPhaseTwo(
        PhaseTwoOpKind opKind,
        HLCTimestamp txId,
        string key,
        KeyValueDurability durability,
        KeyValueProposal? proposal,
        HLCTimestamp currentTime,
        HLCTimestamp ticketId,
        int partitionId,
        string? recordAnchorKey
    )
    {
        OpKind = opKind;
        TxId = txId;
        Key = key;
        Durability = durability;
        Proposal = proposal;
        CurrentTime = currentTime;
        TicketId = ticketId;
        PartitionId = partitionId;
        RecordAnchorKey = recordAnchorKey;
    }

    public static PendingPhaseTwo ForPrepare(HLCTimestamp txId, string key, KeyValueDurability durability)
        => new(PhaseTwoOpKind.Prepare, txId, key, durability, null, HLCTimestamp.Zero, HLCTimestamp.Zero, 0, null);

    public static PendingPhaseTwo ForCommit(
        HLCTimestamp txId,
        string key,
        KeyValueDurability durability,
        KeyValueProposal proposal,
        HLCTimestamp currentTime,
        HLCTimestamp ticketId,
        int partitionId,
        string? recordAnchorKey
    ) => new(PhaseTwoOpKind.Commit, txId, key, durability, proposal, currentTime, ticketId, partitionId, recordAnchorKey);

    public static PendingPhaseTwo ForRollback(
        HLCTimestamp txId,
        string key,
        KeyValueDurability durability,
        HLCTimestamp currentTime,
        HLCTimestamp ticketId,
        int partitionId
    ) => new(PhaseTwoOpKind.Rollback, txId, key, durability, null, currentTime, ticketId, partitionId, null);
}

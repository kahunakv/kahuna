using Kahuna.Server.KeyValues.Logging;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kommander.Data;
using Microsoft.Extensions.Logging;

namespace Kahuna.Server.KeyValues.Writes;

/// <summary>What the write scheduler's completion learned about a submission whose batch committed.</summary>
internal enum DurableCompletionAnswer
{
    /// <summary>The batch did not commit (or the submission was released before dispatch): nothing durable, a clean
    /// retry.</summary>
    NotCommitted,

    /// <summary>The ordered apply ran every entry and every PREPARE took ownership of its key.</summary>
    Acknowledged,

    /// <summary>The ordered apply ran every entry and refused at least one PREPARE (a foreign holder, a moved base).
    /// The batch is durable, so the producer drives the truthful outcome against the record.</summary>
    Refused,

    /// <summary>This node did not observe the ordered apply of the entries: it stopped leading the partition while
    /// the completion waited, or the wait bound elapsed. The batch is quorum-durable and the current leader applies
    /// and judges it, but nothing here can say how. Answered to the producer exactly as <see cref="NotCommitted"/>:
    /// a re-drive of the same entries is idempotent in the log and lands on the leader that can answer.</summary>
    Unobserved
}

/// <summary>
/// The write scheduler's completion side of a committed durable submission. It applies nothing: the durable
/// record and intent stores are mutated only by the ordered consumer apply that Raft drives in log order (see
/// <see cref="DurableApplyResultLedger"/>). For each record/intent entry of the submission it waits for that
/// apply and folds the results into the producer's answer — whether every PREPARE took ownership of its key.
///
/// <para>Why it must not apply: the completion runs when the proposal is quorum-durable, which on the proposing
/// node precedes the consumer apply of the entry and of the entries below it, and can also trail it by an
/// arbitrary delay. An apply run here would judge the delta against an incomplete or already-advanced prefix
/// (a prepare refused because a competitor's intent was not yet removed; a settled prepare re-installed as a
/// zombie after a late completion), and the ordered apply would then skip or contradict it — a fork of this
/// node's state from its peers', on the one node that proposed the entry.</para>
///
/// <para>Why it must not wait forever, or on a node that stopped leading: a leader whose device stalls is stepped
/// down within seconds, and its ordered apply then cannot advance until the device heals, while the entries it
/// proposed are quorum-durable and judged by the new leader. Waiting there only converts the stall into an unknown
/// outcome at the client. The ledger releases parked waits on leadership loss, and the whole submission shares
/// one wait bound; either way the answer is <see cref="DurableCompletionAnswer.Unobserved"/>, which the producer
/// treats as "not committed, retry" — safe, because the re-driven entries are idempotent in the log.</para>
/// </summary>
internal sealed class DurableOrderedApplyAwaiter
{
    /// <summary>How long a completion waits, in total, for the ordered apply of a committed submission's entries
    /// before answering its producer without it. Matches the Raft proposal timeout: an entry the consumer has not
    /// applied this long after its commit is not going to be applied on this node soon (its device is stalled, or a
    /// snapshot install covered the entry). Leadership loss releases the wait earlier.</summary>
    internal static readonly TimeSpan DefaultWaitTimeout = TimeSpan.FromSeconds(10);

    private readonly DurableApplyResultLedger ledger;

    private readonly PreparedIntentStore preparedIntentStore;

    private readonly ILogger<IKahuna>? logger;

    private readonly TimeSpan waitTimeout;

    internal DurableOrderedApplyAwaiter(DurableApplyResultLedger ledger, PreparedIntentStore preparedIntentStore, ILogger<IKahuna>? logger, TimeSpan? waitTimeout = null)
    {
        this.ledger = ledger;
        this.preparedIntentStore = preparedIntentStore;
        this.logger = logger;
        this.waitTimeout = waitTimeout ?? DefaultWaitTimeout;
    }

    /// <summary>
    /// Waits for the ordered apply of every record/intent entry of a committed submission and answers how it went.
    /// <paramref name="entryLogIndices"/> are the committed log indices of <paramref name="entries"/>, in order, as
    /// the executor reported them.
    ///
    /// <para>An entry the consumer applied but whose recorded acknowledgement was displaced is answered from the
    /// store: its prepares are acknowledged when their intents are held under their own identity. An entry the
    /// consumer does not apply within the submission's shared bound, one released by a leadership loss, or one the
    /// executor reported without a log index, ends the wait with <see cref="DurableCompletionAnswer.Unobserved"/>;
    /// applying here would be the out-of-order apply this type exists to prevent.</para>
    /// </summary>
    internal async Task<DurableCompletionAnswer> AwaitAppliedAsync(int partitionId, IReadOnlyList<RaftProposalEntry> entries, IReadOnlyList<long>? entryLogIndices, CancellationToken cancellationToken)
    {
        DurableCompletionAnswer answer = DurableCompletionAnswer.Acknowledged;
        long deadline = Environment.TickCount64 + (long)waitTimeout.TotalMilliseconds;

        for (int i = 0; i < entries.Count; i++)
        {
            RaftProposalEntry entry = entries[i];

            if (entry.Type != ReplicationTypes.TransactionRecord && entry.Type != ReplicationTypes.PreparedIntent)
                continue;

            long logIndex = entryLogIndices is not null && i < entryLogIndices.Count ? entryLogIndices[i] : 0;

            // One bound for the whole submission: its entries apply in order, so the remaining budget is what the
            // next entry may take, never a fresh bound per entry.
            TimeSpan remaining = TimeSpan.FromMilliseconds(Math.Max(0, deadline - Environment.TickCount64));

            DurableApplyWaitOutcome outcome = await ledger.WaitAppliedAsync(partitionId, logIndex, remaining, cancellationToken).ConfigureAwait(false);

            switch (outcome.Status)
            {
                case DurableApplyWaitStatus.Recorded:
                    if (entry.Type == ReplicationTypes.PreparedIntent && !outcome.Result)
                        answer = DurableCompletionAnswer.Refused;
                    break;

                case DurableApplyWaitStatus.AppliedResultDisplaced:
                    DurableTransactionMetrics.OrderedApplyResultDisplaced();
                    if (entry.Type == ReplicationTypes.PreparedIntent && !PreparesHeldUnderOwnIdentity(entry.Data))
                        answer = DurableCompletionAnswer.Refused;
                    break;

                case DurableApplyWaitStatus.LeadershipLost:
                    DurableTransactionMetrics.OrderedApplyWaitReleasedOnLeadershipLoss();
                    return DurableCompletionAnswer.Unobserved;

                default:
                    DurableTransactionMetrics.OrderedApplyWaitTimedOut();
                    logger?.LogDurableCompletionWithoutOrderedApply(logIndex, partitionId, entry.Type, (long)waitTimeout.TotalMilliseconds);
                    return DurableCompletionAnswer.Unobserved;
            }
        }

        return answer;
    }

    /// <summary>The acknowledgement read back from the store for a prepare delta whose recorded result was displaced:
    /// every prepare it carries holds its key under its own (transaction, epoch). A prepare the ordered apply refused
    /// left a foreign holder (or nothing) at the key and reads as unacknowledged, exactly as its recorded result
    /// would have.</summary>
    private bool PreparesHeldUnderOwnIdentity(byte[] prepareDelta)
    {
        foreach (PreparedIntentCommand command in PreparedIntentStore.DecodeDelta(prepareDelta))
        {
            if (command is PrepareIntentCommand prepare
                && preparedIntentStore.GetByIdentity(prepare.Intent.TransactionId, prepare.Intent.Epoch, prepare.Intent.Key) is null)
                return false;
        }

        return true;
    }
}

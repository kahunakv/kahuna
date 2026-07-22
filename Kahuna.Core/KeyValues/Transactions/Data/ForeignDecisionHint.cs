using Kommander.Time;

namespace Kahuna.Server.KeyValues.Transactions.Data;

/// <summary>
/// A foreign prepared intent's canonical decision, resolved off the actor mailbox by routing a lookup to the
/// transaction's anchor-partition leader, and threaded back into a re-issued read so the actor's read-visibility
/// check can resolve a still-pending remote-anchor intent instead of retrying until settlement.
/// <para>The default value (<see cref="TransactionId"/> = <see cref="HLCTimestamp.Zero"/>) means "no hint". A
/// hint applies only when it names the exact <c>(TransactionId, Epoch)</c> of the intent the read meets and
/// carries a terminal decision, so a stale hint can never be misapplied to a different intent.</para>
/// </summary>
internal readonly record struct ForeignDecisionHint(HLCTimestamp TransactionId, long Epoch, TransactionDecision Decision)
{
    /// <summary>True when this hint carries a terminal decision for exactly the given intent identity.</summary>
    public bool Applies(HLCTimestamp transactionId, long epoch) =>
        TransactionId != HLCTimestamp.Zero
        && TransactionId == transactionId
        && Epoch == epoch
        && Decision is TransactionDecision.Commit or TransactionDecision.Abort;
}

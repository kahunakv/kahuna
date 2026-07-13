using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The rollback path may report <see cref="KeyValueResponseType.RolledBack"/> only when every mandatory
/// lock/MVCC release is positively proven gone. <see cref="TransactionCoordinator.IsReleaseAcked"/> is that
/// gate: it must accept only responses that prove no effect of the transaction remains, and reject transient
/// conditions and non-proof errors so the rollback retries instead of falsely claiming success. Previously it
/// accepted "anything but retry", counting a generic <c>Errored</c>/<c>InvalidInput</c> (or a batch result
/// remapped from a missing/replicating response) as acknowledgement while an intent could still remain.
/// </summary>
public sealed class TestReleaseAcknowledgement
{
    [Theory]
    // Proof the effect is gone — the only responses that acknowledge a mandatory release.
    [InlineData(KeyValueResponseType.Unlocked, true)]      // our MVCC removed + our intent cleared
    [InlineData(KeyValueResponseType.DoesNotExist, true)]  // entry absent → nothing of ours can remain
    [InlineData(KeyValueResponseType.AlreadyLocked, true)] // present intent belongs to another tx → ours is gone
    // Transient — the release did not settle, so retry.
    [InlineData(KeyValueResponseType.MustRetry, false)]
    [InlineData(KeyValueResponseType.WaitingForReplication, false)]
    // Non-proof errors — the release did not run; must NOT be treated as acknowledgement.
    [InlineData(KeyValueResponseType.Errored, false)]
    [InlineData(KeyValueResponseType.InvalidInput, false)]
    // Unrelated outcomes are never proof either.
    [InlineData(KeyValueResponseType.Set, false)]
    [InlineData(KeyValueResponseType.Committed, false)]
    [InlineData(KeyValueResponseType.Aborted, false)]
    [InlineData(KeyValueResponseType.Locked, false)]
    public void IsReleaseAcked_AcceptsOnlyPositiveProofOfRelease(KeyValueResponseType type, bool expected)
    {
        Assert.Equal(expected, TransactionCoordinator.IsReleaseAcked(type));
    }
}

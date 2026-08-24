using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Store-level coverage of the prepare-apply staged-base fence: the advisory verdict the intent store attaches
/// to a freshly installed validated-base prepare when the key's last transactionally committed head no longer
/// matches the base the write was validated against. The fence never changes the replicated transition — the
/// intent installs either way — so every test asserts both the flag and the installed intent.
/// </summary>
public sealed class TestPreparedIntentStagedBaseFence
{
    private static PreparedIntent MakeIntent(
        string key,
        long txPhysical,
        long revision,
        long baseRevision,
        KeyValueState baseState,
        KeyValueState state = KeyValueState.Set) => new(
        TransactionId: new HLCTimestamp(0, txPhysical, 0), Epoch: 1, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(0, txPhysical + 1, 0),
        State: state, Value: state == KeyValueState.Set ? [1, 2, 3] : null, Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: baseRevision, BaseState: baseState,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    /// <summary>Runs one competitor's commit lifecycle through the store so its head is remembered.</summary>
    private static void CommitThroughStore(PreparedIntentStore store, PreparedIntent intent)
    {
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(intent)).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true)).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied,
            store.Apply(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key)).Outcome);
    }

    [Fact]
    public void FreshValidatedPrepare_NoHeadRemembered_IsAcknowledged()
    {
        PreparedIntentStore store = new();

        PreparedIntentApplyResult result = store.Apply(new PrepareIntentCommand(
            MakeIntent("k/fresh", txPhysical: 1_000, revision: 6, baseRevision: 5, KeyValueState.Set)));

        Assert.Equal(TransactionApplyOutcome.Applied, result.Outcome);
        Assert.False(result.StaleBase);
        Assert.NotNull(store.Get("k/fresh"));
    }

    [Fact]
    public void PrepareAgainstOvertakenBase_IsFlaggedStale_ButStillInstalls()
    {
        PreparedIntentStore store = new();

        // A competitor read base 5, committed revision 6, and fully settled — the exact state the lost-update
        // window leaves behind: no live intent, only the committed head.
        CommitThroughStore(store, MakeIntent("k/hot", txPhysical: 1_000, revision: 6, baseRevision: 5, KeyValueState.Set));

        PreparedIntentApplyResult result = store.Apply(new PrepareIntentCommand(
            MakeIntent("k/hot", txPhysical: 1_100, revision: 6, baseRevision: 5, KeyValueState.Set)));

        Assert.Equal(TransactionApplyOutcome.Applied, result.Outcome);
        Assert.True(result.StaleBase, "a base the head moved past must refuse the acknowledgement");
        Assert.NotNull(store.Get("k/hot"));
    }

    [Fact]
    public void PrepareAtTheRememberedHead_IsAcknowledged()
    {
        PreparedIntentStore store = new();

        CommitThroughStore(store, MakeIntent("k/at-head", txPhysical: 1_000, revision: 6, baseRevision: 5, KeyValueState.Set));

        // This transaction read the committed value the head describes — revision 6 — so its base is current.
        PreparedIntentApplyResult result = store.Apply(new PrepareIntentCommand(
            MakeIntent("k/at-head", txPhysical: 1_100, revision: 7, baseRevision: 6, KeyValueState.Set)));

        Assert.Equal(TransactionApplyOutcome.Applied, result.Outcome);
        Assert.False(result.StaleBase);
    }

    [Fact]
    public void ValidatedAbsentBase_FlaggedOnlyWhileTheHeadIsAValue()
    {
        PreparedIntentStore store = new();

        // A committed transactional insert makes the key exist; a later validated-absent insert is stale.
        CommitThroughStore(store, MakeIntent("k/appeared", txPhysical: 1_000, revision: 0,
            baseRevision: PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined));

        PreparedIntentApplyResult conflicting = store.Apply(new PrepareIntentCommand(
            MakeIntent("k/appeared", txPhysical: 1_100, revision: 0, baseRevision: -1, KeyValueState.Undefined)));

        Assert.True(conflicting.StaleBase, "a validated-absent base must conflict once the key exists");

        // A committed transactional DELETE keeps the key absent, so a validated-absent insert stays clean.
        CommitThroughStore(store, MakeIntent("k/deleted", txPhysical: 1_000, revision: 4,
            baseRevision: 3, KeyValueState.Set, state: KeyValueState.Deleted));

        PreparedIntentApplyResult clean = store.Apply(new PrepareIntentCommand(
            MakeIntent("k/deleted", txPhysical: 1_100, revision: 0, baseRevision: -1, KeyValueState.Undefined)));

        Assert.Equal(TransactionApplyOutcome.Applied, clean.Outcome);
        Assert.False(clean.StaleBase);
    }

    [Fact]
    public void BlindWrite_NeverFlagged_EvenWhenTheHeadMoved()
    {
        PreparedIntentStore store = new();

        CommitThroughStore(store, MakeIntent("k/blind", txPhysical: 1_000, revision: 6, baseRevision: 5, KeyValueState.Set));

        // No validated base (a write with no prior read): last-writer-wins by design, the fence must not judge it.
        PreparedIntentApplyResult result = store.Apply(new PrepareIntentCommand(
            MakeIntent("k/blind", txPhysical: 1_100, revision: 7,
                baseRevision: PreparedIntent.UnknownBaseRevision, KeyValueState.Undefined)));

        Assert.Equal(TransactionApplyOutcome.Applied, result.Outcome);
        Assert.False(result.StaleBase);
    }

    [Fact]
    public void TransactionOlderThanTheRetentionHorizon_IsRefused()
    {
        PreparedIntentStore store = new();
        store.ConfigureStagedBaseFence(retentionMs: 1_000);

        // Advance the head watermark far past the horizon via an unrelated key's commit.
        CommitThroughStore(store, MakeIntent("k/other", txPhysical: 100_000, revision: 1, baseRevision: 0, KeyValueState.Set));

        // This transaction began long before the horizon: any head its base depended on may already be pruned,
        // so absence of evidence must refuse the acknowledgement rather than admit a possibly-stale base.
        PreparedIntentApplyResult result = store.Apply(new PrepareIntentCommand(
            MakeIntent("k/ancient", txPhysical: 10, revision: 3, baseRevision: 2, KeyValueState.Set)));

        Assert.Equal(TransactionApplyOutcome.Applied, result.Outcome);
        Assert.True(result.StaleBase, "a transaction older than the retention horizon cannot have its base verified");
    }

    [Fact]
    public void HeadBehindTheValidatedBase_IsAcknowledged()
    {
        PreparedIntentStore store = new();

        CommitThroughStore(store, MakeIntent("k/lagging", txPhysical: 1_000, revision: 6, baseRevision: 5, KeyValueState.Set));

        // Non-transactional writes advanced the key past the remembered head; the transaction read revision 9.
        // The memory can attest nothing newer than 6, so a base ahead of the head must stay acknowledged.
        PreparedIntentApplyResult result = store.Apply(new PrepareIntentCommand(
            MakeIntent("k/lagging", txPhysical: 1_100, revision: 10, baseRevision: 9, KeyValueState.Set)));

        Assert.Equal(TransactionApplyOutcome.Applied, result.Outcome);
        Assert.False(result.StaleBase);
    }

    [Fact]
    public void SameIdentityReprepare_StaysIdempotent_EvenAfterTheHeadMoved()
    {
        PreparedIntentStore store = new();

        PreparedIntent original = MakeIntent("k/replay", txPhysical: 1_000, revision: 6, baseRevision: 5, KeyValueState.Set);
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(original)).Outcome);

        // A replay of the exact same prepare while the intent is live must remain a clean no-op: the
        // transaction already owns the key, so the fence has nothing to judge.
        PreparedIntentApplyResult replay = store.Apply(new PrepareIntentCommand(original));

        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, replay.Outcome);
        Assert.False(replay.StaleBase);
    }
}

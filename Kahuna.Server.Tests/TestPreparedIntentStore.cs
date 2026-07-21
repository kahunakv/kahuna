using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for the durable prepared-intent model: the pure <see cref="PreparedIntentStateMachine"/> transition
/// rules and the per-partition <see cref="PreparedIntentStore"/> that enforces one live intent per key, idempotent
/// prepare/resolve/remove replay, terminal-resolution immutability, and the recovery lookups.
/// </summary>
public sealed class TestPreparedIntentStore
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent Intent(
        long txn, long epoch, string key, KeyValueState state = KeyValueState.Set,
        byte[]? value = null, long revision = 1, HLCTimestamp? recoveryDeadline = null,
        PreparedIntentResolution resolution = PreparedIntentResolution.Pending, string? bucket = null) =>
        new(
            TransactionId: Ts(txn),
            Epoch: epoch,
            Key: key,
            ManifestHash: 12345,
            RecordAnchorKey: "anchor",
            CommitTimestamp: Ts(txn + 100),
            State: state,
            Value: value ?? [1, 2, 3],
            Bucket: bucket,
            Revision: revision,
            Expires: HLCTimestamp.Zero,
            NoRevision: false,
            BaseRevision: revision - 1,
            BaseState: KeyValueState.Set,
            RecoveryDeadline: recoveryDeadline ?? Ts(txn + 5000),
            Resolution: resolution);

    // ── prepare ────────────────────────────────────────────────────────────────────

    [Fact]
    public void Prepare_OnEmptyKey_Installs()
    {
        PreparedIntentStore store = new();
        PreparedIntentApplyResult r = store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k")));

        Assert.Equal(TransactionApplyOutcome.Applied, r.Outcome);
        Assert.NotNull(store.Get("k"));
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void Prepare_ExactDuplicate_IsNoop()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k", value: [9, 9])));
        PreparedIntentApplyResult r = store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k", value: [9, 9])));

        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, r.Outcome);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void Prepare_SameIdentity_DifferentMutation_Rejected()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k", value: [1])));
        PreparedIntentApplyResult r = store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k", value: [2])));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.NotNull(r.RejectReason);
    }

    [Fact]
    public void Prepare_DifferentTransaction_OnLiveKey_Conflicts_NotOverwritten()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k", value: [1])));
        PreparedIntentApplyResult r = store.Apply(new PrepareIntentCommand(Intent(2000, 1, "k", value: [2])));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        // The original holder is intact.
        Assert.Equal(Ts(1000), store.Get("k")!.TransactionId);
    }

    [Fact]
    public void Prepare_DifferentTransaction_OnResolvedButNotRemovedKey_StillConflicts()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k")));
        store.Apply(new ResolveIntentCommand(Ts(1000), 1, "k", Commit: true));

        PreparedIntentApplyResult r = store.Apply(new PrepareIntentCommand(Intent(2000, 1, "k")));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Equal(Ts(1000), store.Get("k")!.TransactionId);
    }

    // ── resolve ────────────────────────────────────────────────────────────────────

    [Fact]
    public void Resolve_Pending_ToCommitted()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k")));
        PreparedIntentApplyResult r = store.Apply(new ResolveIntentCommand(Ts(1000), 1, "k", Commit: true));

        Assert.Equal(TransactionApplyOutcome.Applied, r.Outcome);
        Assert.Equal(PreparedIntentResolution.Committed, store.Get("k")!.Resolution);
    }

    [Fact]
    public void Resolve_Idempotent_SameOutcome_IsNoop()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k")));
        store.Apply(new ResolveIntentCommand(Ts(1000), 1, "k", Commit: false));
        PreparedIntentApplyResult r = store.Apply(new ResolveIntentCommand(Ts(1000), 1, "k", Commit: false));

        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, r.Outcome);
        Assert.Equal(PreparedIntentResolution.Aborted, store.Get("k")!.Resolution);
    }

    [Fact]
    public void Resolve_ConflictingOutcome_AfterTerminal_Rejected()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k")));
        store.Apply(new ResolveIntentCommand(Ts(1000), 1, "k", Commit: true));
        PreparedIntentApplyResult r = store.Apply(new ResolveIntentCommand(Ts(1000), 1, "k", Commit: false));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Equal(PreparedIntentResolution.Committed, store.Get("k")!.Resolution);
    }

    [Fact]
    public void Resolve_AbsentKey_IsNoop()
    {
        PreparedIntentStore store = new();
        PreparedIntentApplyResult r = store.Apply(new ResolveIntentCommand(Ts(1000), 1, "gone", Commit: true));

        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, r.Outcome);
    }

    [Fact]
    public void Resolve_IdentityMismatch_Rejected()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k")));
        PreparedIntentApplyResult r = store.Apply(new ResolveIntentCommand(Ts(2000), 1, "k", Commit: true));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Equal(PreparedIntentResolution.Pending, store.Get("k")!.Resolution);
    }

    // ── scan windows ─────────────────────────────────────────────────────────────

    [Fact]
    public void SnapshotScanWindow_HonorsStartExclusivityAndEndInclusivity()
    {
        PreparedIntentStore store = new();
        foreach (string k in new[] { "a", "b", "c", "d" })
            store.Apply(new PrepareIntentCommand(Intent(1000, 1, k)));

        // Half-open [b, d): b included, d excluded.
        List<string> halfOpen = store.SnapshotScanWindow("b", startInclusive: true, "d", endInclusive: false)
            .Select(i => i.Key).OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.Equal(["b", "c"], halfOpen);

        // Start-exclusive (a continuation cursor at "b") + end-inclusive "d": b skipped, d kept.
        List<string> exclStart = store.SnapshotScanWindow("b", startInclusive: false, "d", endInclusive: true)
            .Select(i => i.Key).OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.Equal(["c", "d"], exclStart);
    }

    [Fact]
    public void SnapshotBucket_FiltersByIntentBucket()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "accounts/alice", bucket: "accounts")));
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "accounts/bob", bucket: "accounts")));
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "orders/1", bucket: "orders")));

        List<string> accounts = store.SnapshotBucket("accounts").Select(i => i.Key).OrderBy(k => k, StringComparer.Ordinal).ToList();
        Assert.Equal(["accounts/alice", "accounts/bob"], accounts);

        Assert.Single(store.SnapshotBucket("orders"));
        Assert.Empty(store.SnapshotBucket("missing"));
    }

    // ── remove ─────────────────────────────────────────────────────────────────────

    [Fact]
    public void Remove_Pending_Rejected()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k")));
        PreparedIntentApplyResult r = store.Apply(new RemoveIntentCommand(Ts(1000), 1, "k"));

        Assert.Equal(TransactionApplyOutcome.Rejected, r.Outcome);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void Remove_Resolved_Deletes()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k")));
        store.Apply(new ResolveIntentCommand(Ts(1000), 1, "k", Commit: true));
        PreparedIntentApplyResult r = store.Apply(new RemoveIntentCommand(Ts(1000), 1, "k"));

        Assert.Equal(TransactionApplyOutcome.Applied, r.Outcome);
        Assert.Null(r.Intent);
        Assert.Null(store.Get("k"));
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public void Remove_AbsentKey_IsNoop()
    {
        PreparedIntentStore store = new();
        PreparedIntentApplyResult r = store.Apply(new RemoveIntentCommand(Ts(1000), 1, "gone"));

        Assert.Equal(TransactionApplyOutcome.IdempotentNoop, r.Outcome);
    }

    // ── lookups / recovery ──────────────────────────────────────────────────────────

    [Fact]
    public void GetByIdentity_MatchesOnlyTheOwningTransaction()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "k")));

        Assert.NotNull(store.GetByIdentity(Ts(1000), 1, "k"));
        Assert.Null(store.GetByIdentity(Ts(2000), 1, "k"));
        Assert.Null(store.GetByIdentity(Ts(1000), 2, "k"));
    }

    [Fact]
    public void DueForRecovery_ReturnsOnlyPendingPastDeadline()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "due", recoveryDeadline: Ts(5000))));
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "future", recoveryDeadline: Ts(50000))));
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "resolved", recoveryDeadline: Ts(5000))));
        store.Apply(new ResolveIntentCommand(Ts(1000), 1, "resolved", Commit: true));

        IReadOnlyList<PreparedIntent> due = store.DueForRecovery(Ts(10000));

        Assert.Single(due);
        Assert.Equal("due", due[0].Key);
    }

    [Fact]
    public void Snapshot_ReturnsAllHeldIntents()
    {
        PreparedIntentStore store = new();
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "a")));
        store.Apply(new PrepareIntentCommand(Intent(1000, 1, "b")));

        Assert.Equal(2, store.Snapshot().Count);
    }
}


using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance tests for consulting the durable coordinator decision when the in-memory session is gone (Phase
/// 6.5): a repeated commit or rollback routed by the handle's record anchor reads that partition's decision
/// record — <c>Completed</c> → <c>Committed</c>, <c>CommitDecided</c> → <c>MustRetry</c> — so a retry works even
/// after the coordinating session was evicted or its node failed; a stale unanchored handle or an expired record
/// stays unknown <c>Errored</c>; and a rollback cannot override a durably decided commit.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableOutcomeConsultation
{
    private static EmbeddedKahunaOptions EmbeddedOptions() => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1,
    };

    private static CoordinatorParticipant Participant(string key, bool acked) =>
        new(key, KeyValueDurability.Persistent, HLCTimestamp.Zero, acked, acked);

    private static async Task Seed(EmbeddedKahunaNode node, HLCTimestamp txId, string anchor, CoordinatorDecisionStatus status)
    {
        HLCTimestamp completedAt = status == CoordinatorDecisionStatus.Completed ? txId : HLCTimestamp.Zero;
        bool acked = status == CoordinatorDecisionStatus.Completed;
        CoordinatorDecisionRecord record = new(
            txId, "lost-coord", anchor, txId, status,
            [Participant(anchor, acked)], [], txId, completedAt);
        await node.Kahuna.ImportCoordinatorDecisions([record]);
    }

    // ── A lost session with a Completed record answers Committed on retry ──────────────────────────────────

    [Fact]
    public async Task MissingSession_CompletedRecord_CommitReturnsCommitted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "consult-done:aaa";
        await Seed(node, txId, anchor, CoordinatorDecisionStatus.Completed);

        // A handle for a session that does not exist in memory, carrying its record anchor.
        TransactionHandle handle = new(txId, "lost-coord", anchor);
        (KeyValueResponseType type, string? returnedAnchor) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        Assert.Equal(KeyValueResponseType.Committed, type);
        Assert.Equal(anchor, returnedAnchor);
    }

    // ── A lost session with a still-CommitDecided record answers MustRetry on retry ────────────────────────

    [Fact]
    public async Task MissingSession_CommitDecidedRecord_CommitReturnsMustRetry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "consult-pending:aaa";
        await Seed(node, txId, anchor, CoordinatorDecisionStatus.CommitDecided);

        TransactionHandle handle = new(txId, "lost-coord", anchor);
        (KeyValueResponseType type, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
    }

    // ── A lost session with no record (never created / retention expired) is unknown Errored ───────────────

    [Fact]
    public async Task MissingSession_NoRecord_CommitReturnsErrored()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        TransactionHandle handle = new(txId, "lost-coord", "consult-absent:aaa");

        (KeyValueResponseType type, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        Assert.Equal(KeyValueResponseType.Errored, type);
    }

    // ── A stale unanchored handle cannot locate a lost session, even when a record exists ──────────────────

    [Fact]
    public async Task MissingSession_UnanchoredHandle_CommitReturnsErrored()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "consult-unanchored:aaa";
        await Seed(node, txId, anchor, CoordinatorDecisionStatus.Completed);

        // The retry lost the anchor: it cannot scan the cluster by transaction id, so the outcome is unknown.
        TransactionHandle handle = new(txId, "lost-coord");
        (KeyValueResponseType type, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        Assert.Equal(KeyValueResponseType.Errored, type);
    }

    // ── A rollback cannot override a durably decided commit ────────────────────────────────────────────────

    [Fact]
    public async Task MissingSession_Rollback_CannotOverrideDecidedCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        // Completed decision → rollback of the lost session must report Committed, not RolledBack/Errored.
        HLCTimestamp doneTx = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string doneAnchor = "consult-rb-done:aaa";
        await Seed(node, doneTx, doneAnchor, CoordinatorDecisionStatus.Completed);
        KeyValueResponseType doneRollback = await node.Kahuna.LocateAndRollbackTransaction(new(doneTx, "lost-coord", doneAnchor), ct);
        Assert.Equal(KeyValueResponseType.Committed, doneRollback);

        // CommitDecided decision → rollback must report MustRetry (recovery finishes the commit), not roll back.
        HLCTimestamp pendingTx = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string pendingAnchor = "consult-rb-pending:aaa";
        await Seed(node, pendingTx, pendingAnchor, CoordinatorDecisionStatus.CommitDecided);
        KeyValueResponseType pendingRollback = await node.Kahuna.LocateAndRollbackTransaction(new(pendingTx, "lost-coord", pendingAnchor), ct);
        Assert.Equal(KeyValueResponseType.MustRetry, pendingRollback);
    }
}

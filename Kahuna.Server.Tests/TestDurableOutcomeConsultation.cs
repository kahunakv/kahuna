
using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Acceptance tests for consulting the durable-intent canonical transaction record when the in-memory session is
/// gone: a repeated commit or rollback reads the resident record for the handle's transaction id — a committed
/// record answers <c>Committed</c>, an undecided record answers <c>MustRetry</c> (recovery finishes it), a
/// conflict-aborted record answers <c>Aborted</c> — so a retry works even after the coordinating session was
/// evicted or its node failed; a transaction with no resident record stays unknown <c>Errored</c>; and a rollback
/// cannot override a durably committed transaction.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestDurableOutcomeConsultation
{
    private const long Epoch = 1;

    private static EmbeddedKahunaOptions EmbeddedOptions() => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1,
    };

    private static TransactionRecordStore Store(EmbeddedKahunaNode node) =>
        ((KahunaManager)node.Kahuna).DurableTransactionRecordStore;

    // Seeds the resident canonical record for a transaction with the given terminal decision (or leaves it
    // undecided when decision is null), mirroring what the finalize/recovery apply would install.
    private static void Seed(EmbeddedKahunaNode node, HLCTimestamp txId, string anchor, TransactionDecision? decision)
    {
        HLCTimestamp commitTs = new(txId.N, txId.L + 1, txId.C);
        HLCTimestamp deadline = new(commitTs.N, commitTs.L + 100_000, commitTs.C);
        List<TransactionParticipantRef> manifest = [new TransactionParticipantRef(anchor, KeyValueDurability.Persistent)];
        long manifestHash = TransactionManifest.ComputeHash(txId, Epoch, anchor, commitTs, manifest);

        TransactionRecordStore store = Store(node);
        store.Apply(new InitializeTransactionCommand(txId, Epoch, "lost-coord", anchor, commitTs, deadline, manifestHash, manifest, commitTs, txId));

        switch (decision)
        {
            case TransactionDecision.Commit:
                store.Apply(new CommitTransactionCommand(txId, Epoch, manifestHash, commitTs, commitTs));
                break;
            case TransactionDecision.Abort:
                store.Apply(new AbortTransactionCommand(txId, Epoch, manifestHash, TransactionAbortClass.Conflict, commitTs, commitTs, anchor, commitTs, deadline, txId));
                break;
            // null → leave Undecided
        }
    }

    [Fact]
    public async Task MissingSession_CommittedRecord_CommitReturnsCommitted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "consult-done:aaa";
        Seed(node, txId, anchor, TransactionDecision.Commit);

        // A handle for a session that does not exist in memory.
        TransactionHandle handle = new(txId, "lost-coord", anchor);
        (KeyValueResponseType type, string? returnedAnchor) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        Assert.Equal(KeyValueResponseType.Committed, type);
        Assert.Equal(anchor, returnedAnchor);
    }

    [Fact]
    public async Task MissingSession_UndecidedRecord_CommitReturnsMustRetry()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "consult-pending:aaa";
        Seed(node, txId, anchor, decision: null);

        TransactionHandle handle = new(txId, "lost-coord", anchor);
        (KeyValueResponseType type, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        Assert.Equal(KeyValueResponseType.MustRetry, type);
    }

    [Fact]
    public async Task MissingSession_ConflictAbortedRecord_CommitReturnsAborted()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "consult-abort:aaa";
        Seed(node, txId, anchor, TransactionDecision.Abort);

        TransactionHandle handle = new(txId, "lost-coord", anchor);
        (KeyValueResponseType type, _) = await node.Kahuna.LocateAndCommitTransaction(handle, ct);

        Assert.Equal(KeyValueResponseType.Aborted, type);
    }

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

    [Fact]
    public async Task MissingSession_Rollback_CannotOverrideCommittedTransaction()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        // Committed record → rollback of the lost session must report Committed, not RolledBack/Errored.
        HLCTimestamp doneTx = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string doneAnchor = "consult-rb-done:aaa";
        Seed(node, doneTx, doneAnchor, TransactionDecision.Commit);
        KeyValueResponseType doneRollback = await node.Kahuna.LocateAndRollbackTransaction(new(doneTx, "lost-coord", doneAnchor), ct);
        Assert.Equal(KeyValueResponseType.Committed, doneRollback);

        // Undecided record → rollback must report MustRetry (recovery finishes it), not roll back.
        HLCTimestamp pendingTx = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string pendingAnchor = "consult-rb-pending:aaa";
        Seed(node, pendingTx, pendingAnchor, decision: null);
        KeyValueResponseType pendingRollback = await node.Kahuna.LocateAndRollbackTransaction(new(pendingTx, "lost-coord", pendingAnchor), ct);
        Assert.Equal(KeyValueResponseType.MustRetry, pendingRollback);
    }
}

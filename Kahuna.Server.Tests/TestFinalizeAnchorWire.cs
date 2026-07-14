
using Kahuna.Client;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The finalize transport must carry the record anchor. After the coordinating session is gone (evicted or its
/// node failed), the anchor on a commit/rollback retry is the only route to the durable decision — so a retry
/// that supplies it resolves the outcome (<c>Committed</c> for a decided commit) while an unanchored retry, which
/// cannot scan the cluster by transaction id, stays unknown. This drives the same handle reconstruction the gRPC
/// service and inter-node transport use, so it proves the anchor survives the transport rather than being dropped.
/// </summary>
[Collection("ClusterTests")]
public sealed class TestFinalizeAnchorWire
{
    private static EmbeddedKahunaOptions EmbeddedOptions() => new()
    {
        Storage = "memory",
        WalStorage = "memory",
        InitialPartitions = 1,
    };

    private static async Task SeedCompleted(EmbeddedKahunaNode node, HLCTimestamp txId, string anchor)
    {
        CoordinatorDecisionRecord record = new(
            txId, "lost-coord", anchor, txId, CoordinatorDecisionStatus.Completed,
            [new CoordinatorParticipant(anchor, KeyValueDurability.Persistent, HLCTimestamp.Zero, true, true)],
            [], txId, txId);
        await node.Kahuna.ImportCoordinatorDecisions([record]);
    }

    // ── A commit retry carrying the anchor reaches the durable decision and reports Committed ──────────────

    [Fact]
    public async Task CommitRetry_WithAnchor_OverTransport_ReachesDurableDecision()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "anchorwire-commit:aaa";
        await SeedCompleted(node, txId, anchor);

        InProcessKahunaCommunication comm = new(node.Kahuna);

        // The coordinating session no longer exists in memory; the transport carries the anchor, so the retry
        // resolves to the durably decided commit.
        (bool committed, _) = await comm.CommitTransactionSession("https://localhost", "lost-coord", txId, anchor, ct);
        Assert.True(committed);
    }

    // ── The same retry without the anchor cannot reach the record and stays unknown Errored ────────────────

    [Fact]
    public async Task CommitRetry_WithoutAnchor_CannotReachDurableDecision()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "anchorwire-noanchor:aaa";
        await SeedCompleted(node, txId, anchor);

        InProcessKahunaCommunication comm = new(node.Kahuna);

        // A dropped anchor leaves the retry unable to locate the durable record: unknown Errored, surfaced as a
        // terminal KahunaException — the exact regression the anchor-carrying wire prevents.
        KahunaException ex = await Assert.ThrowsAsync<KahunaException>(() =>
            comm.CommitTransactionSession("https://localhost", "lost-coord", txId, null, ct));
        Assert.Equal(KeyValueResponseType.Errored, ex.KeyValueErrorCode);
    }

    // ── A rollback retry carrying the anchor cannot override a durably decided commit ──────────────────────

    [Fact]
    public async Task RollbackRetry_WithAnchor_CannotOverrideDecidedCommit()
    {
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using EmbeddedKahunaNode node = new(EmbeddedOptions());
        await node.StartAsync(ct);

        HLCTimestamp txId = node.Raft.HybridLogicalClock.TrySendOrLocalEvent(node.Raft.GetLocalNodeId());
        const string anchor = "anchorwire-rollback:aaa";
        await SeedCompleted(node, txId, anchor);

        InProcessKahunaCommunication comm = new(node.Kahuna);

        // The decided commit surfaces as Committed even through the rollback path; the transport must carry the
        // anchor for that consultation to happen at all. A non-RolledBack/non-MustRetry outcome is terminal.
        KahunaException ex = await Assert.ThrowsAsync<KahunaException>(() =>
            comm.RollbackTransactionSession("https://localhost", "lost-coord", txId, anchor, ct));
        Assert.Equal(KeyValueResponseType.Committed, ex.KeyValueErrorCode);
    }
}

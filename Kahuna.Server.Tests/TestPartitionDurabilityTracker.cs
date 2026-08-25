using Kahuna.Server.Persistence;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for the per-partition application-durability watermark: registration/resolution
/// ordering, idempotent redelivery, gaps from undelivered consensus-internal entries, and the
/// per-channel snapshot resolution used by the receipt/record/intent stores.
/// </summary>
public sealed class TestPartitionDurabilityTracker
{
    [Fact]
    public void Watermark_IsNoOpinion_WhenNothingRegistered()
    {
        PartitionDurabilityTracker tracker = new();

        Assert.Equal(-1, tracker.GetWatermark(1));
    }

    [Fact]
    public void Watermark_StaysBelowPending_AndAdvancesOnResolve()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(1, 10, DurabilityChannel.Flush);
        tracker.RegisterPending(1, 11, DurabilityChannel.Flush);

        Assert.Equal(9, tracker.GetWatermark(1));

        tracker.Resolve(1, 10);
        Assert.Equal(10, tracker.GetWatermark(1));

        tracker.Resolve(1, 11);
        Assert.Equal(11, tracker.GetWatermark(1));
    }

    [Fact]
    public void Watermark_CoversGapsFromUndeliveredEntries()
    {
        PartitionDurabilityTracker tracker = new();

        // Ids 11-14 were never delivered (checkpoints, barriers): once 10 and 15 resolve, the
        // watermark passes the gap — undelivered entries need no application durability.
        tracker.RegisterPending(1, 10, DurabilityChannel.Flush);
        tracker.Resolve(1, 10);
        tracker.RegisterPending(1, 15, DurabilityChannel.Flush);

        Assert.Equal(14, tracker.GetWatermark(1));

        tracker.Resolve(1, 15);
        Assert.Equal(15, tracker.GetWatermark(1));
    }

    [Fact]
    public void RegisterPending_IsIdempotentByIndex()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(1, 10, DurabilityChannel.Flush);
        tracker.Resolve(1, 10);

        // Redelivery of an already-resolved index must not re-open it.
        tracker.RegisterPending(1, 10, DurabilityChannel.Flush);
        Assert.Equal(10, tracker.GetWatermark(1));

        // Redelivery of a still-pending index is a no-op too.
        tracker.RegisterPending(1, 11, DurabilityChannel.Flush);
        tracker.RegisterPending(1, 11, DurabilityChannel.Flush);
        tracker.Resolve(1, 11);
        Assert.Equal(11, tracker.GetWatermark(1));
    }

    [Fact]
    public void RegisterDurable_AdvancesWithoutPendingPhase()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterDurable(1, 20);
        Assert.Equal(20, tracker.GetWatermark(1));

        // A later pending entry caps the watermark again.
        tracker.RegisterPending(1, 21, DurabilityChannel.Flush);
        Assert.Equal(20, tracker.GetWatermark(1));
    }

    [Fact]
    public void SnapshotChannels_ResolveUpToCeiling_OnlyTheirOwnChannel()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(1, 10, DurabilityChannel.Receipts);
        tracker.MarkApplied(1, 10, DurabilityChannel.Receipts);
        tracker.RegisterPending(1, 11, DurabilityChannel.Flush);
        tracker.RegisterPending(1, 12, DurabilityChannel.Receipts);
        tracker.MarkApplied(1, 12, DurabilityChannel.Receipts);

        Assert.Equal(12, tracker.GetHighestApplied(1, DurabilityChannel.Receipts));
        Assert.True(tracker.HasPendingSnapshotWork(1));

        // A durable receipt snapshot covering applies up to 12 resolves both receipt entries but
        // never the flush-channel entry sitting between them.
        tracker.ResolveUpTo(1, DurabilityChannel.Receipts, 12);

        Assert.Equal(10, tracker.GetWatermark(1));
        Assert.False(tracker.HasPendingSnapshotWork(1));

        tracker.Resolve(1, 11);
        Assert.Equal(12, tracker.GetWatermark(1));
    }

    [Fact]
    public void SnapshotChannels_ResolveUpTo_LeavesEntriesAboveCeilingPending()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(1, 10, DurabilityChannel.TransactionRecords);
        tracker.MarkApplied(1, 10, DurabilityChannel.TransactionRecords);
        tracker.RegisterPending(1, 11, DurabilityChannel.TransactionRecords);

        // The snapshot was captured with a ceiling of 10 — entry 11 registered but its apply was
        // not marked, so it stays pending.
        tracker.ResolveUpTo(1, DurabilityChannel.TransactionRecords, 10);

        Assert.Equal(10, tracker.GetWatermark(1));
        Assert.True(tracker.HasPendingSnapshotWork(1));
    }

    [Fact]
    public void Partitions_AreIndependent()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(1, 10, DurabilityChannel.Flush);
        tracker.RegisterPending(2, 50, DurabilityChannel.Flush);
        tracker.Resolve(2, 50);

        Assert.Equal(9, tracker.GetWatermark(1));
        Assert.Equal(50, tracker.GetWatermark(2));
        Assert.Equal(2, tracker.ObservedPartitions.Count);
    }

    [Fact]
    public void FailedApply_KeepsFloorBelowEntry()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(1, 10, DurabilityChannel.Flush);
        tracker.Resolve(1, 10);

        // Entry 11's apply failed: registered, never resolved. The floor freezes below it even as
        // later entries register and resolve.
        tracker.RegisterPending(1, 11, DurabilityChannel.Flush);
        tracker.RegisterPending(1, 12, DurabilityChannel.Flush);
        tracker.Resolve(1, 12);

        Assert.Equal(10, tracker.GetWatermark(1));
    }

    // ── Two-channel entries (transactional key-values: flushed row + derived receipt) ─────

    /// <summary>
    /// A transactional key-value entry registers on Flush AND Receipts. The flush ack alone must
    /// not let the floor pass it — its derived completion receipt is durable only when a receipt
    /// snapshot covers it. Both orders of resolution finish the entry.
    /// </summary>
    [Fact]
    public void TwoChannelEntry_FlushAloneDoesNotAdvanceWatermark()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(1, 10, DurabilityChannel.Flush, DurabilityChannel.Receipts);
        tracker.MarkApplied(1, 10, DurabilityChannel.Receipts);

        tracker.Resolve(1, 10);

        // The flushed row landed, but the receipt snapshot has not: still pending.
        Assert.Equal(9, tracker.GetWatermark(1));
        Assert.True(tracker.HasPendingSnapshotWork(1));

        tracker.ResolveUpTo(1, DurabilityChannel.Receipts, 10);

        Assert.Equal(10, tracker.GetWatermark(1));
        Assert.False(tracker.HasPendingSnapshotWork(1));
    }

    [Fact]
    public void TwoChannelEntry_ReceiptSnapshotAloneDoesNotAdvanceWatermark()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(1, 10, DurabilityChannel.Flush, DurabilityChannel.Receipts);
        tracker.MarkApplied(1, 10, DurabilityChannel.Receipts);

        tracker.ResolveUpTo(1, DurabilityChannel.Receipts, 10);

        // The receipt snapshot landed, but the row flush has not: still pending, and the
        // remaining requirement is the flush, so no snapshot work is outstanding.
        Assert.Equal(9, tracker.GetWatermark(1));
        Assert.False(tracker.HasPendingSnapshotWork(1));

        tracker.Resolve(1, 10);

        Assert.Equal(10, tracker.GetWatermark(1));
    }

    /// <summary>
    /// A receipt snapshot ceiling below a two-channel entry must leave its Receipts requirement
    /// pending, and a flush resolve of a neighboring single-channel entry must not disturb it.
    /// </summary>
    [Fact]
    public void TwoChannelEntry_AboveSnapshotCeiling_StaysPendingOnReceipts()
    {
        PartitionDurabilityTracker tracker = new();

        tracker.RegisterPending(1, 10, DurabilityChannel.Flush, DurabilityChannel.Receipts);
        tracker.MarkApplied(1, 10, DurabilityChannel.Receipts);
        tracker.RegisterPending(1, 11, DurabilityChannel.Flush);
        tracker.RegisterPending(1, 12, DurabilityChannel.Flush, DurabilityChannel.Receipts);
        tracker.MarkApplied(1, 12, DurabilityChannel.Receipts);

        tracker.Resolve(1, 10);
        tracker.Resolve(1, 11);
        tracker.Resolve(1, 12);

        // Snapshot captured with a ceiling of 10: entry 12's receipt is not covered.
        tracker.ResolveUpTo(1, DurabilityChannel.Receipts, 10);

        Assert.Equal(11, tracker.GetWatermark(1));
        Assert.True(tracker.HasPendingSnapshotWork(1));

        tracker.ResolveUpTo(1, DurabilityChannel.Receipts, 12);

        Assert.Equal(12, tracker.GetWatermark(1));
        Assert.False(tracker.HasPendingSnapshotWork(1));
    }
}

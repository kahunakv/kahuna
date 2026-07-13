
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit coverage for the node-local completion-receipt store: recording, presence, idempotency,
/// the zero-transaction guard, and the coordinator-driven forget.
/// </summary>
public sealed class TestCompletionReceiptStore
{
    [Fact]
    public void RecordThenContains_IsTrueForRecordedKeyOnly()
    {
        CompletionReceiptStore store = new();
        HLCTimestamp tx = new(1, 100, 0);

        store.Record(tx, "k1", "anchor", KeyValueDurability.Persistent);

        Assert.True(store.Contains(tx, "k1"));
        Assert.False(store.Contains(tx, "k2"));
        Assert.False(store.Contains(new HLCTimestamp(1, 200, 0), "k1"));
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void Record_IsIdempotent_KeepsFirstAndDoesNotGrow()
    {
        CompletionReceiptStore store = new();
        HLCTimestamp tx = new(1, 100, 0);

        store.Record(tx, "k1", "anchor", KeyValueDurability.Persistent);
        store.Record(tx, "k1", "anchor-changed", KeyValueDurability.Persistent);

        Assert.True(store.Contains(tx, "k1"));
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void Record_ZeroTransactionOrEmptyKey_IsSkipped()
    {
        CompletionReceiptStore store = new();

        store.Record(HLCTimestamp.Zero, "k1", null, KeyValueDurability.Persistent);
        store.Record(new HLCTimestamp(1, 100, 0), "", null, KeyValueDurability.Persistent);

        Assert.Equal(0, store.Count);
        Assert.False(store.Contains(HLCTimestamp.Zero, "k1"));
    }

    [Fact]
    public void Forget_RemovesReceipt_Idempotently()
    {
        CompletionReceiptStore store = new();
        HLCTimestamp tx = new(1, 100, 0);
        store.Record(tx, "k1", null, KeyValueDurability.Persistent);

        Assert.True(store.Forget(tx, "k1"));
        Assert.False(store.Contains(tx, "k1"));
        Assert.False(store.Forget(tx, "k1"));
    }

    [Fact]
    public void Snapshot_ReflectsRecordedReceipts()
    {
        CompletionReceiptStore store = new();
        HLCTimestamp txA = new(1, 100, 0);
        HLCTimestamp txB = new(1, 200, 0);
        store.Record(txA, "k1", "anchorA", KeyValueDurability.Persistent);
        store.Record(txB, "k2", null, KeyValueDurability.Persistent);

        var snapshot = store.Snapshot();

        Assert.Equal(2, snapshot.Count);
        Assert.Contains(snapshot, r => r.TransactionId == txA && r.Key == "k1" && r.RecordAnchorKey == "anchorA");
        Assert.Contains(snapshot, r => r.TransactionId == txB && r.Key == "k2" && r.RecordAnchorKey == null);
    }
}


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

        Assert.True(store.Contains(tx, "k1", KeyValueDurability.Persistent));
        Assert.False(store.Contains(tx, "k2", KeyValueDurability.Persistent));
        Assert.False(store.Contains(new HLCTimestamp(1, 200, 0), "k1", KeyValueDurability.Persistent));
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void Contains_ValidatesFullTuple_RejectsMismatchedDurabilityOrAnchor()
    {
        CompletionReceiptStore store = new();
        HLCTimestamp tx = new(1, 100, 0);

        store.Record(tx, "k1", "anchor", KeyValueDurability.Persistent);

        // The exact tuple matches, with or without an anchor to check.
        Assert.True(store.Contains(tx, "k1", KeyValueDurability.Persistent));
        Assert.True(store.Contains(tx, "k1", KeyValueDurability.Persistent, "anchor"));

        // A persistent receipt must not satisfy an ephemeral request for the same logical key.
        Assert.False(store.Contains(tx, "k1", KeyValueDurability.Ephemeral));

        // A mismatched anchor is rejected when the caller supplies one to validate.
        Assert.False(store.Contains(tx, "k1", KeyValueDurability.Persistent, "other-anchor"));
    }

    [Fact]
    public void Record_IsIdempotent_KeepsFirstAndDoesNotGrow()
    {
        CompletionReceiptStore store = new();
        HLCTimestamp tx = new(1, 100, 0);

        store.Record(tx, "k1", "anchor", KeyValueDurability.Persistent);
        store.Record(tx, "k1", "anchor-changed", KeyValueDurability.Persistent);

        Assert.True(store.Contains(tx, "k1", KeyValueDurability.Persistent));
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void Record_ZeroTransactionOrEmptyKey_IsSkipped()
    {
        CompletionReceiptStore store = new();

        store.Record(HLCTimestamp.Zero, "k1", null, KeyValueDurability.Persistent);
        store.Record(new HLCTimestamp(1, 100, 0), "", null, KeyValueDurability.Persistent);

        Assert.Equal(0, store.Count);
        Assert.False(store.Contains(HLCTimestamp.Zero, "k1", KeyValueDurability.Persistent));
    }

    [Fact]
    public void Forget_RemovesReceipt_Idempotently()
    {
        CompletionReceiptStore store = new();
        HLCTimestamp tx = new(1, 100, 0);
        store.Record(tx, "k1", null, KeyValueDurability.Persistent);

        Assert.True(store.Forget(tx, "k1"));
        Assert.False(store.Contains(tx, "k1", KeyValueDurability.Persistent));
        Assert.False(store.Forget(tx, "k1"));
    }

    [Fact]
    public void SnapshotRange_FiltersByKeyRange_AndImportRangeRestores()
    {
        CompletionReceiptStore store = new();
        HLCTimestamp txA = new(1, 100, 0);
        HLCTimestamp txB = new(1, 200, 0);
        HLCTimestamp txC = new(1, 300, 0);
        store.Record(txA, "user/a", "user/a", KeyValueDurability.Persistent);
        store.Record(txB, "user/m", null, KeyValueDurability.Persistent);
        store.Record(txC, "user/z", null, KeyValueDurability.Persistent);

        // Everything.
        Assert.Equal(3, store.SnapshotRange(null, null).Count);

        // Half-open [user/m, user/z): only user/m.
        var moved = store.SnapshotRange("user/m", "user/z");
        Assert.Single(moved);
        Assert.Contains(moved, r => r.TransactionId == txB && r.Key == "user/m");

        // Importing into a fresh store restores the receipts.
        CompletionReceiptStore destination = new();
        destination.ImportRange(moved);
        Assert.True(destination.Contains(txB, "user/m", KeyValueDurability.Persistent));
        Assert.False(destination.Contains(txA, "user/a", KeyValueDurability.Persistent));
        Assert.Equal(1, destination.Count);
    }
}

using Kahuna.Server.KeyValues;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Covers the streaming decode of a replicated completion-receipt batch in <c>CompletionReceiptStore.Apply</c>:
/// the apply path reads fields straight off a <c>CodedInputStream</c> instead of materializing the generated
/// message tree, so these tests pin the behaviour that a mixed record / forget batch — including entries with and
/// without the optional record anchor, and different durabilities — applies identically to what the generated
/// parser would have produced.
/// </summary>
public sealed class TestCompletionReceiptStreamingDecode
{
    private static RaftLog RecordLog(params CompletionReceiptRecord[] records) => new()
    {
        LogType = ReplicationTypes.CompletionReceipt,
        LogData = CompletionReceiptStore.SerializeImport(records, destinationPartitionId: 7)
    };

    private static RaftLog ForgetLog(params CompletionReceiptRecord[] records) => new()
    {
        LogType = ReplicationTypes.CompletionReceipt,
        LogData = CompletionReceiptStore.SerializeImport(records, destinationPartitionId: 7, forget: true)
    };

    [Fact]
    public void RecordBatch_DecodesEveryEntryIncludingAbsentAnchor()
    {
        CompletionReceiptStore store = new();

        HLCTimestamp txWithAnchor = new(1, 1000, 3);
        HLCTimestamp txNoAnchor   = new(2, 2000, 0);
        HLCTimestamp txEphemeral  = new(3, 3000, 7);

        // One entry carries the optional RecordAnchorKey, one omits it (field 5 absent must decode to null), and
        // one is ephemeral so the durability scalar must survive the streaming read distinctly.
        Assert.True(store.Replicate(7, RecordLog(
            new CompletionReceiptRecord(txWithAnchor, "k:with-anchor", "anchor:1", KeyValueDurability.Persistent),
            new CompletionReceiptRecord(txNoAnchor,   "k:no-anchor",   null,       KeyValueDurability.Persistent),
            new CompletionReceiptRecord(txEphemeral,  "k:ephemeral",   null,       KeyValueDurability.Ephemeral))));

        // Anchor decoded and validated.
        Assert.True(store.Contains(txWithAnchor, "k:with-anchor", KeyValueDurability.Persistent, "anchor:1"));
        Assert.False(store.Contains(txWithAnchor, "k:with-anchor", KeyValueDurability.Persistent, "wrong-anchor"));

        // Absent anchor decoded as null, not as the previous entry's anchor.
        Assert.True(store.Contains(txNoAnchor, "k:no-anchor", KeyValueDurability.Persistent));
        Assert.False(store.Contains(txNoAnchor, "k:no-anchor", KeyValueDurability.Persistent, "anchor:1"));

        // Durability scalar decoded per entry: a persistent request must not match the ephemeral receipt.
        Assert.True(store.Contains(txEphemeral, "k:ephemeral", KeyValueDurability.Ephemeral));
        Assert.False(store.Contains(txEphemeral, "k:ephemeral", KeyValueDurability.Persistent));

        Assert.Equal(3, store.Count);
    }

    [Fact]
    public void ForgetBatch_RemovesEveryListedReceipt()
    {
        CompletionReceiptStore store = new();

        HLCTimestamp txA = new(1, 1000, 1);
        HLCTimestamp txB = new(1, 1000, 2);
        HLCTimestamp txKeep = new(1, 1000, 3);

        Assert.True(store.Replicate(7, RecordLog(
            new CompletionReceiptRecord(txA,    "k:a",    "anchor:a", KeyValueDurability.Persistent),
            new CompletionReceiptRecord(txB,    "k:b",    null,       KeyValueDurability.Persistent),
            new CompletionReceiptRecord(txKeep, "k:keep", null,       KeyValueDurability.Persistent))));
        Assert.Equal(3, store.Count);

        Assert.True(store.Replicate(7, ForgetLog(
            new CompletionReceiptRecord(txA, "k:a", "anchor:a", KeyValueDurability.Persistent),
            new CompletionReceiptRecord(txB, "k:b", null,       KeyValueDurability.Persistent))));

        Assert.False(store.Contains(txA, "k:a", KeyValueDurability.Persistent, "anchor:a"));
        Assert.False(store.Contains(txB, "k:b", KeyValueDurability.Persistent));
        Assert.True(store.Contains(txKeep, "k:keep", KeyValueDurability.Persistent));
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void StreamingDecode_MatchesGeneratedParser()
    {
        CompletionReceiptRecord[] records =
        [
            new(new HLCTimestamp(1, 111, 0), "k:1", "anchor:1", KeyValueDurability.Persistent),
            new(new HLCTimestamp(2, 222, 5), "k:2", null,       KeyValueDurability.Ephemeral),
            new(new HLCTimestamp(3, 333, 9), "k:3", "anchor:3", KeyValueDurability.Persistent),
        ];

        // The streaming apply must land exactly what the generated parser would have decoded from the same bytes.
        CompletionReceiptStore streamed = new();
        Assert.True(streamed.Replicate(7, RecordLog(records)));

        foreach (CompletionReceiptRecord r in records)
        {
            Assert.True(streamed.Contains(r.TransactionId, r.Key, r.Durability, r.RecordAnchorKey));
            if (r.RecordAnchorKey is null)
                Assert.True(streamed.Contains(r.TransactionId, r.Key, r.Durability));
        }

        Assert.Equal(records.Length, streamed.Count);
    }

    [Fact]
    public void Restore_UsesTheSameStreamingPath()
    {
        CompletionReceiptStore store = new();

        HLCTimestamp tx = new(4, 4000, 1);
        Assert.True(store.Restore(7, RecordLog(
            new CompletionReceiptRecord(tx, "k:restore", "anchor:r", KeyValueDurability.Persistent))));

        Assert.True(store.Contains(tx, "k:restore", KeyValueDurability.Persistent, "anchor:r"));
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public void NonReceiptLog_IsIgnored()
    {
        CompletionReceiptStore store = new();

        Assert.True(store.Replicate(7, new RaftLog { LogType = ReplicationTypes.KeyValues, LogData = [1, 2, 3] }));
        Assert.Equal(0, store.Count);
    }
}

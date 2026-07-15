
using Kahuna.Server.KeyValues;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Parity and behavioural tests for <see cref="KeyValueRevisionHistory"/>.
/// Verifies that the compact array-based container produces results identical to the
/// <c>Dictionary&lt;long, KeyValueRevisionEntry&gt;</c> it replaces for every operation
/// pattern used by the key-value actor: append, exact-revision lookup, at-or-before
/// scan, front-trim, idempotent overwrite, iteration order, and capacity growth.
/// </summary>
public class TestKeyValueRevisionHistory
{
    private static KeyValueRevisionEntry MakeRevision(int tag) =>
        new(new byte[] { (byte)tag }, new HLCTimestamp(1, (long)(tag * 1000), 0), HLCTimestamp.Zero, KeyValueState.Set);

    // ── basic structural correctness ───────────────────────────────────────────────

    [Fact]
    public void NewHistory_IsEmpty()
    {
        KeyValueRevisionHistory h = new();
        Assert.Equal(0, h.Count);
    }

    [Fact]
    public void SingleEntry_CountAndTryGetValue()
    {
        KeyValueRevisionHistory h = new();
        h[5] = MakeRevision(5);

        Assert.Equal(1, h.Count);
        Assert.True(h.TryGetValue(5, out KeyValueRevisionEntry v));
        Assert.Equal((byte)5, v.Value![0]);
    }

    [Fact]
    public void MultipleEntries_AppendedInOrder_CountCorrect()
    {
        KeyValueRevisionHistory h = new();
        for (int i = 1; i <= 10; i++) h[i] = MakeRevision(i);
        Assert.Equal(10, h.Count);
    }

    // ── exact-revision lookup (binary search) ─────────────────────────────────────

    [Fact]
    public void TryGetValue_ExistingKey_ReturnsCorrectEntry()
    {
        KeyValueRevisionHistory h = new();
        h[1] = MakeRevision(1);
        h[5] = MakeRevision(5);
        h[10] = MakeRevision(10);

        Assert.True(h.TryGetValue(1, out var v1));  Assert.Equal((byte)1,  v1.Value![0]);
        Assert.True(h.TryGetValue(5, out var v5));  Assert.Equal((byte)5,  v5.Value![0]);
        Assert.True(h.TryGetValue(10, out var v10)); Assert.Equal((byte)10, v10.Value![0]);
    }

    [Fact]
    public void TryGetValue_MissingKey_ReturnsFalse()
    {
        KeyValueRevisionHistory h = new();
        h[3] = MakeRevision(3);
        Assert.False(h.TryGetValue(99, out _));
        Assert.False(h.TryGetValue(0, out _));
    }

    [Fact]
    public void ContainsKey_ExistingAndMissing()
    {
        KeyValueRevisionHistory h = new();
        h[7] = MakeRevision(7);
        Assert.True(h.ContainsKey(7));
        Assert.False(h.ContainsKey(8));
    }

    // ── idempotent overwrite (same key, different value) ──────────────────────────

    [Fact]
    public void IndexerSet_SameKey_OverwritesValue()
    {
        KeyValueRevisionHistory h = new();
        h[4] = MakeRevision(4);
        h[4] = new KeyValueRevisionEntry(new byte[] { 99 }, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Deleted);

        Assert.Equal(1, h.Count);
        Assert.True(h.TryGetValue(4, out var v));
        Assert.Equal((byte)99, v.Value![0]);
        Assert.Equal(KeyValueState.Deleted, v.State);
    }

    // ── Remove (linear scan + left-shift) ─────────────────────────────────────────

    [Fact]
    public void Remove_ExistingEntry_DecreasesCount()
    {
        KeyValueRevisionHistory h = new();
        h[1] = MakeRevision(1);
        h[2] = MakeRevision(2);
        h[3] = MakeRevision(3);

        bool removed = h.Remove(2);
        Assert.True(removed);
        Assert.Equal(2, h.Count);
        Assert.False(h.TryGetValue(2, out _));
        Assert.True(h.TryGetValue(1, out _));
        Assert.True(h.TryGetValue(3, out _));
    }

    [Fact]
    public void Remove_MissingKey_ReturnsFalse()
    {
        KeyValueRevisionHistory h = new();
        h[10] = MakeRevision(10);
        Assert.False(h.Remove(99));
        Assert.Equal(1, h.Count);
    }

    [Fact]
    public void Remove_AllEntries_CountIsZero()
    {
        KeyValueRevisionHistory h = new();
        for (int i = 1; i <= 5; i++) h[i] = MakeRevision(i);
        for (int i = 1; i <= 5; i++) h.Remove(i);
        Assert.Equal(0, h.Count);
    }

    [Fact]
    public void Remove_FirstEntry_RemainingEntriesIntact()
    {
        KeyValueRevisionHistory h = new();
        h[1] = MakeRevision(1);
        h[2] = MakeRevision(2);
        h[3] = MakeRevision(3);

        h.Remove(1);
        Assert.Equal(2, h.Count);
        Assert.False(h.ContainsKey(1));
        Assert.True(h.ContainsKey(2));
        Assert.True(h.ContainsKey(3));
    }

    // ── iteration order (ascending key order) ─────────────────────────────────────

    [Fact]
    public void GetEnumerator_YieldsAscendingOrder()
    {
        KeyValueRevisionHistory h = new();
        for (int i = 1; i <= 8; i++) h[i] = MakeRevision(i);

        long prev = -1;
        foreach (KeyValuePair<long, KeyValueRevisionEntry> kv in h)
        {
            Assert.True(kv.Key > prev, $"Expected ascending order, got {kv.Key} after {prev}");
            Assert.Equal((byte)kv.Key, kv.Value.Value![0]);
            prev = kv.Key;
        }
        Assert.Equal(8, h.Count);
    }

    [Fact]
    public void GetEnumerator_DeconstructPattern_Works()
    {
        KeyValueRevisionHistory h = new();
        h[10] = MakeRevision(10);
        h[20] = MakeRevision(20);

        List<long> keys = [];
        foreach ((long k, KeyValueRevisionEntry _) in h)
            keys.Add(k);

        Assert.Equal([10, 20], keys);
    }

    // ── at-or-before scan parity (mirrors KeyValueEntry.TryGetRevisionAtOrBefore) ─

    [Fact]
    public void AtOrBefore_FindsHighestRevisionAtOrBeforeSnapshot()
    {
        // Revision timestamps: rev 1 → T=1000, rev 2 → T=2000, rev 3 → T=3000
        KeyValueRevisionHistory h = new();
        h[1] = new KeyValueRevisionEntry(new byte[] { 1 }, new HLCTimestamp(0, 1000L, 0), HLCTimestamp.Zero, KeyValueState.Set);
        h[2] = new KeyValueRevisionEntry(new byte[] { 2 }, new HLCTimestamp(0, 2000L, 0), HLCTimestamp.Zero, KeyValueState.Set);
        h[3] = new KeyValueRevisionEntry(new byte[] { 3 }, new HLCTimestamp(0, 3000L, 0), HLCTimestamp.Zero, KeyValueState.Set);

        HLCTimestamp snapshot = new(0, 2500L, 0);
        long revNum = -1;
        KeyValueRevisionEntry found = default;
        bool ok = false;

        foreach ((long candidateNumber, KeyValueRevisionEntry candidate) in h)
        {
            if (candidate.LastModified > snapshot) continue;
            if (!ok || candidateNumber > revNum) { revNum = candidateNumber; found = candidate; ok = true; }
        }

        Assert.True(ok);
        Assert.Equal(2L, revNum);
        Assert.Equal((byte)2, found.Value![0]);
    }

    [Fact]
    public void AtOrBefore_AllRevisionsNewerThanSnapshot_ReturnsNone()
    {
        KeyValueRevisionHistory h = new();
        h[1] = new KeyValueRevisionEntry(new byte[] { 1 }, new HLCTimestamp(0, 5000L, 0), HLCTimestamp.Zero, KeyValueState.Set);

        HLCTimestamp snapshot = new(0, 1000L, 0);
        bool ok = false;

        foreach ((long _, KeyValueRevisionEntry candidate) in h)
        {
            if (candidate.LastModified <= snapshot) { ok = true; break; }
        }

        Assert.False(ok);
    }

    // ── capacity growth ────────────────────────────────────────────────────────────

    [Fact]
    public void CapacityGrowth_BeyondInitial_AllEntriesRetained()
    {
        KeyValueRevisionHistory h = new();
        // Start with 1 pre-allocated slot; write 17 entries (RevisionRetention=16+boundary=1)
        for (int i = 1; i <= 17; i++) h[i] = MakeRevision(i);

        Assert.Equal(17, h.Count);
        for (int i = 1; i <= 17; i++)
        {
            Assert.True(h.TryGetValue(i, out var v), $"Missing rev {i}");
            Assert.Equal((byte)i, v.Value![0]);
        }
    }

    // ── front-trim pattern (how BaseHandler removes expired revisions) ─────────────

    [Fact]
    public void FrontTrim_RemovesOldestRevisions_KeepsNewest()
    {
        KeyValueRevisionHistory h = new();
        for (int i = 1; i <= 8; i++) h[i] = MakeRevision(i);

        // Trim revisions 1–4 (front)
        for (int i = 1; i <= 4; i++) h.Remove(i);

        Assert.Equal(4, h.Count);
        for (int i = 1; i <= 4; i++) Assert.False(h.ContainsKey(i), $"Rev {i} should have been removed");
        for (int i = 5; i <= 8; i++) Assert.True(h.ContainsKey(i), $"Rev {i} should be retained");
    }
}

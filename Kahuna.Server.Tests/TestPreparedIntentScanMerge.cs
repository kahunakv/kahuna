using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for <see cref="PreparedIntentScanMerge"/>: how a range/prefix scan page reconciles with prepared
/// intents — override, inject, exclude, invisibility, TTL expiry, page-retry — and how it owns pagination: the
/// merged sequence is capped at exactly <c>limit</c>, an injected key can become the cursor, and a committed delete
/// on the sentinel cannot masquerade as end-of-scan.
/// </summary>
public sealed class TestPreparedIntentScanMerge
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static readonly HLCTimestamp Tc = Ts(1000);

    private static ReadOnlyKeyValueEntry Entry(byte[] value, long revision = 1) =>
        new(value, revision, HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set);

    private static PreparedIntent Intent(string key, PreparedIntentResolution resolution, KeyValueState state = KeyValueState.Set,
        byte[]? value = null, HLCTimestamp expires = default) =>
        new(Ts(500), 1, key, 0, key, CommitTimestamp: Tc,
            State: state, Value: value ?? [9], Bucket: null, Revision: 5, Expires: expires,
            NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(9000),
            Resolution: resolution);

    private static List<(string, ReadOnlyKeyValueEntry)> Page(params string[] keys)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];
        foreach (string k in keys)
            items.Add((k, Entry([1])));
        return items;
    }

    // A page that is fully exhausted (no KV rows beyond it): kvHasMore false, no ceiling. limit defaults large so no
    // sentinel truncation unless a test asks for it.
    private static PreparedIntentScanMerge.ScanMergeResult MergeExhausted(
        List<(string, ReadOnlyKeyValueEntry)> items, PreparedIntent[] intents, HLCTimestamp snapshotTs, HLCTimestamp currentTime = default) =>
        PreparedIntentScanMerge.Merge(items, intents, snapshotTs, currentTime, limit: 100, kvHasMore: false, kvCeilingKey: null, decisionLookup: null);

    private static List<string> Keys(PreparedIntentScanMerge.ScanMergeResult result) =>
        result.Items.Select(i => i.Key).ToList();

    [Fact]
    public void NoVisibleIntents_Unchanged()
    {
        var result = MergeExhausted(Page("a", "b"), [Intent("z", PreparedIntentResolution.Aborted)], HLCTimestamp.Zero);
        Assert.False(result.MustRetry);
        Assert.Equal(["a", "b"], Keys(result));
        Assert.False(result.HasMore);
    }

    [Fact]
    public void CommittedIntent_OverridesExistingValue()
    {
        var result = MergeExhausted(
            Page("a", "b"), [Intent("a", PreparedIntentResolution.Committed, value: [7])], HLCTimestamp.Zero);

        Assert.Equal(["a", "b"], Keys(result));
        Assert.Equal(new byte[] { 7 }, result.Items[0].Entry.Value);
    }

    [Fact]
    public void CommittedIntent_InjectsIntentOnlyKey_InOrdinalPosition()
    {
        var result = MergeExhausted(
            Page("a", "c"), [Intent("b", PreparedIntentResolution.Committed)], HLCTimestamp.Zero);

        Assert.Equal(["a", "b", "c"], Keys(result)); // injected between a and c
    }

    [Fact]
    public void CommittedDelete_ExcludesKey()
    {
        var result = MergeExhausted(
            Page("a", "b", "c"), [Intent("b", PreparedIntentResolution.Committed, state: KeyValueState.Deleted)], HLCTimestamp.Zero);

        Assert.Equal(["a", "c"], Keys(result));
    }

    [Fact]
    public void ExpiredCommittedIntent_ExcludedLikeADelete()
    {
        // A committed value whose TTL elapsed before "now" is not served as live: it is dropped from the page,
        // matching the ordinary scan's expiry filter. currentTime is past the intent's Expires.
        HLCTimestamp expires = Ts(1500);
        HLCTimestamp now = Ts(2000);

        var overrideResult = MergeExhausted(
            Page("a", "b"), [Intent("a", PreparedIntentResolution.Committed, value: [7], expires: expires)], HLCTimestamp.Zero, now);
        Assert.Equal(["b"], Keys(overrideResult)); // expired committed override drops the key entirely

        var injectResult = MergeExhausted(
            Page("a", "c"), [Intent("b", PreparedIntentResolution.Committed, expires: expires)], HLCTimestamp.Zero, now);
        Assert.Equal(["a", "c"], Keys(injectResult)); // expired intent-only key not injected
    }

    [Fact]
    public void UnexpiredCommittedIntent_WithTtl_StillVisible()
    {
        HLCTimestamp expires = Ts(3000);
        HLCTimestamp now = Ts(2000); // before expiry

        var result = MergeExhausted(
            Page("a", "c"), [Intent("b", PreparedIntentResolution.Committed, expires: expires)], HLCTimestamp.Zero, now);
        Assert.Equal(["a", "b", "c"], Keys(result));
    }

    [Fact]
    public void AbortedIntent_Invisible()
    {
        var result = MergeExhausted(
            Page("a"), [Intent("z", PreparedIntentResolution.Aborted)], HLCTimestamp.Zero);

        Assert.Equal(["a"], Keys(result)); // aborted intent-only key not injected
    }

    [Fact]
    public void PendingIntent_MakesPageRetry()
    {
        var result = MergeExhausted(
            Page("a", "b"), [Intent("a", PreparedIntentResolution.Pending)], HLCTimestamp.Zero);

        Assert.True(result.MustRetry);
    }

    [Fact]
    public void SnapshotBelowCommit_Invisible()
    {
        var result = MergeExhausted(
            Page("a"), [Intent("b", PreparedIntentResolution.Committed)], Ts(999)); // T < Tc

        Assert.Equal(["a"], Keys(result)); // intent invisible, no inject
        Assert.False(result.MustRetry);
    }

    // ── Pagination ──────────────────────────────────────────────────────────────

    [Fact]
    public void InjectionsOverflowingLimit_AreCappedAtLimit_AndCursorIsLastEmitted()
    {
        // limit=2, KV page "a","d" plus injected intents "b","c": the merged sequence a,b,c,d exceeds the limit and
        // must be trimmed to exactly two, with the cursor at the second key so the next page resumes after it.
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "d"),
            [Intent("b", PreparedIntentResolution.Committed), Intent("c", PreparedIntentResolution.Committed)],
            HLCTimestamp.Zero, currentTime: default, limit: 2, kvHasMore: false, kvCeilingKey: null, decisionLookup: null);

        Assert.Equal(["a", "b"], Keys(result));
        Assert.True(result.HasMore);
        Assert.Equal("b", result.NextCursorKey);
    }

    [Fact]
    public void InjectedKey_BecomesTheCursor_WhenItIsTheLimitThKey()
    {
        // limit=2, KV "a","c", injected "b": merged a,b,c capped to a,b; the injected intent-only key b is the cursor.
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "c"),
            [Intent("b", PreparedIntentResolution.Committed)],
            HLCTimestamp.Zero, currentTime: default, limit: 2, kvHasMore: false, kvCeilingKey: null, decisionLookup: null);

        Assert.Equal(["a", "b"], Keys(result));
        Assert.True(result.HasMore);
        Assert.Equal("b", result.NextCursorKey);
    }

    [Fact]
    public void KvHasMore_NoOverflow_ReportsHasMoreWithKvCeilingCursor()
    {
        // The KV side signalled more beyond this page (a sentinel), the intents do not overflow the limit: the page
        // still reports HasMore and resumes after the KV ceiling.
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "b"),
            [Intent("a", PreparedIntentResolution.Committed, value: [7])],
            HLCTimestamp.Zero, currentTime: default, limit: 5, kvHasMore: true, kvCeilingKey: "b", decisionLookup: null);

        Assert.Equal(["a", "b"], Keys(result));
        Assert.True(result.HasMore);
        Assert.Equal("b", result.NextCursorKey);
    }

    [Fact]
    public void CommittedDeleteOnSentinel_StillReportsHasMore_ViaKvCeiling()
    {
        // A committed delete removes the last (sentinel) key, shrinking the merged page below the KV window; because
        // the KV side had more, the scan must not conclude end-of-scan — it resumes after the KV ceiling.
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "b"),
            [Intent("b", PreparedIntentResolution.Committed, state: KeyValueState.Deleted)],
            HLCTimestamp.Zero, currentTime: default, limit: 5, kvHasMore: true, kvCeilingKey: "b", decisionLookup: null);

        Assert.Equal(["a"], Keys(result));
        Assert.True(result.HasMore);
        Assert.Equal("b", result.NextCursorKey);
    }

    [Fact]
    public void ExhaustedPage_NoKvMore_ReportsNoMore()
    {
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "c"),
            [Intent("b", PreparedIntentResolution.Committed)],
            HLCTimestamp.Zero, currentTime: default, limit: 5, kvHasMore: false, kvCeilingKey: null, decisionLookup: null);

        Assert.Equal(["a", "b", "c"], Keys(result));
        Assert.False(result.HasMore);
        Assert.Null(result.NextCursorKey);
    }
}

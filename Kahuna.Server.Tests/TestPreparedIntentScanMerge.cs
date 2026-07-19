using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// Unit tests for <see cref="PreparedIntentScanMerge"/>: how a range/prefix scan page reconciles with prepared
/// intents — override, inject, exclude, invisibility, page-retry, and the pagination-window boundary for
/// intent-only injected keys.
/// </summary>
public sealed class TestPreparedIntentScanMerge
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static readonly HLCTimestamp Tc = Ts(1000);

    private static ReadOnlyKeyValueEntry Entry(byte[] value, long revision = 1) =>
        new(value, revision, HLCTimestamp.Zero, HLCTimestamp.Zero, HLCTimestamp.Zero, KeyValueState.Set);

    private static PreparedIntent Intent(string key, PreparedIntentResolution resolution, KeyValueState state = KeyValueState.Set, byte[]? value = null) =>
        new(Ts(500), 1, key, 0, key, CommitTimestamp: Tc,
            State: state, Value: value ?? [9], Bucket: null, Revision: 5, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(9000),
            Resolution: resolution);

    private static List<(string, ReadOnlyKeyValueEntry)> Page(params string[] keys)
    {
        List<(string, ReadOnlyKeyValueEntry)> items = [];
        foreach (string k in keys)
            items.Add((k, Entry([1])));
        return items;
    }

    private static List<string> Keys((List<(string Key, ReadOnlyKeyValueEntry Entry)> Items, bool _) result) =>
        result.Items.Select(i => i.Key).ToList();

    [Fact]
    public void NoIntents_Unchanged()
    {
        var result = PreparedIntentScanMerge.Merge(Page("a", "b"), [], HLCTimestamp.Zero, pageExhausted: true);
        Assert.False(result.MustRetry);
        Assert.Equal(["a", "b"], Keys(result));
    }

    [Fact]
    public void CommittedIntent_OverridesExistingValue()
    {
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "b"), [Intent("a", PreparedIntentResolution.Committed, value: [7])], HLCTimestamp.Zero, pageExhausted: true);

        Assert.Equal(["a", "b"], Keys(result));
        Assert.Equal(new byte[] { 7 }, result.Items[0].Entry.Value);
    }

    [Fact]
    public void CommittedIntent_InjectsIntentOnlyKey_InOrdinalPosition()
    {
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "c"), [Intent("b", PreparedIntentResolution.Committed)], HLCTimestamp.Zero, pageExhausted: true);

        Assert.Equal(["a", "b", "c"], Keys(result)); // injected between a and c
    }

    [Fact]
    public void CommittedDelete_ExcludesKey()
    {
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "b", "c"), [Intent("b", PreparedIntentResolution.Committed, state: KeyValueState.Deleted)], HLCTimestamp.Zero, pageExhausted: true);

        Assert.Equal(["a", "c"], Keys(result));
    }

    [Fact]
    public void AbortedIntent_Invisible()
    {
        var result = PreparedIntentScanMerge.Merge(
            Page("a"), [Intent("z", PreparedIntentResolution.Aborted)], HLCTimestamp.Zero, pageExhausted: true);

        Assert.Equal(["a"], Keys(result)); // aborted intent-only key not injected
    }

    [Fact]
    public void PendingIntent_MakesPageRetry()
    {
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "b"), [Intent("a", PreparedIntentResolution.Pending)], HLCTimestamp.Zero, pageExhausted: true);

        Assert.True(result.MustRetry);
        Assert.Equal(["a", "b"], Keys(result)); // unmodified on retry
    }

    [Fact]
    public void SnapshotBelowCommit_Invisible()
    {
        var result = PreparedIntentScanMerge.Merge(
            Page("a"), [Intent("b", PreparedIntentResolution.Committed)], readTimestampBelowTc(), pageExhausted: true);

        Assert.Equal(["a"], Keys(result)); // T < Tc: intent invisible, no inject
        Assert.False(result.MustRetry);
    }

    private static HLCTimestamp readTimestampBelowTc() => Ts(999);

    [Fact]
    public void HasMorePage_IntentOnlyKeyBeyondWindow_NotInjected()
    {
        // Page fetched up to "c" and is not exhausted (more keys follow). An intent-only committed "e" is beyond
        // the window and belongs to a later page.
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "c"), [Intent("e", PreparedIntentResolution.Committed)], HLCTimestamp.Zero, pageExhausted: false);

        Assert.Equal(["a", "c"], Keys(result));
    }

    [Fact]
    public void HasMorePage_IntentOnlyKeyWithinWindow_Injected()
    {
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "c"), [Intent("b", PreparedIntentResolution.Committed)], HLCTimestamp.Zero, pageExhausted: false);

        Assert.Equal(["a", "b", "c"], Keys(result)); // b <= window max "c" → injected
    }

    [Fact]
    public void ExhaustedPage_IntentOnlyKeyBeyondItems_Injected()
    {
        var result = PreparedIntentScanMerge.Merge(
            Page("a", "c"), [Intent("e", PreparedIntentResolution.Committed)], HLCTimestamp.Zero, pageExhausted: true);

        Assert.Equal(["a", "c", "e"], Keys(result)); // exhausted → inject even beyond the last item
    }
}

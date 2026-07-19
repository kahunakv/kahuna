using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// The read-visibility matrix of <see cref="PreparedIntentVisibility"/>: latest and snapshot reads against
/// committed, aborted, and undecided intents, including the key property that a snapshot strictly below the
/// intent's commit timestamp is served the prior value with no decision lookup.
/// </summary>
public sealed class TestPreparedIntentVisibility
{
    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static readonly HLCTimestamp Tc = Ts(1000);

    private static PreparedIntent Intent(PreparedIntentResolution resolution) =>
        new(Ts(500), 1, "k", 0, "k", CommitTimestamp: Tc,
            State: KeyValueState.Set, Value: [1], Bucket: null, Revision: 1, Expires: HLCTimestamp.Zero,
            NoRevision: false, BaseRevision: 0, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(9000),
            Resolution: resolution);

    private static ReadVisibilityAction Latest(PreparedIntent? intent) =>
        PreparedIntentVisibility.Resolve(intent, HLCTimestamp.Zero);

    private static ReadVisibilityAction Snapshot(PreparedIntent? intent, HLCTimestamp at) =>
        PreparedIntentVisibility.Resolve(intent, at);

    [Fact]
    public void NoIntent_UsesExisting()
    {
        Assert.Equal(ReadVisibilityAction.UseExisting, Latest(null));
        Assert.Equal(ReadVisibilityAction.UseExisting, Snapshot(null, Ts(2000)));
    }

    [Fact]
    public void Latest_Committed_UsesIntentValue() =>
        Assert.Equal(ReadVisibilityAction.UseIntentValue, Latest(Intent(PreparedIntentResolution.Committed)));

    [Fact]
    public void Latest_Aborted_UsesExisting() =>
        Assert.Equal(ReadVisibilityAction.UseExisting, Latest(Intent(PreparedIntentResolution.Aborted)));

    [Fact]
    public void Latest_Pending_Retries() =>
        Assert.Equal(ReadVisibilityAction.Retry, Latest(Intent(PreparedIntentResolution.Pending)));

    [Fact]
    public void SnapshotBelowCommit_UsesExisting_RegardlessOfOutcome()
    {
        HLCTimestamp below = Ts(999);
        Assert.Equal(ReadVisibilityAction.UseExisting, Snapshot(Intent(PreparedIntentResolution.Committed), below));
        Assert.Equal(ReadVisibilityAction.UseExisting, Snapshot(Intent(PreparedIntentResolution.Pending), below));
        Assert.Equal(ReadVisibilityAction.UseExisting, Snapshot(Intent(PreparedIntentResolution.Aborted), below));
    }

    [Fact]
    public void SnapshotAtOrAfterCommit_Committed_UsesIntentValue()
    {
        Assert.Equal(ReadVisibilityAction.UseIntentValue, Snapshot(Intent(PreparedIntentResolution.Committed), Tc));       // T == Tc
        Assert.Equal(ReadVisibilityAction.UseIntentValue, Snapshot(Intent(PreparedIntentResolution.Committed), Ts(2000))); // T > Tc
    }

    [Fact]
    public void SnapshotAtOrAfterCommit_Aborted_UsesExisting() =>
        Assert.Equal(ReadVisibilityAction.UseExisting, Snapshot(Intent(PreparedIntentResolution.Aborted), Ts(2000)));

    [Fact]
    public void SnapshotAtOrAfterCommit_Undecided_Retries() =>
        Assert.Equal(ReadVisibilityAction.Retry, Snapshot(Intent(PreparedIntentResolution.Pending), Ts(2000)));
}

using Kommander.Data;
using Kommander.Time;

using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.KeyValues;
using Kahuna.Server.Replication;
using Kahuna.Shared.KeyValue;
using Kahuna.Utils;

namespace Kahuna.Server.Tests;

/// <summary>
/// A whole-partition section captures the committed-head ledger, then walks the intents. A transaction settled between
/// the two is in neither unless the section adds its head, and the replica it seeds would then hold one head fewer than
/// its peers at the same log position, which reads as an apply divergence the moment that replica is compared.
/// </summary>
public sealed class TestExportKeepsHeadsSettledDuringCapture
{
    private const int Partition = 7;

    private static PreparedIntent Intent(string key, long physical, long revision) => new(
        TransactionId: new HLCTimestamp(1, physical, 0), Epoch: 1, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: new HLCTimestamp(1, physical + 1, 0),
        State: KeyValueState.Set, Value: [1, 2, 3], Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: false,
        BaseRevision: revision - 1, BaseState: KeyValueState.Set,
        RecoveryDeadline: HLCTimestamp.Zero, Resolution: PreparedIntentResolution.Pending);

    private static void Prepare(PreparedIntentStore store, PreparedIntent intent) =>
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new PrepareIntentCommand(intent), Partition).Outcome);

    private static void Settle(PreparedIntentStore store, PreparedIntent intent)
    {
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true), Partition).Outcome);
        Assert.Equal(TransactionApplyOutcome.Applied, store.Apply(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key), Partition).Outcome);
    }

    private static PreparedIntentStore Install(PreparedIntentStore source)
    {
        using SegmentedBufferStream payload = new();
        source.WritePartitionSection(payload, Partition, static _ => true);

        byte[] bytes = new byte[payload.Length];
        Assert.Equal(bytes.Length, payload.Read(bytes, 0, bytes.Length));

        PreparedIntentStore target = new();
        target.ImportPartitionIntents(Partition, PreparedIntentStore.DeserializePartitionIntents(bytes), requireLedger: true);
        return target;
    }

    [Fact]
    public void AHeadSettledBetweenTheCaptureAndTheWalk_ReachesTheSection()
    {
        PreparedIntentStore source = new();

        PreparedIntent settledInTheGap = Intent("k/gap", 1_000, 5);
        PreparedIntent stillPending = Intent("k/pending", 1_010, 8);
        PreparedIntent alreadySettled = Intent("k/early", 990, 3);

        Prepare(source, alreadySettled);
        Settle(source, alreadySettled);
        Prepare(source, settledInTheGap);
        Prepare(source, stillPending);

        source.AfterLedgerCaptureForTesting = () => Settle(source, settledInTheGap);

        PreparedIntentStore target = Install(source);

        Assert.True(target.TryGetCommittedHead("k/gap", out long revision, out _));
        Assert.Equal(5, revision);
        Assert.True(target.TryGetCommittedHead("k/early", out long early, out _));
        Assert.Equal(3, early);

        // The fingerprint compares exactly this count.
        Assert.Equal(source.CommittedHeadCountForPartition(Partition), target.CommittedHeadCountForPartition(Partition));

        // A transaction still pending is carried as an intent and has no head yet.
        Assert.False(target.TryGetCommittedHead("k/pending", out _, out _));
        Assert.NotNull(target.Get("k/pending"));
    }

    [Fact]
    public void ANewerHeadSettledInTheGap_ReplacesTheOneTheCaptureRead()
    {
        PreparedIntentStore source = new();

        PreparedIntent first = Intent("k/a", 1_000, 5);
        PreparedIntent second = Intent("k/a", 1_100, 6);

        Prepare(source, first);
        Settle(source, first);
        Prepare(source, second);

        source.AfterLedgerCaptureForTesting = () => Settle(source, second);

        PreparedIntentStore target = Install(source);

        Assert.True(target.TryGetCommittedHead("k/a", out long revision, out _));
        Assert.Equal(6, revision);
    }

    [Fact]
    public void AnEntryAppliedDuringTheCapture_IsNotCertifiedAsFullyReflected()
    {
        PreparedIntentStore source = new();

        RaftLog Logged(long index, params PreparedIntentCommand[] commands) => new()
        {
            Id = index,
            LogType = ReplicationTypes.PreparedIntent,
            LogData = [.. PreparedIntentStore.SerializeDelta(commands)]
        };

        PreparedIntent before = Intent("k/before", 1_000, 5);
        Assert.True(source.ApplyDeltaAckPrepares(Partition, Logged(10, new PrepareIntentCommand(before))));

        // Both of its steps land after the capture read the ledger, so only a replay can restore its head, and a
        // replay at or below the fully reflected position would install nothing for its prepare.
        PreparedIntent during = Intent("k/during", 1_100, 8);
        source.AfterLedgerCaptureForTesting = () =>
        {
            source.AfterLedgerCaptureForTesting = null;
            Assert.True(source.ApplyDeltaAckPrepares(Partition, Logged(11, new PrepareIntentCommand(during))));
            Assert.True(source.ApplyDeltaAckPrepares(Partition, Logged(
                12,
                new ResolveIntentCommand(during.TransactionId, during.Epoch, during.Key, Commit: true),
                new RemoveIntentCommand(during.TransactionId, during.Epoch, during.Key))));
        };

        PreparedIntentStore target = Install(source);

        Assert.Equal(10, target.GetLedgerFullyReflectedThroughIndex(Partition));
        Assert.Equal(12, target.GetLedgerReflectedThroughIndex(Partition));

        // The replay the receiver runs brings the head back.
        Assert.False(target.TryGetCommittedHead("k/during", out _, out _));
        Assert.True(target.ApplyDeltaAckPrepares(Partition, Logged(11, new PrepareIntentCommand(during))));
        Assert.True(target.ApplyDeltaAckPrepares(Partition, Logged(
            12,
            new ResolveIntentCommand(during.TransactionId, during.Epoch, during.Key, Commit: true),
            new RemoveIntentCommand(during.TransactionId, during.Epoch, during.Key))));

        Assert.True(target.TryGetCommittedHead("k/during", out long revision, out _));
        Assert.Equal(8, revision);
        Assert.Equal(source.CommittedHeadCountForPartition(Partition), target.CommittedHeadCountForPartition(Partition));
    }

    [Fact]
    public void AnAbortInTheGap_AddsNoHead()
    {
        PreparedIntentStore source = new();

        PreparedIntent aborted = Intent("k/a", 1_000, 5);
        Prepare(source, aborted);

        source.AfterLedgerCaptureForTesting = () =>
        {
            source.Apply(new ResolveIntentCommand(aborted.TransactionId, aborted.Epoch, aborted.Key, Commit: false), Partition);
            source.Apply(new RemoveIntentCommand(aborted.TransactionId, aborted.Epoch, aborted.Key), Partition);
        };

        PreparedIntentStore target = Install(source);

        Assert.False(target.TryGetCommittedHead("k/a", out _, out _));
    }
}

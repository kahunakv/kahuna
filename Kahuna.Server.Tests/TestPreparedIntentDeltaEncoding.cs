using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Server.Replication;
using Kahuna.Server.Replication.Protos;
using Kahuna.Shared.KeyValue;
using Kommander.Data;
using Kommander.Time;

namespace Kahuna.Server.Tests;

/// <summary>
/// A prepared-intent delta normally carries one transaction attempt's transitions for one partition, so the
/// transaction identity, manifest hash, commit timestamp, recovery deadline and record-anchor key are identical on
/// every command. Those are encoded once for the batch instead of once per key.
///
/// <para>The compaction must be invisible: a delta that is compacted, one whose commands disagree and so is not, and
/// one written before the compact form existed must all reconstruct exactly the same transitions.</para>
/// </summary>
public sealed class TestPreparedIntentDeltaEncoding
{
    private const int PartitionId = 3;

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent Intent(HLCTimestamp txId, long epoch, string key, string anchor = "anchor") =>
        new(txId, epoch, key, ManifestHash: 4242, RecordAnchorKey: anchor, CommitTimestamp: Ts(1100),
            State: KeyValueState.Set, Value: [7, 8, 9], Bucket: "bucket", Revision: 5, Expires: Ts(50000),
            NoRevision: false, BaseRevision: 4, BaseState: KeyValueState.Set, RecoveryDeadline: Ts(6000),
            Resolution: PreparedIntentResolution.Pending);

    private static RaftLog IntentLog(params PreparedIntentCommand[] commands) =>
        new() { LogType = ReplicationTypes.PreparedIntent, LogData = PreparedIntentStore.SerializeDelta(commands) };

    [Fact]
    public void SingleTransactionBatch_RoundTripsEveryField()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 3;

        PreparedIntentCommand[] commands =
        [
            new PrepareIntentCommand(Intent(txId, epoch, "row/1")),
            new PrepareIntentCommand(Intent(txId, epoch, "row/2")),
            new PrepareIntentCommand(Intent(txId, epoch, "row/3"))
        ];

        PreparedIntentStore store = new();
        Assert.True(store.Replicate(PartitionId, IntentLog(commands)));

        foreach (string key in new[] { "row/1", "row/2", "row/3" })
        {
            PreparedIntent? intent = store.Get(key);
            Assert.NotNull(intent);

            // The hoisted fields are what would silently corrupt if the header were mis-read.
            Assert.Equal(txId, intent!.TransactionId);
            Assert.Equal(epoch, intent.Epoch);
            Assert.Equal(4242, intent.ManifestHash);
            Assert.Equal("anchor", intent.RecordAnchorKey);
            Assert.Equal(Ts(1100), intent.CommitTimestamp);
            Assert.Equal(Ts(6000), intent.RecoveryDeadline);

            // The per-command fields must survive alongside them.
            Assert.Equal(key, intent.Key);
            Assert.Equal(KeyValueState.Set, intent.State);
            Assert.Equal(new byte[] { 7, 8, 9 }, intent.Value);
            Assert.Equal("bucket", intent.Bucket);
            Assert.Equal(5, intent.Revision);
            Assert.Equal(Ts(50000), intent.Expires);
            Assert.Equal(4, intent.BaseRevision);
        }
    }

    [Fact]
    public void SingleTransactionBatch_IsSmallerThanOneThatCannotBeCompacted()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 3;

        // Same shape and the same anchor-key length either way; only agreement differs, so the size gap is exactly
        // what hoisting the shared fields buys.
        PreparedIntentCommand[] shared = new PreparedIntentCommand[16];
        PreparedIntentCommand[] divergent = new PreparedIntentCommand[16];

        for (int i = 0; i < 16; i++)
        {
            shared[i] = new PrepareIntentCommand(Intent(txId, epoch, $"row/{i:D2}", "anchor00"));
            divergent[i] = new PrepareIntentCommand(Intent(txId, epoch, $"row/{i:D2}", $"anchor{i:D2}"));
        }

        int sharedBytes = PreparedIntentStore.SerializeDelta(shared).Length;
        int divergentBytes = PreparedIntentStore.SerializeDelta(divergent).Length;

        Assert.True(sharedBytes < divergentBytes,
            $"expected the compacted delta to be smaller, got shared={sharedBytes} divergent={divergentBytes}");
    }

    [Fact]
    public void MixedTransactionBatch_IsNotCompactedAndStillRoundTrips()
    {
        HLCTimestamp txA = Ts(1000);
        HLCTimestamp txB = Ts(2000);

        PreparedIntentStore store = new();
        Assert.True(store.Replicate(PartitionId, IntentLog(
            new PrepareIntentCommand(Intent(txA, 1, "row/a")),
            new PrepareIntentCommand(Intent(txB, 7, "row/b")))));

        // Each key keeps its own transaction identity — a header applied across disagreeing commands would collapse
        // both onto one transaction and silently misattribute an intent.
        Assert.Equal(txA, store.Get("row/a")!.TransactionId);
        Assert.Equal(1, store.Get("row/a")!.Epoch);
        Assert.Equal(txB, store.Get("row/b")!.TransactionId);
        Assert.Equal(7, store.Get("row/b")!.Epoch);
    }

    [Fact]
    public void MixedKindBatch_HoistsOnlyWhatEveryCommandAgreesOn()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 2;

        PreparedIntentStore store = new();
        Assert.True(store.Replicate(PartitionId, IntentLog(
            new PrepareIntentCommand(Intent(txId, epoch, "row/1")),
            new PrepareIntentCommand(Intent(txId, epoch, "row/2")),
            new ResolveIntentCommand(txId, epoch, "row/1", Commit: true))));

        // The resolve targeted the same identity, so it must have matched the live intent rather than been rejected
        // as belonging to a different transaction.
        Assert.Equal(PreparedIntentResolution.Committed, store.Get("row/1")!.Resolution);
        Assert.Equal(PreparedIntentResolution.Pending, store.Get("row/2")!.Resolution);
    }

    [Fact]
    public void DeltaWrittenWithoutAHeader_IsStillRead()
    {
        HLCTimestamp txId = Ts(1000);
        const long epoch = 4;

        // The form written before the header existed, and the form still written whenever commands disagree: every
        // field lives on the command. Built here directly so the reader is exercised against it explicitly rather
        // than only through whatever the current writer happens to emit.
        PreparedIntentDeltaMessage delta = new();
        delta.Commands.Add(new PreparedIntentCommandMessage
        {
            Kind = PreparedIntentCommandKindMessage.PreparedIntentPrepare,
            TransactionIdNode = txId.N, TransactionIdPhysical = txId.L, TransactionIdCounter = txId.C,
            Epoch = epoch, Key = "row/legacy",
            ManifestHash = 4242, RecordAnchorKey = "anchor",
            CommitTimestampNode = Ts(1100).N, CommitTimestampPhysical = Ts(1100).L, CommitTimestampCounter = Ts(1100).C,
            State = (int)KeyValueState.Set,
            Value = Google.Protobuf.ByteString.CopyFrom(7, 8, 9), ValueNull = false,
            Bucket = "bucket", BucketNull = false,
            Revision = 5,
            ExpiresNode = Ts(50000).N, ExpiresPhysical = Ts(50000).L, ExpiresCounter = Ts(50000).C,
            NoRevision = false,
            BaseRevision = 4, BaseState = (int)KeyValueState.Set,
            RecoveryDeadlineNode = Ts(6000).N, RecoveryDeadlinePhysical = Ts(6000).L, RecoveryDeadlineCounter = Ts(6000).C,
            Resolution = (int)PreparedIntentResolution.Pending
        });

        PreparedIntentStore store = new();
        Assert.True(store.Replicate(PartitionId, new RaftLog
        {
            LogType = ReplicationTypes.PreparedIntent,
            LogData = ReplicationSerializer.Serialize(delta)
        }));

        PreparedIntent? intent = store.Get("row/legacy");
        Assert.NotNull(intent);
        Assert.Equal(txId, intent!.TransactionId);
        Assert.Equal(epoch, intent.Epoch);
        Assert.Equal(4242, intent.ManifestHash);
        Assert.Equal("anchor", intent.RecordAnchorKey);
        Assert.Equal(Ts(1100), intent.CommitTimestamp);
        Assert.Equal(Ts(6000), intent.RecoveryDeadline);
        Assert.Equal(new byte[] { 7, 8, 9 }, intent.Value);
    }
}

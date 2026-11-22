using Google.Protobuf;
using Kommander.Time;

using Kahuna.Server.KeyValues;
using Kahuna.Server.KeyValues.Transactions;
using Kahuna.Server.KeyValues.Transactions.Data;
using Kahuna.Shared.KeyValue;

namespace Kahuna.Server.Tests;

/// <summary>
/// The prepared-intent snapshot is decoded one entry at a time: at a restart from the checkpoint file, and at
/// the install of a whole-partition section. The whole-message parser held one protobuf object for every
/// intent, head and settled identity at once, next to the slice they became. These tests check that the
/// entry-by-entry reader gives the state the whole-message path gave, for both orders of the fields that a
/// writer produces, and that an install still refuses a bad section before it removes anything.
/// </summary>
public sealed class TestPreparedIntentSnapshotStreaming : IDisposable
{
    private const int Partition = 3;

    private readonly string dir = Path.Combine(Path.GetTempPath(), "kahuna-intent-stream-" + Guid.NewGuid().ToString("N"));

    public TestPreparedIntentSnapshotStreaming() => Directory.CreateDirectory(dir);

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best-effort temp cleanup */ }
    }

    private static HLCTimestamp Ts(long l) => new(0, l, 0);

    private static PreparedIntent MakeIntent(string key, long tx, long revision = 1) => new(
        TransactionId: Ts(tx), Epoch: 1, Key: key,
        ManifestHash: 0, RecordAnchorKey: key,
        CommitTimestamp: Ts(tx + 1),
        State: KeyValueState.Set, Value: [1, 2, 3], Bucket: null,
        Revision: revision, Expires: HLCTimestamp.Zero, NoRevision: true,
        BaseRevision: PreparedIntent.UnknownBaseRevision, BaseState: KeyValueState.Undefined,
        RecoveryDeadline: Ts(tx + 5_000), Resolution: PreparedIntentResolution.Pending);

    private static void Settle(PreparedIntentStore store, PreparedIntent intent)
    {
        store.Apply(new PrepareIntentCommand(intent), Partition);
        store.Apply(new ResolveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key, Commit: true), Partition);
        store.Apply(new RemoveIntentCommand(intent.TransactionId, intent.Epoch, intent.Key), Partition);
    }

    /// <summary>A store with heads, settled identities, an aborted leg, a pending intent and a committed intent
    /// that is not removed yet.</summary>
    private static PreparedIntentStore Exporter(int settled = 400)
    {
        PreparedIntentStore store = new();

        for (int i = 0; i < settled; i++)
            Settle(store, MakeIntent($"s/k{i:D5}", 10_000 + i, revision: 3));

        PreparedIntent aborted = MakeIntent("s/aborted", 20_000);
        store.Apply(new PrepareIntentCommand(aborted), Partition);
        store.Apply(new ResolveIntentCommand(aborted.TransactionId, 1, aborted.Key, Commit: false), Partition);
        store.Apply(new RemoveIntentCommand(aborted.TransactionId, 1, aborted.Key), Partition);

        store.Apply(new PrepareIntentCommand(MakeIntent("live/pending", 30_000)), Partition);

        PreparedIntent committed = MakeIntent("live/committed", 30_100, revision: 7);
        store.Apply(new PrepareIntentCommand(committed), Partition);
        store.Apply(new ResolveIntentCommand(committed.TransactionId, 1, committed.Key, Commit: true), Partition);

        return store;
    }

    private static byte[] StreamedSection(PreparedIntentStore store)
    {
        using MemoryStream output = new();
        store.WritePartitionSection(output, Partition, static _ => true);
        return output.ToArray();
    }

    /// <summary>The section with one more entry whose length says 127 bytes and that has none: a stream that
    /// ends inside an entry.</summary>
    private static byte[] Truncated(byte[] section) => [.. section, 0x0A, 0x7F];

    private static void AssertSameSlice(PreparedIntentStore expected, PreparedIntentStore actual)
    {
        Assert.Equal(expected.SnapshotLedger(Partition), actual.SnapshotLedger(Partition));
        Assert.Equal(expected.SnapshotSettledIdentities(Partition), actual.SnapshotSettledIdentities(Partition));

        // The slice keeps its own count of the heads; the gauges and the apply fingerprint read it.
        Assert.Equal(actual.SnapshotLedger(Partition).Count, actual.CommittedHeadCountForPartition(Partition));
        Assert.Equal(expected.CommittedHeadCount, actual.CommittedHeadCount);
        Assert.Equal(expected.GetLedgerWatermark(Partition), actual.GetLedgerWatermark(Partition));
        Assert.Equal(expected.GetLedgerReflectedThroughIndex(Partition), actual.GetLedgerReflectedThroughIndex(Partition));
        Assert.Equal(expected.GetLedgerFullyReflectedThroughIndex(Partition), actual.GetLedgerFullyReflectedThroughIndex(Partition));
        Assert.Equal(expected.GetLedgerSizeFloor(Partition), actual.GetLedgerSizeFloor(Partition));

        IReadOnlyList<PreparedIntent> expectedIntents = [.. expected.SnapshotRange(null, null).OrderBy(i => i.Key, StringComparer.Ordinal)];
        IReadOnlyList<PreparedIntent> actualIntents = [.. actual.SnapshotRange(null, null).OrderBy(i => i.Key, StringComparer.Ordinal)];

        Assert.Equal(expectedIntents.Count, actualIntents.Count);
        for (int i = 0; i < expectedIntents.Count; i++)
        {
            Assert.Equal(expectedIntents[i].Key, actualIntents[i].Key);
            Assert.Equal(expectedIntents[i].TransactionId, actualIntents[i].TransactionId);
            Assert.Equal(expectedIntents[i].Resolution, actualIntents[i].Resolution);
            Assert.Equal(expectedIntents[i].Revision, actualIntents[i].Revision);
            Assert.Equal(expectedIntents[i].Value, actualIntents[i].Value);
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TheStreamInstall_GivesTheStateOfTheSectionInstall_ForBothFieldOrders(bool streamedOrder)
    {
        PreparedIntentStore exporter = Exporter();

        // The exporter writes the settled identities first; the message serializer writes the fields in the
        // order of their numbers, with the scalar fields between the heads and the settled identities.
        byte[] section = streamedOrder
            ? StreamedSection(exporter)
            : exporter.SerializePartitionIntents(Partition, [.. exporter.SnapshotRange(null, null)], fullyReflectedThroughIndex: 0);

        PreparedIntentStore bySection = new();
        bySection.ReplacePartitionIntents(Partition, PreparedIntentStore.DeserializePartitionIntents(section), requireLedger: true, static _ => true);

        PreparedIntentStore.SnapshotHeader header = PreparedIntentStore.VerifyPartitionSection(new MemoryStream(section));
        Assert.True(header.LedgerPresent);
        Assert.Equal(Partition, header.LedgerPartitionId);
        Assert.Equal(2, header.Intents);
        Assert.Equal(401, header.Heads);
        Assert.Equal(401, header.SettledIdentities);
        Assert.Equal(exporter.GetLedgerWatermark(Partition), header.LedgerWatermark);

        // The receiver holds state of its own for the partition; the install replaces it.
        PreparedIntentStore byStream = new();
        Settle(byStream, MakeIntent("old/settled", 500));
        byStream.Apply(new PrepareIntentCommand(MakeIntent("old/pending", 600)), Partition);

        byStream.ReplacePartitionIntents(Partition, new MemoryStream(section), header, requireLedger: true, static _ => true);

        Assert.Null(byStream.Get("old/pending"));
        AssertSameSlice(bySection, byStream);

        // The verdicts of the installed state: a replayed prepare of a settled leg is rejected, the pending
        // intent holds its key, and the committed intent settles.
        Assert.True(byStream.Apply(new PrepareIntentCommand(MakeIntent("s/k00123", 10_123, revision: 3)), Partition).SettledReplay);
        Assert.True(byStream.Apply(new PrepareIntentCommand(MakeIntent("s/aborted", 20_000)), Partition).SettledReplay);
        Assert.NotNull(byStream.Get("live/pending"));
        Assert.Equal(PreparedIntentResolution.Committed, byStream.Get("live/committed")!.Resolution);
    }

    [Fact]
    public void ARestart_LoadsAFileInEitherFieldOrder()
    {
        PreparedIntentStore source = new(dir, "rev", null);
        source.AttachPartitionResolver(_ => Partition);

        for (int i = 0; i < 300; i++)
            Settle(source, MakeIntent($"s/k{i:D5}", 10_000 + i, revision: 2));

        source.Apply(new PrepareIntentCommand(MakeIntent("live/pending", 30_000)), Partition);
        Assert.True(source.PersistSnapshot(Partition, appliedThroughIndex: 0));

        // The checkpoint file has the settled identities first.
        PreparedIntentStore fromCheckpoint = new(dir, "rev", null);
        fromCheckpoint.AttachPartitionResolver(_ => Partition);
        AssertSameSlice(source, fromCheckpoint);

        // The same content in the order of the message serializer: the scalar fields come before the settled
        // identities and after the heads. The loader reads the file once, so it must not need them first.
        string path = Directory.GetFiles(dir, "*_p3.snapshot").Single();
        File.WriteAllBytes(path, source.SerializePartitionIntents(Partition, [.. source.SnapshotRange(null, null)]));

        PreparedIntentStore fromSerializer = new(dir, "rev", null);
        fromSerializer.AttachPartitionResolver(_ => Partition);
        AssertSameSlice(source, fromSerializer);

        Assert.True(fromSerializer.Apply(new PrepareIntentCommand(MakeIntent("s/k00042", 10_042, revision: 2)), Partition).SettledReplay);
    }

    [Fact]
    public void ARestart_FromAFileWithoutALedger_LoadsTheIntents_AndMarksThePartition()
    {
        // A file of a build that predates the ledger: intents only.
        PreparedIntent pending = MakeIntent("legacy/pending", 1_000);
        File.WriteAllBytes(Path.Combine(dir, $"preparedintent_rev_p{Partition}.snapshot"), PreparedIntentStore.SerializeIntents([pending]));

        PreparedIntentStore store = new(dir, "rev", null);
        store.AttachPartitionResolver(_ => Partition);

        Assert.NotNull(store.Get("legacy/pending"));
        Assert.Empty(store.SnapshotLedger(Partition));

        // The apply-time gate cannot be turned on over a slice that was never written.
        Assert.Throws<InvalidDataException>(() => store.ConfigureOnePhaseApplyTimeValidation(true));
    }

    [Fact]
    public void ACorruptFile_StopsTheStart()
    {
        byte[] section = StreamedSection(Exporter(settled: 50));
        File.WriteAllBytes(Path.Combine(dir, $"preparedintent_rev_p{Partition}.snapshot"), Truncated(section));

        Assert.Throws<InvalidDataException>(() => new PreparedIntentStore(dir, "rev", null));
    }

    [Fact]
    public void TheVerifyPass_RejectsASectionThatDoesNotDecode_AndKeepsNothing()
    {
        byte[] section = StreamedSection(Exporter(settled: 50));

        Assert.Throws<InvalidProtocolBufferException>(() => PreparedIntentStore.VerifyPartitionSection(new MemoryStream(Truncated(section))));

        // The pass that reads the scalar fields only does not decode the entries, but it reads to the end of
        // the stream, so it meets the same end.
        Assert.Equal(50 + 1, PreparedIntentStore.VerifyPartitionSection(new MemoryStream(section)).SettledIdentities);

        // A section of a build that predates the ledger decodes, and says that it has none.
        PreparedIntentStore.SnapshotHeader legacy = PreparedIntentStore.VerifyPartitionSection(
            new MemoryStream(PreparedIntentStore.SerializeIntents([MakeIntent("legacy/k", 1_000)])));
        Assert.False(legacy.LedgerPresent);
        Assert.Equal(1, legacy.Intents);

        // No section at all: the header an install uses for an empty payload.
        PreparedIntentStore.SnapshotHeader empty = PreparedIntentStore.VerifyPartitionSection(Stream.Null);
        Assert.False(empty.LedgerPresent);
        Assert.Equal(0, empty.Intents);
    }

    [Fact]
    public void AStreamInstall_WithoutARequiredLedger_ThrowsBeforeItRemovesAnything()
    {
        byte[] legacy = PreparedIntentStore.SerializeIntents([MakeIntent("legacy/k", 1_000)]);
        PreparedIntentStore.SnapshotHeader header = PreparedIntentStore.VerifyPartitionSection(new MemoryStream(legacy));

        PreparedIntentStore store = new();
        Settle(store, MakeIntent("held/settled", 500));
        store.Apply(new PrepareIntentCommand(MakeIntent("held/pending", 600)), Partition);

        Assert.Throws<InvalidDataException>(() =>
            store.ReplacePartitionIntents(Partition, new MemoryStream(legacy), header, requireLedger: true, static _ => true));

        Assert.NotNull(store.Get("held/pending"));
        Assert.Single(store.SnapshotLedger(Partition));
        Assert.Single(store.SnapshotSettledIdentities(Partition));

        // Without the requirement the same section installs: the intents replace the held ones, and the slice
        // is empty, as the exporter's build held it.
        store.ReplacePartitionIntents(Partition, new MemoryStream(legacy), header, requireLedger: false, static _ => true);

        Assert.Null(store.Get("held/pending"));
        Assert.NotNull(store.Get("legacy/k"));
        Assert.Empty(store.SnapshotLedger(Partition));
        Assert.Empty(store.SnapshotSettledIdentities(Partition));
    }

    [Fact]
    public void AStreamInstall_KeepsTheReflectedPositionsOfTheSliceItReplaces()
    {
        // A re-delivered or older section must not re-open the history window a newer install closed.
        PreparedIntentStore exporter = Exporter(settled: 10);

        byte[] newer = exporter.SerializePartitionIntents(Partition, [.. exporter.SnapshotRange(null, null)], fullyReflectedThroughIndex: 0);
        PreparedIntentStore.PartitionIntentSection newerSection = PreparedIntentStore.DeserializePartitionIntents(newer) with
        {
            ReflectedThroughIndex = 900,
            FullyReflectedThroughIndex = 800
        };

        PreparedIntentStore receiver = new();
        receiver.ReplacePartitionIntents(Partition, newerSection, requireLedger: true, static _ => true);
        Assert.Equal(900, receiver.GetLedgerReflectedThroughIndex(Partition));

        byte[] older = StreamedSection(exporter);
        PreparedIntentStore.SnapshotHeader header = PreparedIntentStore.VerifyPartitionSection(new MemoryStream(older));
        Assert.True(header.ReflectedThroughIndex < 900);

        receiver.ReplacePartitionIntents(Partition, new MemoryStream(older), header, requireLedger: true, static _ => true);

        Assert.Equal(900, receiver.GetLedgerReflectedThroughIndex(Partition));
        Assert.Equal(800, receiver.GetLedgerFullyReflectedThroughIndex(Partition));
        Assert.True(receiver.IsHistoricalApply(Partition, 850));
    }
}
